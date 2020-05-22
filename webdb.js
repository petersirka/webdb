require('total.js');

// Global variable
global.WebDB = {};

// Common
const IMAGES = { jpg: 1, png: 1, gif: 1, svg: 1, jpeg: 1, heic: 1, heif: 1, webp: 1, tiff: 1, bmp: 1 };

// Headers
const Path = require('path');
const Fs = require('fs');

var H = exports.H = {};
global.WebDB.H = H;

H.PAGESIZE = (1024 * 12) + 1;    // 12 kB + 1 byte (page state)
H.BLOCKSIZE = 1024 * 3;          // 3 kB
H.HEADERSIZE = 10;
H.SCHEMASIZE = 1024 * 3;         // 3 kB
H.STORAGEHEADERSIZE = 300;       // 300 bytes
H.DATAOFFSET = H.HEADERSIZE + H.SCHEMASIZE;
H.DELIMITER = '\0';

// Flags & Types
H.FLAG_EMPTY = 0;
H.FLAG_RECORD = 1;
H.FLAG_REMOVED = 2;

H.STORAGE_EMPTY = 0;
H.STORAGE_JSON = 1;
H.STORAGE_IMAGE = 2;
H.STORAGE_BINARY = 3;

H.TYPE_STRING = 1;
H.TYPE_NUMBER = 2;
H.TYPE_BOOLEAN = 3;
H.TYPE_DATE = 4;

// Dependencies
const DButils = require('./utils');
global.WebDB.Utils = DButils;

const QueryBuilder = require('./builder').QueryBuilder;
const Reader = require('./reader').Reader;
const Writer = require('./writer').Writer;

// TYPES:
// 0: empty
// 1: ready
// 2: removed

// 25 bytes header
// TYPE (BYTE),ID (50 bytes),FILTERDATA (max. 3 kb)
// TYPE (BYTE),ID (50 bytes),FILTERDATA (max. 3 kb)
// TYPE (BYTE),ID (50 bytes),FILTERDATA (max. 3 kb)

function Database(filename) {
	var t = this;

	t.directory = Path.dirname(filename);
	t.filename = filename;
	t.name = Path.basename(filename).replace(/\..*?$/, '');

	t.pendingreader = [];
	t.pendingwriter = [];
	t.pendingalter = null;
	t.readers = [new Reader(t), new Reader(t)];
	t.size = 0;
	t.fd = 0;
	t.pending = 0;
	t.ready = false;
	t.mapreduce = (doc) => doc;
	t.schema = [];

	Fs.mkdir(Path.join(t.directory, t.name + '1'), NOOP);
	Fs.mkdir(Path.join(t.directory, t.name + '2'), NOOP);
	Fs.mkdir(Path.join(t.directory, t.name + '3'), NOOP);
	Fs.mkdir(Path.join(t.directory, t.name + '4'), NOOP);
	Fs.mkdir(Path.join(t.directory, t.name + '5'), NOOP);
	Fs.mkdir(Path.join(t.directory, t.name + '6'), NOOP);
	Fs.mkdir(Path.join(t.directory, t.name + '7'), NOOP);
	Fs.mkdir(Path.join(t.directory, t.name + '8'), NOOP);
	Fs.mkdir(Path.join(t.directory, t.name + '9'), NOOP);

	t.open();
	t.nextforce = function() {

		t.nextforceid = null;

		if (t.pendingalter) {

			// updating of schema
			if (!t.pending)
				t.alterforce(t.pendingalter);

			return;
		}

		if (t.ready) {

			if (t.writer.ready && t.pendingwriter.length) {
				var write = t.pendingwriter.splice(0);
				for (var i = 0; i < write.length; i++) {
					var w = write[i];
					if (w.type === 2)
						t.writer.insert.push(w);
					else
						t.writer.update.push(w);
				}
				t.writer.open();
			}

			if (t.pendingreader.length) {
				for (var i = 0; i < t.readers.length; i++) {
					if (t.readers[i].ready) {
						t.readers[i].push(t.pendingreader.splice(0));
						break;
					}
				}
			}
		}
	};
}

function parsefilename(filename) {
	var index = filename.lastIndexOf('.');
	var ext = filename.substring(index + 1).toLowerCase();
	var name = filename;

	index = name.lastIndexOf('/');
	if (index !== -1)
		name = name.substring(index + 1);

	return { name: name, filename: filename, ext: ext, storage: IMAGES[ext] ? H.STORAGE_IMAGE : H.STORAGE_BINARY };
}

Database.prototype.modify = function(doc, filename) {
	var self = this;
	var builder = new QueryBuilder();
	builder.db = self;
	builder.type = 1;
	builder.newbie = doc;
	builder.filename = filename ? parsefilename(filename) : null;
	self.pendingwriter.push(builder);
	self.next();
	return builder;
};

Database.prototype.insert = function(doc, filename) {
	var self = this;
	var builder = new QueryBuilder();
	builder.db = self;
	builder.type = 2;
	builder.newbie = doc;
	builder.filename = filename ? parsefilename(filename) : null;
	self.pendingwriter.push(builder);
	self.next();
	return builder;
};

Database.prototype.remove = function() {
	var self = this;
	var builder = new QueryBuilder();
	builder.db = self;
	builder.type = 3;
	self.pendingwriter.push(builder);
	self.next();
	return builder;
};

Database.prototype.find = function() {
	var self = this;
	var builder = new QueryBuilder();
	builder.db = self;
	self.pendingreader.push(builder);
	self.next();
	return builder;
};

Database.prototype.open = function() {
	var self = this;
	Fs.open(self.filename, 'r+', function(err, fd) {

		if (err) {
			self.create(self.filename, 1, () => self.open());
			return;
		}

		self.fd = fd;
		Fs.fstat(self.fd, function(err, stat) {
			self.writer = new Writer(self);
			self.size = stat.size;
			self.readschema();
		});
	});
};

Database.prototype.read = function(id, callback) {
	var self = this;
	var filename = self.makefilename(id);

	Fs.open(filename, function(err, fd) {

		if (err) {
			callback(err);
			return;
		}

		var buffer = Buffer.alloc(H.STORAGEHEADERSIZE);
		Fs.read(fd, buffer, 0, H.STORAGEHEADERSIZE, 0, function(err) {

			if (err) {
				callback(err);
				Fs.close(fd, NOOP);
				return;
			}

			var meta = {};
			meta.type = buffer.readInt8(10);
			meta.state = buffer.readInt8(11);
			meta.replication = buffer.readInt8(12);
			meta.size = buffer.readInt32BE(14);
			meta.width = buffer.readInt32BE(18);
			meta.height = buffer.readInt32BE(22);
			meta.name = buffer.toString('ascii', 27, 27 + buffer.readInt8(26));
			meta.stream = Fs.createReadStream(filename, { fd: fd, start: H.STORAGEHEADERSIZE });
			CLEANUP(meta.stream, () => Fs.close(fd, NOOP));
			callback(err, meta);
		});
	});
};

Database.prototype.readschema = function(callback) {

	var self = this;
	var buffer = Buffer.alloc(H.SCHEMASIZE);

	self.ready = false;

	Fs.read(self.fd, buffer, 0, buffer.length, H.HEADERSIZE, function(err, size) {

		// @TODO: missing error handling

		// no schema
		if (!size) {
			callback && callback();
			return;
		}

		var count = buffer.readInt8(0);
		var offset = 1;

		self.schema = [];
		self.schemameta = {};

		for (var i = 0; i < count; i++) {
			var item = {};
			item.type = buffer.readInt8(offset);
			item.sortindex = buffer.readInt8(offset + 1);
			var size = buffer.readInt8(offset + 2);
			item.name = buffer.toString('ascii', offset + 3, offset + size + 3);
			self.schema.push(item);
			self.schemameta[item.name] = item;
			offset += 30;
		}

		self.ready = self.schema.length > 0;
		self.next();
		callback && callback();
	});
};

Database.prototype.next = function() {
	var self = this;
	if (self.fd) {
		self.nextforceid && clearImmediate(self.nextforceid);
		self.nextforceid = setImmediate(self.nextforce);
	}
};

Database.prototype.makedata = function(dbschema, doc) {
	var self = this;
	var reduced = self.mapreduce(doc);
	var filter = [];

	for (var i = 0; i < dbschema.length; i++) {
		var schema = dbschema[i];
		var val = reduced[schema.name];
		var type = typeof(val);
		switch (schema.type) {
			case H.TYPE_NUMBER:
				if (type === 'string')
					val = val.parseFloat();
				filter.push(val == null ? '' : val);
				break;
			case H.TYPE_STRING:
				filter.push(val == null ? '' : (val + ''));
				break;
			case H.TYPE_DATE:
				filter.push(val == null ? '' : val instanceof Date ? val.getTime() : type === 'string' ? val.parseDate().getTime() : 0);
				break;
			case H.TYPE_BOOLEAN:
				filter.push(val == null ? '' : val ? 1 : 0);
				break;
		}
	}

	return filter.join(H.DELIMITER);
};

Database.prototype.create = function(filename, alloc, callback) {
	var buffer = Buffer.alloc(H.DATAOFFSET);
	buffer.write('WebDB', 0, 'ascii');
	buffer.writeInt16BE(1, 6);
	Fs.writeFile(filename, buffer, callback);
};

Database.prototype.alter = function(schema, callback) {
	var self = this;
	self.pendingalter = schema;
	self.pendingaltercallback = callback;
	self.next();
	return self;
};

Database.prototype.makefilename = function(id) {

	var sum = 0;
	var path = this.directory + '/' + this.name;

	for (var i = 0; i < id.length; i++)
		sum += id.charCodeAt(i);

	id += '.dbf';

	// return path + '1/' + id;

	if (sum % 21 === 0)
		return path + '9/' + id;

	if (sum % 18 === 0)
		return path + '8/' + id;

	if (sum % 15 === 0)
		return path + '7/' + id;

	if (sum % 12 === 0)
		return path + '6/' + id;

	if (sum % 9 === 0)
		return path + '5/' + id;

	if (sum % 6 === 0)
		return path + '4/' + id;

	if (sum % 3 === 0)
		return path + '3/' + id;

	return path + (sum % 2 === 0 ? '2/' : '1/') + id;
};

Database.prototype.loadphysical = function(modified, queue, callback) {
	var self = this;
	var id = queue.shift();

	if (!id) {
		callback(modified);
		return;
	}

	var filter = modified.cache[id];

	// JSON document
	// @TODO: implement multipe load
	if (filter.storage === H.STORAGE_JSON) {
		filter.filename = self.makefilename(filter.id);
		Fs.readFile(filter.filename, function(err, buffer) {
			if (buffer)
				filter.file = buffer.slice(H.STORAGEHEADERSIZE).toString('utf8').parseJSON(true);
			self.loadphysical(modified, queue, callback);
		});
	} else
		self.loadphysical(modified, queue, callback);
};

Database.prototype.alterforce = function(schema) {

	var buffer = Buffer.alloc(H.DATAOFFSET);
	var self = this;
	var offset = 11;
	var maxlength = 27;
	var size = 30;
	var newschema = [];

	buffer.write('WebDB', 0, 'ascii');
	buffer.writeInt16BE(1, 6);

	self.ready = false;
	self.pending++;

	// Schema items count
	buffer.writeInt8(schema.length, 10);

	// Max. 100 fields
	for (var i = 0; i < schema.length; i++) {

		var item = schema[i];
		var type = H.TYPE_STRING;

		switch (item.type) {
			case 'number':
				type = H.TYPE_NUMBER;
				break;
			case 'boolean':
				type = H.TYPE_BOOLEAN;
				break;
			case 'date':
				type = H.TYPE_DATE;
				break;
		}

		// 30 bytes
		var name = item.name.length > maxlength ? item.name.substring(0, maxlength) : item.name;
		buffer.writeInt8(type, offset);                     // Type
		buffer.writeInt8(item.sortindex || 0, offset + 1);  // Sortindex
		buffer.writeInt8(name.length, offset + 2);          // Name length
		buffer.write(name, offset + 3, 'ascii');            // Name
		offset += size;

		newschema.push({ name: name, sortindex: item.sortindex, type: type });
	}

	var modified = false;

	for (var i = 0; i < self.schema.length; i++) {
		var a = self.schema[i];
		var b = newschema[i];
		if ((!a && b) || (a && !b) || (a.name !== b.name) || (a.type !== b.type)) {
			modified = true;
			break;
		}
	}

	if (!modified) {
		Fs.write(self.fd, buffer, 0, buffer.length, 0, function(err) {
			self.pendingalter = null;
			self.pendingaltercallback && self.pendingaltercallback(err);
			self.pending--;
			self.readschema();
		});
		return self;
	}

	var filenametmp = self.filename + '-tmp';
	var offset = 0;

	Fs.open(filenametmp, 'w', function(err, fd) {
		Fs.write(fd, buffer, 0, buffer.length, 0, function(err, size) {

			if (err) {
				self.pendingalter = null;
				self.pending--;
				self.pendingaltercallback && self.pendingaltercallback(err);
				self.readschema();
				return;
			}

			offset += size;
			streamer(self, 0, function(filter, next) {

				for (var i = 0; i < filter.length; i++) {
					var item = filter[i];
					if (item.type !== H.FLAG_EMPTY) {
						item.replication = 0;
						item.data = self.makedata(newschema, item.file || item.filter);
					}
				}

				var buffer = Buffer.alloc(H.PAGESIZE);
				for (var i = 0; i < filter.length; i++) {
					var item = filter[i];
					self.writer.make(buffer, i * H.BLOCKSIZE, item);
				}

				Fs.write(fd, buffer, 0, buffer.length, offset, function(err, size) {

					if (err) {
						self.pendingalter = null;
						self.pending--;
						self.pendingaltercallback && self.pendingaltercallback(err);
						next = null;
					} else {
						offset += size;
						next();
					}
				});

			}, function() {
				Fs.close(fd, function() {
					Fs.close(self.fd, function() {
						// Rewrite DB
						Fs.rename(filenametmp, self.filename, function(err) {

							// DONE
							self.pendingalter = null;
							self.pending--;

							if (err)
								self.readschema();
							else
								self.open();

							self.pendingaltercallback && self.pendingaltercallback(err);
						});
					});
				});
			});
		});
	});
};

function streamer(database, cursor, processor, callback) {

	var buffer = Buffer.alloc(H.PAGESIZE);
	Fs.read(database.fd, buffer, 0, buffer.length, (cursor * H.PAGESIZE) + H.DATAOFFSET, function(err, size) {

		if (!size) {
			// end
			callback();
			return;
		}

		var filter = DButils.parsefilter(database.schema, readpage_streamer(buffer));

		filter.wait(function(item, next) {
			if (item.type === H.STORAGE_JSON) {
				Fs.readFile(database.makefilename(item.id), function(err, buffer) {
					item.file = buffer ? buffer.slice(H.STORAGEHEADERSIZE).toString('utf8').parseJSON(true) : EMPTYOBJECT;
					next();
				});
			} else
				next();
		}, function() {
			processor(filter, function() {
				streamer(database, cursor + 1, processor, callback);
			});
		});
	});
}

function readpage_streamer(buffer) {

	// buffer === Page
	var filter = [];

	for (var i = 0; i < ((H.PAGESIZE - 1) / H.BLOCKSIZE); i++)
		filter.push({ type: 0, storage: 0, id: null, data: null });

	var pagetype = buffer.readInt8(0);

	// Page is empty
	if (pagetype === 0)
		return filter;

	// Read documents
	for (var i = 0; i < filter.length; i++) {
		var offset = (i * H.BLOCKSIZE); // because 1 is type of page
		var type = buffer.readInt8(offset);
		var replication = buffer.readInt8(offset + 1);
		var storage = buffer.readInt8(offset + 2);
		var idsize = buffer.readInt8(offset + 3);
		var datasize = buffer.readInt16BE(offset + 4);

		var id = type ? buffer.toString('ascii', offset + 6, offset + idsize + 6) : null;
		var data = type ? buffer.toString('utf8', offset + 56, offset + 56 + datasize) : null;
		var meta = filter[i];

		meta.replication = replication;
		meta.storage = storage;
		meta.type = type;
		meta.id = id;
		meta.data = data;
	}

	return filter;
}

// var writer = new Writer();
// var reader = new Reader();
// var buffer = Buffer.alloc(PAGESIZE);

var database = new Database('test.db');
// database.pendingreader.push(null);

//database.modify({}).make('doc.id=="159234047ia61b"').modify('doc.kokot=true;doc.price=100');
//database.remove().make('doc.id=="159234047ia61b"');

// setTimeout(function() {
// 	//database.find().make('doc.id==="160498002aa60b"').callback(console.log);
// 	database.remove().make('doc.id=="160498002aa60b"').callback(console.log);
// 	//database.remove().make('doc.id=="160498002aa64b"').callback(console.log);
// }, 100);

// database.find().make('doc.price>1 && doc.price<5').take(2).callback(console.log);
//database.find().make('doc.id==="159234047ia61b"').callback(console.log);

setTimeout(function() {
	// 160706001ts61b a 160706001aa61b

	database.find().make('true').take(5).callback(console.log);
	// database.find().make('true').scalar('arg.max=Math.max(arg.max||0,item.price);arg.min=Math.min(arg.min||100,item.price)', {}).callback(console.log);
	// database.find().make('true').scalar('arg.max=Math.max(arg.max||0,item.price);arg.min=Math.min(arg.min||100,item.price)', {}).callback(console.log);

	// database.alter([{ name: 'name', type: 'string' }, { name: 'date', type: 'date' }]);
	// database.alter([{ name: 'price', type: 'number', sortindex: 3 }, { name: 'name', type: 'string' }, { name: 'date', type: 'date' }]);
	// database.remove().make('item.id=="160706001ts61b"').callback(console.log);
	// database.read('160706001ts61b', function(err, meta) {
	// 	console.log(meta);
	// });
	// database.insert({ id: UID(), name: GUID(30), price: U.random(10, 0.1), date: new Date(), body: GUID(50) }, '/Users/petersirka/Desktop/logo.png');
	// database.insert({ id: UID(), name: GUID(30), price: U.random(10, 0.1), date: new Date(), body: GUID(50) });

	// for (var i = 0; i < 10000; i++)
	// 	database.insert({ id: UID(), name: GUID(30), price: U.random(10, 0.1), date: new Date(), body: GUID(50) });

}, 200);

// database.scalar('max', 'price').callback(console.log);
// database.scalar('min', 'price').callback(console.log);
// database.scalar('avg', 'price').callback(console.log);

// database.insert({ id: UID(), name: 'Čučoriedky', price: 2.8, date: new Date() });
// database.insert({ id: UID(), name: 'Jahody', price: 2.2, date: new Date() });
// database.insert({ id: UID(), name: 'Pomaračne', price: 0.7, date: new Date() });
// database.insert({ id: UID(), name: 'Citróny', price: 0.8, date: new Date() });
// database.insert({ id: UID(), name: 'Jablká', price: 1.3, date: new Date() });
// database.insert({ id: UID(), name: 'Hrušky', price: 1.5, date: new Date() });

// database.create('test.nosqlb', 1, NOOP);
// database.alter([{ name: 'price', type: 'number' }, { name: 'name', type: 'string' }, { name: 'date', type: 'date' }]);

// setTimeout(function() {
// 	setTimeout(function() {
// 		database.insert({ id: UID(), name: GUID(30), price: U.random(10, 0.1), date: new Date(), body: GUID(50) }).callback(console.log);
// 	}, 1000);
// 	database.insert({ id: UID(), name: GUID(30), price: U.random(10, 0.1), date: new Date(), body: GUID(50) }).callback(console.log);
// }, 100);

// console.log(reader.read(writer.make(buffer, 1, [{ name: 'price' }, { name: 'name' }], '157896001rl61b', { price: 100, name: 'Peter Širka' })));

// reader.next();

