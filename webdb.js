require('total.js');

// Dependencies
const DButils = require('./utils');

// Common
const IMAGES = { jpg: 1, png: 1, gif: 1, svg: 1, jpeg: 1, heic: 1, heif: 1, webp: 1, tiff: 1, bmp: 1 };

// Headers
const Path = require('path');
const Fs = require('fs');
const PAGESIZE = (1024 * 12) + 1;    // 12 kB + 1 byte (page state)
const BLOCKSIZE = 1024 * 3;          // 3 kB
const HEADERSIZE = 10;
const SCHEMASIZE = 1024 * 3;         // 3 kB
const STORAGEHEADERSIZE = 300;       // 300 bytes
const DATAOFFSET = HEADERSIZE + SCHEMASIZE;
const DELIMITER = '\0';

// Flags & Types
const FLAG_EMPTY = 0;
const FLAG_RECORD = 1;
const FLAG_REMOVED = 2;

const STORAGE_EMPTY = 0;
const STORAGE_JSON = 1;
const STORAGE_IMAGE = 2;
const STORAGE_BINARY = 3;

const TYPE_STRING = 1;
const TYPE_NUMBER = 2;
const TYPE_BOOLEAN = 3;
const TYPE_DATE = 4;

// TYPES:
// 0: empty
// 1: ready
// 2: removed

// 25 bytes header
// TYPE (BYTE),ID (50 bytes),FILTERDATA (max. 3 kb)
// TYPE (BYTE),ID (50 bytes),FILTERDATA (max. 3 kb)
// TYPE (BYTE),ID (50 bytes),FILTERDATA (max. 3 kb)

function QueryBuilder() {

	var t = this;
	t.items = [];
	t.count = 0;
	t.counter = 0;
	t.scanned = 0;
	t.findarg = EMPTYOBJECT;
	t.modifyarg = EMPTYOBJECT;
	t.$take = 1000;
	t.$skip = 0;
	t.$storage = false;
	t.$storagetype = 1;

	// t.$fields
	// t.$sortname
	// t.$sortasc
}

QueryBuilder.prototype.fields = function(value) {

	var self = this;

	if (value === '*') {
		self.$storage = true;
		return self;
	}

	// @TODO: cache it
	self.$fields = value.split(',').trim();

	for (var i = 0; i < self.$fields.length; i++) {
		var field = self.$fields[i];
		if (field !== 'id') {
			if (!self.db.schemameta[field]) {
				self.$storage = true;
				break;
			}
		}
	}

	return self;
};

QueryBuilder.prototype.transform = function(doc) {

	var self = this;
	if (!self.$fields)
		return doc;

	var obj = {};

	// @TODO: add a custom transformation
	for (var i = 0; i < self.$fields.length; i++) {
		var name = self.$fields[i];
		obj[name] = doc[name];
	}

	return obj;
};

QueryBuilder.prototype.push = function(filter) {
	var self = this;

	if (self.$sortname)
		return DButils.sort(self, filter);

	self.items.push(filter);
	return true;
};

QueryBuilder.prototype.take = function(take) {
	this.$take = take;
	return this;
};

QueryBuilder.prototype.skip = function(skip) {
	this.$skip = skip;
	return this;
};

QueryBuilder.prototype.sort = function(field, desc) {
	this.$sortname = field;
	this.$sortasc = desc !== true;
	return this;
};

QueryBuilder.prototype.make = function(rule, arg) {
	var self = this;

	if (arg)
		self.findarg = arg;

	self.findrule = new Function('item', 'arg', 'return ' + rule);
	return self;
};

function modifyrule(doc) {
	return doc;
}

QueryBuilder.prototype.modify = function(rule, arg) {
	var self = this;

	if (arg)
		self.modifyarg = arg;

	self.modifyrule = rule ? new Function('item', 'arg', 'file', rule) : modifyrule;
	return self;
};

QueryBuilder.prototype.scalar = function(rule, arg) {
	var self = this;

	if (arg)
		self.scalararg = arg;

	self.scalarrule = new Function('item', 'arg', rule);
	return self;
};

QueryBuilder.prototype.callback = function(fn) {
	var self = this;
	self.$callback = fn;
	return self;
};

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

	return { name: name, filename: filename, ext: ext, storage: IMAGES[ext] ? STORAGE_IMAGE : STORAGE_BINARY };
}

function chownfile(filename, state, callback) {
	Fs.open(filename, 'r+', function(err, fd) {
		var buffer = Buffer.alloc(STORAGEHEADERSIZE);
		Fs.read(fd, buffer, 0, buffer.length, 0, function(err) {

			if (err) {
				callback(err);
				return;
			}

			// Changes state
			buffer.writeInt8(state, 11);

			// Resets replication
			buffer.writeInt8(0, 13);

			Fs.write(fd, buffer, 0, buffer.length, 0, function() {
				Fs.close(fd, callback);
			});
		});
	});
}

function makefile(filename, data, callback) {

	var header = Buffer.alloc(STORAGEHEADERSIZE);

	Fs.open(filename, 'w', function(err, fd) {

		if (err) {
			callback(err);
			return;
		}

		var buffer = Buffer.from(data, 'utf8');

		// Header
		header.write('WebDBfile');

		// Storage type
		header.writeInt8(STORAGE_JSON, 10);

		// State
		header.writeInt8(FLAG_RECORD, 11);

		// Compression
		header.writeInt8(0, 12);

		// Replication state
		header.writeInt8(0, 13);

		// File size
		header.writeInt32BE(buffer.length, 14);

		// Width
		header.writeInt32BE(0, 18);

		// Height
		header.writeInt32BE(0, 22);

		var name = 'document.json';

		// Name length
		header.writeInt8(name.length, 26);
		header.write(name, 27, 'ascii');

		Fs.write(fd, header, 0, header.length, 0, function() {
			Fs.write(fd, buffer, 0, buffer.length, header.length, function() {
				Fs.close(fd, callback);
			});
		});
	});
}

function movefile(filename, filenameto, callback) {

	var header = Buffer.alloc(STORAGEHEADERSIZE);
	var writer = Fs.createWriteStream(filenameto);
	var reader = Fs.createReadStream(filename.filename);
	var meta = { name: filename.name, size: 0, width: 0, height: 0 };
	var tmp;

	writer.write(header, 'binary');

	if (filename.storage === STORAGE_IMAGE) {
		reader.once('data', function(buffer) {
			switch (filename.ext) {
				case 'gif':
					tmp = framework_image.measureGIF(buffer);
					break;
				case 'png':
					tmp = framework_image.measurePNG(buffer);
					break;
				case 'jpg':
				case 'jpeg':
					tmp = framework_image.measureJPG(buffer);
					break;
				case 'svg':
					tmp = framework_image.measureSVG(buffer);
					break;
			}
		});
	}

	reader.pipe(writer);

	CLEANUP(writer, function() {

		Fs.open(filenameto, 'r+', function(err, fd) {

			if (err) {
				// Unhandled error
				callback(err);
				return;
			}

			if (tmp) {
				meta.width = tmp.width;
				meta.height = tmp.height;
			}

			// Header
			header.write('WebDBfile');

			// Storage type
			header.writeInt8(tmp ? STORAGE_IMAGE : STORAGE_BINARY, 10);

			// State
			header.writeInt8(FLAG_RECORD, 11);

			// Compression
			header.writeInt8(0, 12);

			// Replication state
			header.writeInt8(0, 13);

			meta.size = writer.bytesWritten - STORAGEHEADERSIZE;

			// File size
			header.writeInt32BE(meta.size, 14);

			// Width
			header.writeInt32BE(meta.width, 18);

			// Height
			header.writeInt32BE(meta.height, 22);

			// Name length
			header.writeInt8(meta.name.length, 26);
			header.write(meta.name, 27, 'ascii');

			// Update header
			Fs.write(fd, header, 0, header.length, 0, () => Fs.close(fd, () => callback(null, meta)));

			// Remove source file
			// @TODO: Uncomment
			// Fs.unlink(filenamefrom, NOOP);
		});

	});

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

		var buffer = Buffer.alloc(STORAGEHEADERSIZE);
		Fs.read(fd, buffer, 0, STORAGEHEADERSIZE, 0, function(err) {

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
			meta.stream = Fs.createReadStream(filename, { fd: fd, start: STORAGEHEADERSIZE });
			CLEANUP(meta.stream, () => Fs.close(fd, NOOP));
			callback(err, meta);
		});
	});
};

Database.prototype.readschema = function(callback) {

	var self = this;
	var buffer = Buffer.alloc(SCHEMASIZE);

	self.ready = false;

	Fs.read(self.fd, buffer, 0, buffer.length, HEADERSIZE, function(err, size) {

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
			case TYPE_NUMBER:
				if (type === 'string')
					val = val.parseFloat();
				filter.push(val == null ? '' : val);
				break;
			case TYPE_STRING:
				filter.push(val == null ? '' : (val + ''));
				break;
			case TYPE_DATE:
				filter.push(val == null ? '' : val instanceof Date ? val.getTime() : type === 'string' ? val.parseDate().getTime() : 0);
				break;
			case TYPE_BOOLEAN:
				filter.push(val == null ? '' : val ? 1 : 0);
				break;
		}
	}

	return filter.join(DELIMITER);
};

Database.prototype.create = function(filename, alloc, callback) {
	var buffer = Buffer.alloc(DATAOFFSET);
	buffer.write('WebDB', 0, 'ascii');
	buffer.writeInt16BE(1, 6);
	Fs.writeFile(filename, buffer, callback);
};

Database.prototype.alter = function(schema) {
	var self = this;
	self.pendingalter = schema;
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
	if (filter.storage === STORAGE_JSON) {
		filter.filename = self.makefilename(filter.id);
		Fs.readFile(filter.filename, function(err, buffer) {
			if (buffer)
				filter.file = buffer.slice(STORAGEHEADERSIZE).toString('utf8').parseJSON(true);
			self.loadphysical(modified, queue, callback);
		});
	} else
		self.loadphysical(modified, queue, callback);
};

Database.prototype.alterforce = function(schema) {

	var buffer = Buffer.alloc(DATAOFFSET);
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
		var type = TYPE_STRING;

		switch (item.type) {
			case 'number':
				type = TYPE_NUMBER;
				break;
			case 'boolean':
				type = TYPE_BOOLEAN;
				break;
			case 'date':
				type = TYPE_DATE;
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
		Fs.write(self.fd, buffer, 0, buffer.length, 0, function() {
			self.pendingalter = null;
			self.pending--;
			self.readschema();
		});
		return self;
	}

	var filenametmp = self.filename + '-tmp';
	var offset = 0;

	Fs.open(filenametmp, 'w', function(err, fd) {
		Fs.write(fd, buffer, 0, buffer.length, 0, function(err, size) {
			offset += size;
			streamer(self, 0, function(filter, next) {

				for (var i = 0; i < filter.length; i++) {
					var item = filter[i];
					if (item.type !== FLAG_EMPTY) {
						item.replication = 0;
						item.data = self.makedata(newschema, item.file || item.filter);
					}
				}

				var buffer = Buffer.alloc(PAGESIZE);
				for (var i = 0; i < filter.length; i++) {
					var item = filter[i];
					self.writer.make(buffer, i * BLOCKSIZE, item);
				}

				Fs.write(fd, buffer, 0, buffer.length, offset, function(err, size) {
					offset += size;
					next();
				});

			}, function() {
				Fs.close(fd, function() {
					Fs.close(self.fd, function() {
						// Rewrite DB
						Fs.rename(filenametmp, self.filename, function(err) {
							// DONE
							self.pendingalter = null;
							self.pending--;
							self.open();
						});
					});
				});
			});
		});
	});
};

function streamer(database, cursor, processor, callback) {

	var buffer = Buffer.alloc(PAGESIZE);
	Fs.read(database.fd, buffer, 0, buffer.length, (cursor * PAGESIZE) + DATAOFFSET, function(err, size) {

		if (!size) {
			// end
			callback();
			return;
		}

		var filter = parsefilter(database.schema, readpage_streamer(buffer));

		filter.wait(function(item, next) {
			if (item.type === STORAGE_JSON) {
				Fs.readFile(database.makefilename(item.id), function(err, buffer) {
					item.file = buffer ? buffer.slice(STORAGEHEADERSIZE).toString('utf8').parseJSON(true) : EMPTYOBJECT;
					next();
				});
			} else
				next();
		}, function() {
			processor(filter, function(write) {
				streamer(database, cursor + 1, processor, callback);
			});
		});
	});
}

function Writer(database) {

	var t = this;
	t.ready = true;
	t.buffer = Buffer.alloc(PAGESIZE);
	t.insert = [];
	t.update = [];
	t.files = [];
	t.callbacks = [];
	t.db = database;
	t.scanned = 0;
	t.insertcursor = 0;
	t.pagecount = 0;
	t.emptycount = 0;
	t.removecount = 0;

	t.readbuffer = function(err, size) {

		if (!size) {

			// end
			if (t.insert.length) {
				t.alloc();
				return;
			}

			var now = Date.now();
			var builder;

			for (var i = 0; i < t.update.length; i++) {
				builder = t.update[i];
				builder.db = null;
				builder.scanned = t.scanned;
				builder.tsfilter = now - t.ts;
				builder.pagecount = t.pagecount;
				builder.$callback(null, builder);
			}

			if (t.update.length)
				t.update = [];

			if (t.files.length)
				t.files = [];

			for (var i = 0; i < t.callbacks.length; i++) {
				builder = t.callbacks[i];
				if (builder.$callback) {
					builder.db = null;
					builder.scanned = t.scanned;
					builder.tsfilter = now - t.ts;
					builder.pagecount = t.pagecount;
					builder.$callback(null, builder);
				}
			}

			t.ready = true;
			t.db.pending--;
			t.db.next();
			return;
		}

		t.pagecount++;
		t.filter = parsefilter(t.db.schema, readpage_writer(t));

		var is = 0;
		var modified;

		t.scanned += t.filter.length;

		for (var i = 0; i < t.filter.length; i++) {

			var filter = t.filter[i];

			if (t.insert.length && filter.type === FLAG_EMPTY) {

				var data = t.insert.shift();
				filter.type = FLAG_RECORD;
				filter.cursor = t.cursor;
				filter.storage = data.$storagetype || STORAGE_EMPTY;
				filter.id = data.newbie.id;

				if (data.filename)
					t.files.push({ builder: data, filter: filter });
				else
					filter.data = t.db.makedata(t.db.schema, data.newbie);

				data.cursor = t.cursor;
				data.count++;
				data.counter++;
				data.current = filter;

				switch (filter.storage) {
					case STORAGE_JSON: // json
						makefile(t.db.makefilename(filter.id), JSON.stringify(data.newbie), NOOP);
						break;
				}

				data.callback && t.callbacks.push(data);

				if (!is)
					is = 1;

				t.insertcursor = t.cursor;
				continue;
			}

			if (filter.type !== FLAG_RECORD)
				continue;

			if (t.update.length) {

				var stop = 0;
				var length = t.update.length;

				for (var j = 0; j < t.update.length; j++) {
					var builder = t.update[j];
					builder.scanned++;

					if (builder.findrule(filter.filter, builder.findarg)) {

						builder.count++;
						builder.counter++;

						if (t.cancelable && builder.counter === builder.$take) {
							builder.cancel = true;
							stop++;
						}

						// Is remove?
						if (builder.type === 3) {
							if (!is)
								is = 1;
							filter.type = FLAG_REMOVED;
							filter.storage && chownfile(t.db.makefilename(filter.id), filter.type, NOOP);
							continue;
						}

						if (builder.filename) {
							t.files.push({ builder: builder, filter: filter });
							continue;
						}

						if (is != 2) {
							is = 2;
							modified = {};
							modified.id = [];
							modified.filter = {};
							modified.cache = {};
						}

						if (!modified.cache[filter.id]) {
							filter.modified = true;
							modified.cache[filter.id] = filter;
							modified.id.push(filter.id);
						}

						var tmp = { filter: filter, builder: builder };

						if (modified.filter[filter.id])
							modified.filter[filter.id].push(tmp);
						else
							modified.filter[filter.id] = [tmp];
					}
				}

				if (t.cancelable && stop === length && !t.insert.length)
					t.cancel = true;
			}
		}

		// @TODO: process new uploaded files
		// t.files

		if (t.files.length)
			t.savefiles(modified);
		else if (modified)
			t.db.loadphysical(modified, modified.id.slice(0), t.loadphysical);
		else if (is)
			t.flush();
		else {
			if (t.cancel) {
				t.readbuffer(null, 0);
			} else {
				t.cursor++;
				t.read();
			}
		}
	};

	t.loadphysical = function(modified) {

		// compare old
		for (var i = 0; i < modified.id.length; i++) {

			var id = modified.id[i];
			var filters = modified.filter[id];
			var data = modified.cache[id];
			var doc = data.file || data.filter;

			for (var j = 0; j < filters.length; j++) {
				var filter = filters[j];
				filter.builder.modifyrule(doc, filter.builder.modifyarg);
			}

			data.data = t.db.makedata(t.db.schema, doc);

			switch (data.storage) {
				case STORAGE_JSON:
					Fs.writeFile(t.db.makefilename(data.id), JSON.stringify(doc), NOOP);
					break;
			}
		}

		t.flush();
	};

	t.flushbuffer = function(err) {
		if (t.cancel || (!t.insert.length && !t.update.length)) {
			t.readbuffer(err, 0);
		} else {
			t.cursor++;
			t.read();
		}
	};

	t.writealloc = function(err, size) {
		t.db.size += size;
		// t.cursor++; because we are at end
		t.read();
	};
}

Writer.prototype.savefiles = function(modified) {

	var self = this;
	var files = self.files;

	files.wait(function(file, next) {
		var builder = file.builder;
		var filter = file.filter;
		movefile(builder.filename, self.db.makefilename(filter.id), function(err, meta) {
			builder.filename.width = meta.width;
			builder.filename.height = meta.height;
			builder.filename.size = meta.size;
			if (builder.modifyrule)
				builder.modifyrule(builder.newbie, builder.modifyarg, builder.filename);
			filter.data = self.db.makedata(self.db.schema, builder.newbie);
			filter.storage = builder.filename.storage;
			next();
		});
	}, function() {
		if (modified)
			self.db.loadphysical(modified, modified.id.slice(0), self.loadphysical);
		else
			self.flush();
	});
};

Writer.prototype.make = function(buffer, offset, filter) {

	var bufferdata;

	if (filter.type)
		bufferdata = Buffer.from(filter.data);

	buffer.writeInt8(filter.type, offset);
	buffer.writeInt8(filter.replication, offset + 1);
	buffer.writeInt8(filter.storage, offset + 2);

	if (filter.type) {
		buffer.writeInt8(filter.id.length, offset + 3);
		buffer.writeInt16BE(bufferdata.length, offset + 4);
		buffer.write(filter.id, offset + 6, filter.id.length, 'ascii');
		buffer.write(filter.data, offset + 56, 'utf8');
	}

	return buffer;
};

Writer.prototype.alloc = function() {
	var self = this;
	var buffer = Buffer.alloc(PAGESIZE);
	Fs.write(self.db.fd, buffer, 0, PAGESIZE, self.db.size, self.writealloc);
};

Writer.prototype.open = function() {
	var self = this;
	self.scanned = 0;
	self.ts = Date.now();
	self.ready = false;
	self.pages = (self.size - DATAOFFSET) / PAGESIZE;
	self.pagecount = 0;
	self.db.pending++;

	if (!self.update.length && self.insert.length && self.insertcursor)
		self.cursor = self.insertcursor;
	else
		self.cursor = 0;

	self.read();
};

Writer.prototype.read = function() {
	var self = this;
	Fs.read(self.db.fd, self.buffer, 0, PAGESIZE, DATAOFFSET + (self.cursor * PAGESIZE), self.readbuffer);
};

Writer.prototype.close = function() {
	var self = this;
	Fs.close(self.fd, function(err) {
		err && F.error(err);
		self.$callback && self.$callback();
	});
};

Writer.prototype.flush = function() {
	var self = this;
	var buffer = Buffer.alloc(PAGESIZE);
	for (var i = 0; i < self.filter.length; i++) {
		var filter = self.filter[i];
		self.make(buffer, i * BLOCKSIZE, filter);
	}
	Fs.write(self.db.fd, buffer, 0, PAGESIZE, DATAOFFSET + (self.cursor * PAGESIZE), self.flushbuffer);
};

function Reader(database) {

	var t = this;

	t.buffer = Buffer.alloc(PAGESIZE);
	t.cursor = 0;
	t.pages = 0;
	t.db = database;
	t.count = 0;
	t.ready = false;
	t.items = {};
	t.pagecount = 0;

	Fs.open(database.filename, 'r+', function(err, fd) {
		t.fd = fd;
		t.pages = (t.size - DATAOFFSET) / PAGESIZE;
		t.ready = true;
		t.db.next();
	});

	t.readbuffer = function(err, size) {

		if (!size) {

			// Load documents
			var loadstorage = false;
			var model = {};
			var ts = t.ts;

			model.now = Date.now();
			model.cache = {};
			model.filters = t.filters;
			model.duration = model.now - ts;
			model.pagecount = t.pagecount;

			for (var i = 0; i < t.filters.length; i++) {
				var filter = t.filters[i];
				if (!loadstorage && filter.$storage)
					loadstorage = true;
				for (var j = 0; j < filter.items.length; j++) {
					var item = filter.items[j];
					if (filter.$storage && item.storage === STORAGE_JSON) {
						filter.$transform = true;
						model.cache[item.id] = item;
					} else
						filter.items[j] = filter.transform(item.filter);
				}
			}

			if (loadstorage)
				t.db.loadphysical(model, Object.keys(model.cache), t.handlephysical);
			else {
				for (var i = 0; i < t.filters.length; i++) {
					var filter = t.filters[i];
					filter.db = null;
					filter.tsfilter = model.duration;
					filter.pagecount = model.pagecount;
					filter.$callback(null, filter);
				}
			}

			t.pagecount = 0;
			t.ready = true;
			t.items = {};
			t.filters = [];
			t.db.next();
			t.db.pending--;
			return;
		}

		t.pagecount++;
		t.filter = readpage_reader(t);

		if (t.filter.length) {
			t.filter = parsefilter(t.db.schema, t.filter);
			for (var i = 0; i < t.filter.length; i++) {

				var filter = t.filter[i];
				var length = t.filters.length;
				var stop = 0;

				for (var j = 0; j < length; j++) {
					var builder = t.filters[j];
					builder.scanned++;

					if (builder.findrule(filter.filter, builder.findarg)) {

						builder.count++;

						if (!builder.$sortname && ((builder.$skip && builder.$skip >= builder.count) || (builder.$take && builder.$take <= builder.counter)))
							continue;

						builder.counter++;

						if (builder.scalarrule)
							builder.scalarrule(filter.filter, builder.scalararg);
						else
							builder.push(filter);

						if (t.cancelable && !builder.$sortname && builder.items.length === builder.$take) {
							builder.cancel = true;
							stop++;
						}
					}
				}

				if (t.cancelable && stop === length) {
					t.readbuffer(null, 0);
					return;
				}
			}
		}

		t.cursor++;
		t.read();
	};

	t.handlephysical = function(response) {
		for (var i = 0; i < response.filters.length; i++) {
			var filter = response.filters[i];

			if (filter.$transform) {
				for (var j = 0; j < filter.items.length; j++) {
					var item = filter.items[j];
					if (item.storage === STORAGE_JSON)
						filter.items[j] = filter.transform(response.cache[item.id].file);
				}
			}

			filter.tsfilter = response.duration;
			filter.tsfiles = Date.now() - response.now;
			filter.db = null;
			filter.$callback(null, filter);
		}
	};
}

Reader.prototype.push = function(filters) {
	var self = this;
	self.filters = filters;
	self.ready = false;
	self.pagecount = 0;
	self.cancelable = true;
	self.db.pending++;

	for (var i = 0; i < filters.length; i++) {
		var filter = filters[i];
		if (filter.$sortname) {
			self.cancelable = false;
			break;
		}
	}

	self.ts = Date.now();
	self.read();
};

Reader.prototype.read = function() {
	var self = this;
	Fs.read(self.fd, self.buffer, 0, PAGESIZE, DATAOFFSET + (self.cursor * PAGESIZE), self.readbuffer);
};

function readpage_writer(instance) {

	// buffer === Page
	var buffer = instance.buffer;
	var filter = [];

	for (var i = 0; i < ((PAGESIZE - 1) / BLOCKSIZE); i++)
		filter.push({ type: 0, storage: 0, id: null, data: null });

	var pagetype = buffer.readInt8(0);

	// Page is empty
	if (pagetype === 0)
		return filter;

	// Read documents
	for (var i = 0; i < filter.length; i++) {

		var offset = (i * BLOCKSIZE); // because 1 is type of page
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

		switch (type) {
			case FLAG_RECORD:
				instance.count++;
				break;
			case FLAG_REMOVED:
				instance.removecount++;
				break;
			case FLAG_EMPTY:
				instance.emptycount;
				break;
		}
	}

	return filter;
}

function readpage_reader(instance) {

	// buffer === Page
	var buffer = instance.buffer;
	var count = ((PAGESIZE - 1) / BLOCKSIZE);
	var filter = [];

	var pagetype = buffer.readInt8(0);

	// Page is empty
	if (pagetype === 0)
		return filter;

	// Reads document
	for (var i = 0; i < count; i++) {

		var offset = (i * BLOCKSIZE); // because 1 is type of page
		var type = buffer.readInt8(offset);
		if (type !== FLAG_RECORD)
			continue;

		var replication = buffer.readInt8(offset + 1);
		var storage = buffer.readInt8(offset + 2);
		var idsize = buffer.readInt8(offset + 3);
		var datasize = buffer.readInt16BE(offset + 4);

		var id = type === FLAG_RECORD ? buffer.toString('ascii', offset + 6, offset + idsize + 6) : null;
		var data = type === FLAG_RECORD ? buffer.toString('utf8', offset + 56, offset + 56 + datasize) : null;
		var output = { type: type, storage: storage, id: id, data: data, cursor: instance.cursor, replication: replication };

		filter.push(output);

		switch (type) {
			case FLAG_RECORD:
				instance.count++;
				break;
			case FLAG_REMOVED:
				instance.removecount++;
				break;
			case FLAG_EMPTY:
				instance.emptycount;
				break;
		}
	}

	return filter;
}

function readpage_streamer(buffer) {

	// buffer === Page
	var filter = [];

	for (var i = 0; i < ((PAGESIZE - 1) / BLOCKSIZE); i++)
		filter.push({ type: 0, storage: 0, id: null, data: null });

	var pagetype = buffer.readInt8(0);

	// Page is empty
	if (pagetype === 0)
		return filter;

	// Read documents
	for (var i = 0; i < filter.length; i++) {
		var offset = (i * BLOCKSIZE); // because 1 is type of page
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

function parsefilter(schema, filter) {

	for (var i = 0; i < filter.length; i++) {

		var item = filter[i];

		if (item.type !== FLAG_RECORD)
			continue;

		var data = item.data.split(DELIMITER);
		var obj = {};

		for (var j = 0; j < schema.length; j++) {
			var meta = schema[j];
			var val = data[j];
			switch (meta.type) {
				case TYPE_NUMBER:
					val = val == '' ? null : +val;
					break;
				case TYPE_BOOLEAN:
					val = val === '2' ? null : val === '1';
					break;
				case TYPE_DATE:
					val = val ? new Date(+val) : null;
					break;
			}
			obj[meta.name] = val;
		}
		obj.id = item.id;
		item.filter = obj;
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

	//database.find().make('true').take(5).callback(console.log);
	// database.find().make('true').scalar('arg.max=Math.max(arg.max||0,item.price);arg.min=Math.min(arg.min||100,item.price)', {}).callback(console.log);
	database.find().make('true').scalar('arg.max=Math.max(arg.max||0,item.price);arg.min=Math.min(arg.min||100,item.price)', {}).callback(console.log);

	// database.alter([{ name: 'name', type: 'string' }, { name: 'date', type: 'date' }]);
	// database.alter([{ name: 'price', type: 'number', sortindex: 3 }, { name: 'name', type: 'string' }, { name: 'date', type: 'date' }]);
	// database.remove().make('doc.id=="160706001ts61b"').callback(console.log);
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

