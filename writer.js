const Fs = require('fs');
const H = WebDB.H;
const DButils = WebDB.Utils;

function Writer(database) {

	var t = this;
	t.ready = true;
	t.buffer = Buffer.alloc(H.PAGESIZE);
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
				builder.$callback && builder.$callback(null, builder);
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
					builder.$callback && builder.$callback(null, builder);
				}
			}

			t.ready = true;
			t.db.pending--;
			t.db.next();
			return;
		}

		t.pagecount++;
		t.filter = DButils.parsefilter(t.db.schema, readpage_writer(t));

		var is = 0;
		var modified;

		t.scanned += t.filter.length;

		for (var i = 0; i < t.filter.length; i++) {

			var filter = t.filter[i];

			if (t.insert.length && filter.type === H.FLAG_EMPTY) {

				var data = t.insert.shift();
				filter.type = H.FLAG_RECORD;
				filter.cursor = t.cursor;
				filter.storage = data.$storagetype || H.STORAGE_EMPTY;
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
					case H.STORAGE_JSON: // json
						DButils.makefile(t.db.makefilename(filter.id), JSON.stringify(data.newbie), NOOP);
						break;
				}

				data.callback && t.callbacks.push(data);

				if (!is)
					is = 1;

				t.insertcursor = t.cursor;
				continue;
			}

			if (filter.type !== H.FLAG_RECORD)
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
							filter.type = H.FLAG_REMOVED;
							filter.storage && DButils.chownfile(t.db.makefilename(filter.id), filter.type, NOOP);
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
				try {
					filter.builder.modifyrule(doc, filter.builder.modifyarg);
				} catch (e) {
					filter.builder.error = e;
				}
			}

			data.data = t.db.makedata(t.db.schema, doc);

			switch (data.storage) {
				case H.STORAGE_JSON:
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
		DButils.movefile(builder.filename, self.db.makefilename(filter.id), function(err, meta) {
			builder.filename.width = meta.width;
			builder.filename.height = meta.height;
			builder.filename.size = meta.size;
			if (builder.modifyrule) {
				try {
					builder.modifyrule(builder.newbie, builder.modifyarg, builder.filename);
				} catch (e) {
					builder.error = e;
				}
			}
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
	var buffer = Buffer.alloc(H.PAGESIZE);
	Fs.write(self.db.fd, buffer, 0, H.PAGESIZE, self.db.size, self.writealloc);
};

Writer.prototype.open = function() {
	var self = this;
	self.scanned = 0;
	self.ts = Date.now();
	self.ready = false;
	self.pages = (self.size - H.DATAOFFSET) / H.PAGESIZE;
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
	Fs.read(self.db.fd, self.buffer, 0, H.PAGESIZE, H.DATAOFFSET + (self.cursor * H.PAGESIZE), self.readbuffer);
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
	var buffer = Buffer.alloc(H.PAGESIZE);
	for (var i = 0; i < self.filter.length; i++) {
		var filter = self.filter[i];
		self.make(buffer, i * H.BLOCKSIZE, filter);
	}
	Fs.write(self.db.fd, buffer, 0, H.PAGESIZE, H.DATAOFFSET + (self.cursor * H.PAGESIZE), self.flushbuffer);
};

function readpage_writer(instance) {

	// buffer === Page
	var buffer = instance.buffer;
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

		switch (type) {
			case H.FLAG_RECORD:
				instance.count++;
				break;
			case H.FLAG_REMOVED:
				instance.removecount++;
				break;
			case H.FLAG_EMPTY:
				instance.emptycount;
				break;
		}
	}

	return filter;
}

exports.Writer = Writer;