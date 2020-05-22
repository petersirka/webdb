const Fs = require('fs');
const DButils = require('./utils');
const H = WebDB.H;

function Reader(database) {

	var t = this;

	t.buffer = Buffer.alloc(H.PAGESIZE);
	t.cursor = 0;
	t.pages = 0;
	t.db = database;
	t.count = 0;
	t.ready = false;
	t.items = {};
	t.pagecount = 0;

	Fs.open(database.filename, 'r+', function(err, fd) {
		t.fd = fd;
		t.pages = (t.size - H.DATAOFFSET) / H.PAGESIZE;
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
					if (filter.$storage && item.storage === H.STORAGE_JSON) {
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
			t.filter = DButils.parsefilter(t.db.schema, t.filter);
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
					if (item.storage === H.STORAGE_JSON)
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

// Start reading
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

// Read data from the file
Reader.prototype.read = function() {
	var self = this;
	Fs.read(self.fd, self.buffer, 0, H.PAGESIZE, H.DATAOFFSET + (self.cursor * H.PAGESIZE), self.readbuffer);
};

// The method reads data from the current page
function readpage_reader(instance) {

	// buffer === Page
	var buffer = instance.buffer;
	var count = ((H.PAGESIZE - 1) / H.BLOCKSIZE);
	var filter = [];

	var pagetype = buffer.readInt8(0);

	// Page is empty
	if (pagetype === 0)
		return filter;

	// Reads document
	for (var i = 0; i < count; i++) {

		var offset = (i * H.BLOCKSIZE); // because 1 is type of page
		var type = buffer.readInt8(offset);
		if (type !== H.FLAG_RECORD)
			continue;

		var replication = buffer.readInt8(offset + 1);
		var storage = buffer.readInt8(offset + 2);
		var idsize = buffer.readInt8(offset + 3);
		var datasize = buffer.readInt16BE(offset + 4);

		var id = type === H.FLAG_RECORD ? buffer.toString('ascii', offset + 6, offset + idsize + 6) : null;
		var data = type === H.FLAG_RECORD ? buffer.toString('utf8', offset + 56, offset + 56 + datasize) : null;
		var output = { type: type, storage: storage, id: id, data: data, cursor: instance.cursor, replication: replication };

		filter.push(output);

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

exports.Reader = Reader;