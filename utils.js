// Dependencies
const H = WebDB.H;
const COMPARER = global.Intl.Collator().compare;
const Fs = require('fs');

exports.sort = function(builder, item) {
	var length = builder.items.length;
	if (length < builder.$take) {
		length = builder.items.push(item);
		if (length >= builder.$take) {
			var type = builder.db.schemameta[builder.$sortname];
			builder.items.sort((a, b) => sortcompare(type.type, builder, a, b));
		}
		return true;
	} else
		return chunkyscroll(builder, item);
};

// Parse raw data according to the schema
exports.parsefilter = function(schema, filter) {

	for (var i = 0; i < filter.length; i++) {

		var item = filter[i];

		if (item.type !== H.FLAG_RECORD)
			continue;

		var data = item.data.split(H.DELIMITER);
		var obj = {};

		for (var j = 0; j < schema.length; j++) {
			var meta = schema[j];
			var val = data[j];
			switch (meta.type) {
				case H.TYPE_NUMBER:
					val = val == '' ? null : +val;
					break;
				case H.TYPE_BOOLEAN:
					val = val === '' ? null : val === '1';
					break;
				case H.TYPE_DATE:
					val = val ? new Date(+val) : null;
					break;
			}
			obj[meta.name] = val;
		}
		obj.id = item.id;
		item.filter = obj;
	}

	return filter;
};

exports.chownfile = function(filename, state, callback) {
	Fs.open(filename, 'r+', function(err, fd) {
		var buffer = Buffer.alloc(H.STORAGEHEADERSIZE);
		Fs.read(fd, buffer, 0, buffer.length, 0, function(err) {

			if (err) {
				callback(err);
				return;
			}

			// Changes state
			buffer.writeInt8(state, 11);

			// Resets replication
			buffer.writeInt8(0, 13);

			Fs.write(fd, buffer, 0, buffer.length, 0, function(err) {
				if (err)
					callback(err);
				else
					Fs.close(fd, callback);
			});
		});
	});
};

exports.movefile = function(filename, filenameto, callback) {

	var header = Buffer.alloc(H.STORAGEHEADERSIZE);
	var writer = Fs.createWriteStream(filenameto);
	var reader = Fs.createReadStream(filename.filename);
	var meta = { name: filename.name, size: 0, width: 0, height: 0 };
	var tmp;

	writer.write(header, 'binary');

	if (filename.storage === H.STORAGE_IMAGE) {
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
			header.writeInt8(tmp ? H.STORAGE_IMAGE : H.STORAGE_BINARY, 10);

			// State
			header.writeInt8(H.FLAG_RECORD, 11);

			// Compression
			header.writeInt8(0, 12);

			// Replication state
			header.writeInt8(0, 13);

			meta.size = writer.bytesWritten - H.STORAGEHEADERSIZE;

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
			Fs.write(fd, header, 0, header.length, 0, function(err) {
				if (err) {
					callback(err);
					Fs.close(fd, NOOP);
				} else
					Fs.close(fd, () => callback(null, meta));
			});

			// Remove source file
			// @TODO: Uncomment
			// Fs.unlink(filenamefrom, NOOP);
		});

	});
};

exports.makefile = function(filename, data, callback) {

	var header = Buffer.alloc(H.STORAGEHEADERSIZE);

	Fs.open(filename, 'w', function(err, fd) {

		if (err) {
			callback(err);
			return;
		}

		var buffer = Buffer.from(data, 'utf8');

		// Header
		header.write('WebDBfile');

		// Storage type
		header.writeInt8(H.STORAGE_JSON, 10);

		// State
		header.writeInt8(H.FLAG_RECORD, 11);

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

		Fs.write(fd, header, 0, header.length, 0, function(err) {

			if (err) {
				callback(err);
				return;
			}

			Fs.write(fd, buffer, 0, buffer.length, header.length, function(err) {
				callback(err);
				Fs.close(fd, NOOP);
			});
		});
	});
};

function sortcompare(type, builder, a, b) {
	var va = a.filter[builder.$sortname];
	var vb = b.filter[builder.$sortname];
	var vr = 0;
	switch (type) {
		case 1: // string
			vr = va && vb ? COMPARER(va, vb) : va && !vb ? -1 : 1;
			break;
		case 2: // number
			vr = va != null && vb != null ? (va < vb ? -1 : 1) : va != null && vb == null ? -1 : va === vb ? 0 : 1;
			break;
		case 3: // boolean
			vr = va === true && vb === false ? -1 : va === false && vb === true ? 1 : 0;
			break;
		case 4: // Date
			vr = va != null && vb != null ? (va < vb ? -1 : 1) : va != null && vb == null ? -1 : 1;
			break;
	}
	return builder.$sortasc ? vr : (vr * -1);
}

function chunkyscroll(builder, item) {

	var beg = 0;
	var length = builder.items.length;
	var tmp = length - 1;
	var type = builder.db.schemameta[builder.$sortname];

	var sort = sortcompare(type.type, builder, item, builder.items[tmp]);
	if (sort !== -1)
		return;

	tmp = builder.items.length / 2 >> 0;
	sort = sortcompare(type.type, builder, item, builder.items[tmp]);

	if (sort !== -1)
		beg = tmp + 1;

	for (var i = beg; i < length; i++) {
		var old = builder.items[i];
		var sort = sortcompare(type.type, builder, item, old);
		if (sort === -1) {
			for (var j = length - 1; j > i; j--)
				builder.items[j] = builder.items[j - 1];
			builder.items[i] = item;
		}
		return true;
	}
}