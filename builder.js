// Dependencies
const DButils = require('./utils');
const WebDB = require('./webdb');

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

exports.QueryBuilder = QueryBuilder;