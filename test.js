const Database = require('./index').Database;

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

