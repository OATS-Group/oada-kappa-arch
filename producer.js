'use strict';

let kafka = require('kafka-node');
let avro = require('avsc');
let sqlite3 = require('sqlite3').verbose();
let Promise = require('bluebird');

let HighLevelProducer = kafka.HighLevelProducer;
let client = new kafka.Client('vip1.ecn.purdue.edu:2181/');
let producer = new HighLevelProducer(client);
let producerPromise = Promise.fromCallback(function(done) {
	producer.on('ready', function() {
		done(null, producer);
	});

	producer.on('error', function(err) {
		done(err, null);
	});
}); 

let db = new sqlite3.Database('../aarons_combine.sqlite3');
let isobusMsgType = avro.parse({
	name: 'isobusmsg',
	type: 'record',
	fields: [
		{ name: 'timestamp', type: 'long' },
		{ name: 'pgn', type: 'int' },
		{ name: 'data', type: 'bytes' }
	]
});

db.serialize(function() {
	db.each('SELECT time, pgn, data FROM isobus_messages', function(err, row) {
		let buf = isobusMsgType.toBuffer(
			{
				timestamp: row.time,
				pgn: row.pgn,
				data: row.data
			}
		);

		let val = isobusMsgType.fromBuffer(buf);
		
		let payloads = [
			{ topic: 'isobus-msg', messages: val },
		];

		producerPromise.then(function(producer) {
			producer.send(payloads, function(err, data) {
			
			});
		});
	});
});

db.close();
