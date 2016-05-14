'use strict';

let kafka = require('kafka-node');
let avro = require('avsc');
let sqlite3 = require('sqlite3').verbose();
let Promise = require('bluebird');
let isobus_msg_type = avro.parse({
	name: 'isobusmsg',
	type: 'record',
	fields: [
		{ name: 'timestamp', type: 'long' },
		{ name: 'pgn', type: 'int' },
		{ name: 'data', type: 'bytes' }
	]
});

let db = Promise.promisifyAll(new sqlite3.Database('../aarons_combine.sqlite3'));

let HighLevelProducer = kafka.HighLevelProducer;
let client = new kafka.Client('vip1.ecn.purdue.edu:2181/');
let producer = Promise.promisifyAll(new HighLevelProducer(client));

producer.on('ready', function() {
	db.allAsync('SELECT time, pgn, data FROM isobus_messages')
		.each(function(row, count, total) {
//			console.log('Sending %d of %d', count+1, total);

			let buf = isobus_msg_type.toBuffer({
					timestamp: row.time,
					pgn: row.pgn,
					data: row.data
			});

			let payloads = [{
				topic: 'isobus-msg8',
				messages: buf,
				//key: 'candroid-0',
				//partition: 0
			}];

			return producer.sendAsync(payloads);
		})
		.then(function() {
			client.close();
			db.close();
		})
		.catch(function(err) {
			console.error(err);
			process.exit();
		});
});

producer.on('error', function(err) {
	console.error(err);
	process.exit();
});
