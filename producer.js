'use strict';

let kafka = require('kafka-node');
let sqlite3 = require('sqlite3').verbose();
let Promise = require('bluebird');
let raw_isobus_type = require('./avro-types').raw_isobus_type;

let db = Promise.promisifyAll(new sqlite3.Database('../aarons_combine.sqlite3'));

let HighLevelProducer = kafka.HighLevelProducer;
let client = new kafka.Client('vip1.ecn.purdue.edu:2181/');
let producer = Promise.promisifyAll(new HighLevelProducer(client));

producer.on('ready', function() {
	db.allAsync('SELECT time, pgn, data FROM isobus_messages')
		/*
		.tap((rows) => {
			console.log(rows[rows.length-1].time - rows[0].time);
			process.exit();
		})
		*/
		.each(function(row, count, total) {
			// console.log('%d', row.time);
			// console.log('Sending %d of %d', count+1, total);

			let buf = raw_isobus_type.toBuffer({
					timestamp: row.time,
					pgn: row.pgn,
					data: row.data
			});

			let payloads = [{
				topic: 'raw-isobus',
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
