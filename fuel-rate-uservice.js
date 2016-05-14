'use strict';

let kafka = require('kafka-node');
let Promise = require('bluebird');
let Parser = require('binary-parser').Parser;
let type = require('./avro-types');
let pgns = require('./pgns');

let raw_isobus_type = type.raw_isobus_type;

// Create Kafka consumer
let HighLevelConsumer = kafka.HighLevelConsumer;
let client = new kafka.Client('vip1.ecn.purdue.edu:2181/');
let consumer = new HighLevelConsumer(
		client,
		[
			{ topic: 'raw-isobus' }
		],
		{
			encoding: 'buffer'
		}
);

// listen on topic
consumer.on('message', function(message) {
	let msg_buffer = raw_isobus_type.fromBuffer(message.value);
	let p = pgns[msg_buffer.pgn];
	if (p && msg_buffer.pgn === 65266) {
		let data = p.parse(new Buffer(msg_buffer.data, 'hex'));

		console.log('ts:', msg_buffer.timestamp, 'fuel rate:', data.fuel_rate_lhr);
	}
});

/*
producer.on('ready', function() {
	console.log('after ready');
	let payloads = [{
		topic: 'gps-latlon',
		messages: buf
	}];

	producer.sendAsync(payloads)
	.then(function() {
		prod_client.close();
	})
	.catch(function(err) {
		console.error(err);
		process.exit();
	})
});

producer.on('error', function(err) {
	console.error(err);
	process.exit();
});
*/

process.on('exit', function() {
	cons_client.close();
	consumer.close();
});
