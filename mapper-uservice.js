'use strict';

let kafka = require('kafka-node');
let Promise = require('bluebird');
let pgns = require('./pgns');
let types = require('./avro-types');

// get two defined Avro types
let fuel_rate_type = types.fuel_rate_type;
let gps_latlon_type = types.gps_latlon_type;

let gps_msg_buf = [];
let fr_msg_buf = [];
let data = {};

// Creat gps message consumer
let Consumer = kafka.Consumer;
let gps_client = new kafka.Client('vip1.ecn.purdue.edu:2181');
let gps_consumer = new Consumer(
		gps_client,
		[
			{ topic: 'gps-latlon' }
		],
		{
			encoding: 'buffer',
			autoCommit: true,
			autoCommitIntervalMs: 5000
		}
);

// Creat fuel rate message consumer
let fr_client = new kafka.Client('vip1.ecn.purdue.edu:2181');
let fr_consumer = new Consumer(
		fr_client,
		[
			{ topic: 'fuel-rate' }
		],
		{
			encoding: 'buffer',
			autoCommit: true,
			autoCommitIntervalMs: 5000
		}
);

// Create work promise/queue
let work = Promise.resolve();

function mapper() {
	let diff = [];
	let len;
	fr_msg_buf.every(function(fr_msg, index, array) {
		let ready = gps_msg_buf.some(function(gps_msg, index, array) {
			return gps_msg.timstamp >= fr_msg.timestamp;
		});

		if (!ready) {
			return false;
		} else {
		}
	});
}	

gps_consumer.on('message', function(message) {
	let gps_buf = gps_latlon_type.fromBuffer(message.value);
	gps_msg_buf.push(gps_buf);
	mapper();
});

fr_consumer.on('message', function(message) {
	let fr_buf = fuel_rate_type.fromBuffer(message.value);
	fr_msg_buf.push(fr_buf);
	mapper();
});
