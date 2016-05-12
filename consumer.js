'use strict';

let kafka = require('kafka-node');
let avro = require('avsc');
let Promise = require('bluebird');

let HighLevelConsumer = kafka.HighLevelConsumer;
let client = new kafka.Client('vip1.ecn.purdue.edu:2181/');
let consumer = new HighLevelConsumer(
		client,
		[
			{ topic: 'isobus-msg' }
		]
);

consumer.on('message', function(message) {
	console.log(message);
});
