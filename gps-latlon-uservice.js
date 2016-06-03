'use strict';

let fs = require('fs');

let kafka = require('kafka-node');
let Promise = require('bluebird');
let Parser = require('binary-parser').Parser;
let type = require('./avro-types');
let pgns = require('./pgns');

// get two defined Avro types
let raw_isobus_type = type.raw_isobus_type;
let gps_latlon_type = type.gps_latlon_type;

// Creat Kafka consumer
let Consumer = kafka.Consumer;
let cons_client = new kafka.Client('vip1.ecn.purdue.edu:2181');
let consumer = new Consumer(
		cons_client,
		[
			{
				topic: 'raw-isobus',
				offset: 0
			}
		],
		{
			encoding: 'buffer',
			fromOffset: true,
			autoCommit: true,
			autoCommitIntervalMs: 5000
		}
);

// Create Kafka producer
let Producer = kafka.Producer;
let prod_client = new kafka.Client('vip1.ecn.purdue.edu:2181');
let producer = Promise.promisifyAll(new Producer(prod_client));

// Create work promise/queue
let work = Promise.resolve();

// variables for reconstructing fast packets
let fastpacket_frame_count = 0;
let fastpacket_datasize;
let fastpacket_payloads;
let fastpacket_data = [];
let expected_sequence_id;
let gps_latlon = [];

// reconstruct fast packets
function reconstruct_fp(val) {
	if (val.data[0] % 32 === 0 && fastpacket_frame_count === 0) {
		fastpacket_frame_count = 1;
		expected_sequence_id = val.data[0] + 1;
		fastpacket_datasize = val.data[1];
		for (let k = 2; k < 8; k++) {
			if (fastpacket_data.length < fastpacket_datasize) {
				fastpacket_data.push(val.data[k]);
			}
		}
	} else if (val.data[0] === expected_sequence_id) {
		for (let k = 1; k < 8; k++) {
			if (fastpacket_data.length < fastpacket_datasize) {
				fastpacket_data.push(val.data[k]);
			}
		}
		expected_sequence_id++;
		fastpacket_frame_count++;
	} else {
		fastpacket_frame_count = 0;
		expected_sequence_id = undefined;
		fastpacket_datasize = undefined;
		fastpacket_data = [];
		return null;
	}
	if (fastpacket_data.length >= fastpacket_datasize && fastpacket_data.length != 0) {
		return fastpacket_data;
	}
}

/*
   Listen on topic
   and have the producer to send it
*/
producer.on('ready', function() {
	consumer.on('message', function(message) {
		// process the message
		let rx_buf = raw_isobus_type.fromBuffer(message.value);
		let p = pgns[rx_buf.pgn];
		let data;
		let payloads;
		if (p && rx_buf.pgn === 129029) {
			fastpacket_payloads = reconstruct_fp(rx_buf);
			if (fastpacket_payloads) {
				data = p.parse(new Buffer(fastpacket_payloads, 'hex'));

				let tx_buf = gps_latlon_type.toBuffer({
						timestamp: rx_buf.timestamp,
						lat: (data.latlb + data.lathb) * 1e-16,
						lon: (data.lonlb + data.lonhb) * 1e-16
				});

				payloads = [{
						topic: 'gps-latlon',
						messages: tx_buf
				}];
				console.log('ts:', rx_buf.timestamp, 'lat:', (data.latlb + data.lathb) * 1e-16,
										'lon:', (data.lonlb + data.lonhb) * 1e-16);
//				console.log((data.latlb + data.lathb) * 1e-16, (data.lonlb + data.lonhb) * 1e-16);
			}
		}

		// send the message in order
		if (payloads) {
			work = work.then(() => {
				return producer.sendAsync(payloads);
			})
			.catch(function(err) {
				console.error(err);
				process.exit();
			});
		}
	});
});

producer.on('error', function(err) {
	console.error(err);
	process.exit();
});

process.on('exit', function() {
	cons_client.close();
	prod_client.close();
	consumer.commit();
	consumer.close();
});
