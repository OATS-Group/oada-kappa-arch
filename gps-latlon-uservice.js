'use strict';

let fs = require('fs');

let kafka = require('kafka-node');
let Promise = require('bluebird');
let Parser = require('binary-parser').Parser;
let raw_isobus = require('./avro-types').raw_isobus;

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

// variables for reconstructing fast packets
let fastpacket_frame_count = 0;
let fastpacket_datasize;
let expected_sequence_id;
let fastpacket_data = [];
let gps_latlon = [];

/**************************************************************
 * binary-parser (or javascript) doesn't support 64 bit numbers.
 * Need to parse out high bytes and low bytes and add them
 * together.
 *************************************************************/

let gps_fp = new Parser()
		.skip(7)
		.uint32le('latlb', {
			formatter: (state) => {
				return (state);
			}
		})
		.int32le('lathb', {
			formatter: (state) => {
				return (state * Math.pow(2, 32));
			}
		})
		.uint32le('lonlb', {
			formatter: (state) => {
				return (state);
			}
		})
		.int32le('lonhb', {
			formatter: (state) => {
				return (state * Math.pow(2, 32));
			}
		});

// reconstruct fast packets
function reconstruct_fp(val) {
	if (val.pgn === 129029) {
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
}

consumer.on('message', function(message) {
	let msg_buffer = raw_isobus.fromBuffer(message.value);
	let fp_payloads = reconstruct_fp(msg_buffer);

	if (fp_payloads != null) {
		let data = gps_fp.parse(new Buffer(fp_payloads, 'hex'));
		data.lat = (data.latlb + data.lathb) * 1e-16;
		data.lon = (data.lonlb + data.lonhb) * 1e-16;
		data.ts = parseInt(msg_buffer.timestamp);

		gps_latlon.push(data);
		console.log('ts:', data.ts, 'lat:', data.lat, 'lon:', data.lon);
	}
});

process.on('exit', function() {
	client.close();
	consumer.close();
	// fs.writeFileSync('data.json', JSON.stringify(gps_latlon, undefined, 2));
});
