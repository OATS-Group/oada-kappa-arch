'use strict';

let fs = require('fs');

let kafka = require('kafka-node');
let avro = require('avsc');
let Promise = require('bluebird');
let Parser = require('binary-parser').Parser;

// Create Kafka consumer
let HighLevelConsumer = kafka.HighLevelConsumer;
let client = new kafka.Client('vip1.ecn.purdue.edu:2181/');
let consumer = new HighLevelConsumer(
		client,
		[
			{ topic: 'isobus-msg8' }
		],
		{
			encoding: 'buffer'
		}
);

// define ISOBUS Avro type
let isobus_msg_type = avro.parse({
	name: 'isobusmsg',
	type: 'record',
	fields: [
		{ name: 'timestamp', type: 'long' },
		{ name: 'pgn', type: 'int' },
		{ name: 'data', type: 'bytes' }
	]
});

let fastpacket_frame_count = 0;
let fastpacket_data = [];
let fastpacket_datasize;
let expected_sequence_id;
let gps_latlon = [];

/**************************************************************
 * binary-parser (or javascript) doesn't support 64 bit numbers.
 * Need to parse out high bytes and low bytes and add them
 * together.
 *************************************************************/

let pgn_129029 = new Parser()
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
function parse_gps(val) {
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

// consumer.addTopics([{ topic: 'isobus-msg8', offset: 0}], function(err, added) {}, true);


consumer.on('message', function(message) {
	let val = isobus_msg_type.fromBuffer(message.value);
	let gps_data_payloads = parse_gps(val);

	if (gps_data_payloads != null) {
		/*
		console.log('lat bytes:',
								gps_data_payloads[7], gps_data_payloads[8],
								gps_data_payloads[9], gps_data_payloads[10],
								gps_data_payloads[11], gps_data_payloads[12],
								gps_data_payloads[13], gps_data_payloads[14]);
		console.log('loni bytes:',
								gps_data_payloads[15], gps_data_payloads[16],
								gps_data_payloads[17], gps_data_payloads[18],
								gps_data_payloads[19], gps_data_payloads[20],
								gps_data_payloads[21], gps_data_payloads[22]);
		*/
		// console.log(pgn_129029.parse(new Buffer(gps_data_payloads, 'hex')));
		let parsed_gps_data = pgn_129029.parse(new Buffer(gps_data_payloads, 'hex'));
		parsed_gps_data.lat = (parsed_gps_data.latlb + parsed_gps_data.lathb) * 1e-16;
		parsed_gps_data.lon = (parsed_gps_data.lonlb + parsed_gps_data.lonhb) * 1e-16;
		parsed_gps_data.ts = parseInt(val.timestamp);

		gps_latlon.push(parsed_gps_data);
		console.log('ts:', parsed_gps_data.ts,
								'lat:', parsed_gps_data.lat,
								'lon:', parsed_gps_data.lon);
	}
});

process.on('exit', function() {
	client.close();
	consumer.close();
	// fs.writeFileSync('data.json', JSON.stringify(gps_latlon, undefined, 2));
});
