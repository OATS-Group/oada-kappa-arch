'use strict';

let kafka = require('kafka-node');
let types = require('./avro-types');

let fr_map_type = types.fr_map_type;

// Creat fr-map message consumer
let Consumer = kafka.Consumer;
let cons_client = new kafka.Client('vip1.ecn.purdue.edu:2181');
let cons_consumer = new Consumer(
		cons_client,
		[
			{
				topic: 'fr-map',
				offset: 0
			}
		],
		{
			encoding: 'buffer',
			autoCommit: true,
			fromOffset: true,
			autoCommitIntervalMs: 5000
		}
);

let fr_map_msg_buf = [];

function oada_pusher() {
	let old_gps_lat = fr_map_msg_buf[0].lat;
	let old_gps_lon = fr_map_msg_buf[0].lon;
	let index = fr_map_msg_buf.findIndex((fr_map_msg) => ((fr_map_msg.lat !== old_gps_lat)
																				|| (fr_map_msg.lon !== old_gps_lon)));
	if (index > 0) {
		let new_fr_map_msg = fr_map_msg_buf.slice(0, index - 1);
		let oada_gps_lat = fr_map_msg_buf[index - 1].lat;
		let oada_gps_lon = fr_map_msg_buf[index - 1].lon;
//		console.log('new length:', new_fr_map_msg.length);
//		new_fr_map_msg.forEach((msg) => console.log(msg.timestamp, msg.lat, msg.lon, msg.fuelrate));
		let sum_fr = 0;
		for (let i = 0; i < new_fr_map_msg.length; i++) {
			sum_fr += new_fr_map_msg[i].fuelrate;
		}
		let avg_fr = sum_fr / new_fr_map_msg.length;

		fr_map_msg_buf = fr_map_msg_buf.slice(index);
		console.log('%d,%d,%d', oada_gps_lat, oada_gps_lon, avg_fr);
	}
}

cons_consumer.on('message', function(message) {
	let fr_map_buf = fr_map_type.fromBuffer(message.value);
//	console.log(fr_map_buf.timestamp, fr_map_buf.lat, fr_map_buf.lon, fr_map_buf.fuelrate);
	fr_map_msg_buf.push(fr_map_buf);
	oada_pusher();
});
