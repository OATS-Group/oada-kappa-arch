'use strict';

let rp = require('request-promise');
let geohash = require('ngeohash');
let kafka = require('kafka-node');
let types = require('./avro-types');

// Get Avro data type
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

// Create headers for HTTP request
let headers = {
	'Content-Type': 'application/json',
	'Authorization': 'Bearer CXURd_VrRJSn1m-0cNyiqKxq7xintCBCx6Zu7Mwf'
};

let data_payloads = {};
let res_uri = 'https://vip4.ecn.purdue.edu:3000/bookmarks/geohash-7/';

// Create options for HTTP request
let options = {
	uri: res_uri,
	method: 'POST',
	headers: headers,
	body: {
		data: data_payloads,
	},
	json: true
};

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
		let sum_fr = 0;
		for (let i = 0; i < new_fr_map_msg.length; i++) {
			sum_fr += new_fr_map_msg[i].fuelrate;
		}
		let avg_fr = sum_fr / new_fr_map_msg.length;

		fr_map_msg_buf = fr_map_msg_buf.slice(index);
		console.log('%d,%d,%d', oada_gps_lat, oada_gps_lon, avg_fr);

		let gh = geohash.encode(oada_gps_lat, oada_gps_lon);

		options.uri = res_uri + gh;

		data_payloads.lat = oada_gps_lat;
		data_payloads.lon = oada_gps_lon;
		data_payloads.fr = avg_fr;

		if (data_payloads) {
			return rp(options)
				.then(function(parsedBody) {
					console.log('POST succeeded');
				})
				.catch(function(err) {
					console.error(err);
				});
		}
	}
}

cons_consumer.on('message', function(message) {
	let fr_map_buf = fr_map_type.fromBuffer(message.value);
	fr_map_msg_buf.push(fr_map_buf);
	oada_pusher();

});
