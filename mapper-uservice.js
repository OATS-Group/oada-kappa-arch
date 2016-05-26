'use strict';

let kafka = require('kafka-node');
let Promise = require('bluebird');
let types = require('./avro-types');

// get two defined Avro types
let fuel_rate_type = types.fuel_rate_type;
let gps_latlon_type = types.gps_latlon_type;
let fr_map_type = types.fr_map_type;

let gps_msg_buf = [];
let fr_msg_buf = [];
let data = {};

// Creat gps message consumer
let Consumer = kafka.Consumer;
let gps_client = new kafka.Client('vip1.ecn.purdue.edu:2181');
let gps_consumer = new Consumer(
		gps_client,
		[
			{
				topic: 'gps-latlon',
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

// Creat fuel rate message consumer
let fr_client = new kafka.Client('vip1.ecn.purdue.edu:2181');
let fr_consumer = new Consumer(
		fr_client,
		[
			{
				topic: 'fuel-rate',
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

// Create Kafka producer
let Producer = kafka.Producer;
let prod_client = new kafka.Client('vip1.ecn.purdue.edu:2181');
let producer = Promise.promisifyAll(new Producer(prod_client));

producer.on('ready', function() {
	gps_consumer.on('message', function(message) {
		let gps_buf = gps_latlon_type.fromBuffer(message.value);
		gps_msg_buf.push(gps_buf);
		gps_matcher();
	});

	fr_consumer.on('message', function(message) {
		let fr_buf = fuel_rate_type.fromBuffer(message.value);
		fr_msg_buf.push(fr_buf);
		gps_matcher();
	});
});

producer.on('error', function(err) {
	console.error(err);
	process.exit();
});

// Create work promise/queue
let work = Promise.resolve();

function gps_matcher() {
	let used_fr_index;
	let closest_gps_index;
	fr_msg_buf.every(function(fr_msg, fr_index) {
		// find the index of gps ts that is greater than the fuel rate ts
		let index = gps_msg_buf.findIndex((gps_msg) => gps_msg.timestamp >= fr_msg.timestamp);
		let closest_gps_index;

		if (index < 0) {
			return false;
		} else if (index === 0) {
			closest_gps_index = index;
		} else {
			/*
			console.log('index: %d', index);
			console.log('%d:', fr_msg.timestamp - gps_msg_buf[index].timestamp);
			console.log('%d:', fr_msg.timestamp - gps_msg_buf[index - 1].timestamp);
			*/
			closest_gps_index =
				Math.abs(fr_msg.timestamp - gps_msg_buf[index].timestamp) <
				Math.abs(fr_msg.timestamp - gps_msg_buf[index - 1].timestamp) ?
				index : index - 1;
		}

		used_fr_index = fr_index;

		/*
		console.log('closest gps index: %d of length %d', closest_gps_index, gps_msg_buf.length);
		console.log('fr ts:', fr_msg.timestamp, 'gps ts:', gps_msg_buf[closest_gps_index].timestamp);
		*/
		console.log(gps_msg_buf[closest_gps_index].timestamp, gps_msg_buf[closest_gps_index].lat,
								gps_msg_buf[closest_gps_index].lon, fr_msg.fuelrate)
		let tx_buf = fr_map_type.toBuffer({
				timestamp: gps_msg_buf[closest_gps_index].timestamp,
				lat: gps_msg_buf[closest_gps_index].lat,
				lon: gps_msg_buf[closest_gps_index].lon,
				fuelrate: fr_msg.fuelrate
		});

		let payloads = [{
				topic: 'fr-map',
				messages: tx_buf
		}];

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
		
		gps_msg_buf = gps_msg_buf.slice(closest_gps_index);

		return true;
	});
	if (used_fr_index !== undefined) {
		fr_msg_buf = fr_msg_buf.slice(used_fr_index + 1);
	}
}

process.on('exit', function() {
	gps_client.close();
	fr_client.close();
	prod_client.close();
	consumer.commit();
	consumer.close();
});
