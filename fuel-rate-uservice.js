'use strict';

let kafka = require('kafka-node');
let Promise = require('bluebird');
let Parser = require('binary-parser').Parser;
let colors = require('colors');
let type = require('./avro-types');
let pgns = require('./pgns');

function fuel_rate_uservice() {
	let raw_isobus_type = type.raw_isobus_type;
	let fuel_rate_type = type.fuel_rate_type;

	// Creat Kafka consumer
	let Consumer = kafka.Consumer;
	let cons_client = new kafka.Client('vip4.ecn.purdue.edu:2181');
	let consumer = new Consumer(
			cons_client,
			[
				{
					topic: 'raw-can1',
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
	let prod_client = new kafka.Client('vip4.ecn.purdue.edu:2181');
	let producer = Promise.promisifyAll(new Producer(prod_client));

	// Create work promise/queue
	let work = Promise.resolve();

	// define producer topic name
	let fr_topic = 'fr';

	// create topic
	producer.createTopics([fr_topic], false, function(err, data) {});

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
			if (p && rx_buf.pgn === 65266) {
				data = p.parse(new Buffer(rx_buf.data, 'hex'));

				let tx_buf = fuel_rate_type.toBuffer({
					timestamp: rx_buf.timestamp,
					fuelrate: data.fuel_rate_lhr
				});

				payloads = [{
					topic: fr_topic,
					messages: tx_buf
				}];

				console.log(colors.green('fuel_rate_uservice | ts:', rx_buf.timestamp,
							'fuel rate:', data.fuel_rate_lhr));
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
}

module.exports = fuel_rate_uservice;
