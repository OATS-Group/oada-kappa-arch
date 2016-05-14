'use strict';

let avro = require('avsc');

let raw_isobus_type = avro.parse({
	name: 'rawisobus',
	type: 'record',
	fields: [
		{ name: 'timestamp', type: 'long' },
		{ name: 'pgn', type: 'int' },
		{ name: 'data', type: 'bytes' }
	]
});

exports.raw_isobus = raw_isobus_type;
