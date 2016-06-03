'use strict';

let Parser = require('binary-parser').Parser;

module.exports = {
	'65266': new Parser()
		.uint16le('fuel_rate_lhr', {
			formatter: (state) => {
				return (state * 0.05);
			}
		})
		.uint16le('engine_inst_fuel_economy_kml', {
			formatter: (state) => {
				return (state / 512);
			}
		})
		.uint16le('engine_avg_fuel_economy_kml', {
			formatter: (state) => {
				if (state === 0xFB00) {
					return Infinity;
				}
				return (state / 512);
			}
		})
		.uint8('engine_throttle_valve_1_pos', {
			formatter: (state) => {
				return (state * 0.4);
			}
		})
		.uint8('engine_throttle_valve_2_pos', {
			formatter: (state) => {
				return (state * 0.4);
			}
		}),
	'61444': new Parser()
		.skip(3)
		.uint16le('engine_rpm', {
			formatter: (state) => {
				return (state * 0.125);
			}
		}),
	'65091': new Parser()
		.uint16le('rear_pto_rpm', {
			formatter: (state) => {
				return (state * 0.125);
			}
		}),

/**************************************************************
* binary-parser (or javascript) doesn't support 64 bit numbers.
* Need to parse out high bytes and low bytes and add them
* together.
**************************************************************/

	'129029': new Parser()
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
		}),

	'65488': new Parser()
		.uint32le('yield', {
			formatter: (state) => {
				return (state * .0000189545096358038);
			}
		})
}
