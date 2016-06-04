'use strict';

let fuel_rate_uservice = require('./fuel-rate-uservice');
let gps_uservice = require('./gps-latlon-uservice');
let mapper_uservice = require('./mapper-uservice');

fuel_rate_uservice();
gps_uservice();
mapper_uservice();
