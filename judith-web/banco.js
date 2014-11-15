var mongo = require('mongoskin');
module.exports = mongo.db("mongodb://localhost:27017/judith-model", {native_parser:true});
