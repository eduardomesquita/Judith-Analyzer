var db = require('../banco.js');

module.exports = function(){
    var asyncController = { 
        findCollectionName : function( name, callback ){
           var collection =  db.collection( name );
           collection.find().toArray( callback );
        }       
    };
    return asyncController;
};