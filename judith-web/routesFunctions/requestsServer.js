var request = require('request');

module.exports = {

    request : function(url, callback) {
        default_headers = {'Content-type': 'application/json'};

        request({
            url: url,
            headers: default_headers,
            method: 'GET'
        } , function(error, response, body) {
            if (!error && response.statusCode === 200) {
                var json = JSON.parse(body);
                callback(json);
            }else{
                console.log(error);
            }
        });
    },


    requestPost : function(url, data, callback) {
      
        request.post( url, { form: data },
                        function (error, response, body) {
                            if (!error && response.statusCode == 200) {
                                callback(JSON.parse(body));
                            }
                        }
                      );
    },



}