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


    get_midia_social : function ( json_resquests ){

        response_json = [];
        media_social = 'TWITTER';
            
        for(i in json_resquests){
            palavra = ultimaAtualizacao = '';
            linguagem = json_resquests[i].language;
            if(linguagem == 'pt'){
                linguagem = 'PT-BR';
        }
        ultimaAtualizacao = json_resquests[i].last_tweet_text;
        for(word in json_resquests[i].keysWords){
            palavra += '#' + json_resquests[i].keysWords[word] + ' ';
        }
        
        response_json.push( {'media': media_social, 
                             'palavra': palavra.toUpperCase(),
                             'linguagem' : linguagem,
                              'ultimaAtualizacao' : ultimaAtualizacao.toUpperCase().substring(0, 45)
                            });
        }

       return response_json;
    },

}