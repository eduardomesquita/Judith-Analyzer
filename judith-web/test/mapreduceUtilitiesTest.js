var assert = require('assert');
var mapreduceUtilities = require('../routesFunctions/mapreduceUtilities');

describe('mapreduceUtilities', function(){

	describe('#transformaListaMapReduceEmJson()', function(){

		var consultaMockVazio = "";
		var consultaMockNormal = [[{"_id":{"chave":"bairro","uf":"AC"},"value":56478}],[{"_id":{"chave":"bairro","uf":"AP"},"value":231654}]];
		var objetoVazio = {};
		var objetoEsperado = {"AC":[{"chave":"bairro","quantidade":56478}],"AP":[{"chave":"bairro","quantidade":231654}]};

		it('deve retornar objeto vazio ao passar parametro vazio', function(done){
			
			mapreduceUtilities.transformaListaMapReduceEmJson( consultaMockVazio, function(objeto){
				
				objeto = JSON.stringify( objeto );
				objetoVazio = JSON.stringify( objetoVazio );
				assert.equal( objetoVazio, objeto );
				done();
			});
		})

		it('deve retornar esperado  ao passar parametro consultaMockNormal', function(done){	
			mapreduceUtilities.transformaListaMapReduceEmJson( consultaMockNormal, function(objeto){
				objeto = JSON.stringify( objeto );
				objetoEsperado = JSON.stringify( objetoEsperado );
				assert.equal( objetoEsperado, objeto );
				done();
			});

		})
	})

})