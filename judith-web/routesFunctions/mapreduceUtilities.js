module.exports = 
{


	showCollectionsMapReduceOneStep : function( db, callback ){
		
	    db.collectionNames(function(err, collections) {
			
			if (err) throw err;

				var retorno = [];
				for(var i in collections) {		

					collectionName = collections[ i ]['name'];
					
					if(collectionName.indexOf('mapReduce_') > -1) {	
						console.log('--- Collection_name: 	',collectionName);
					    collectionName = collectionName.replace('sebastiana.', '')
						retorno.push( collectionName );
					}
				}
		
	 			callback( retorno );
		});

	},



	insereDadosNoCacheEchamaMapreduce : function(db, estado, campos, data, usuario, res)
	{
		var context = this;
		res.redirect('/waiting/' + data + '/' + estado);


		db.collection('cacheInformation').insert({ 'uf' : estado, 'keys' : campos, '_id' : data, 'user' : usuario.usuario, 'status' : 'EM ANDAMENTO'}, function(err, result) 
		{
    		context.defineFuncoesMapEreduce(db, estado, campos, data, usuario);
		});
	},

	defineFuncoesMapEreduce : function (db, estado, campos, data, usuario){

		console.log('Aplicando map reduce..')
		console.log(data)

		var context = this;
		var mapFunction = function(){

			if(this.telefone != undefined && this.ddd != undefined  ){

				for (var key in this) { 
					var objeto = {};

					for (var i in campos){
						objeto[campos[i]] = this[campos[i]];
					}
					objeto["key"] = data;
				}
				emit(objeto, 1); 
			}
		};

		var reduceFunction = function(object, qtd){ 
			return  Array.sum(qtd);
		};

		console.log('montando a funcao map_Reduce')
		context.executaFuncaoMapreduce (db, estado, campos, data, usuario, mapFunction, reduceFunction)
	},

	executaFuncaoMapreduce : function (db, estado, campos, data, usuario, mapFunction, reduceFunction)
	{
		var context = this;

		db.command(
					{
						mapReduce: estado,
						map: mapFunction.toString(),
						reduce: reduceFunction.toString(),
						out: {reduce: 'mapReduceDetalhado'},
						scope: {campos : campos, data : data}
					} 

					, function (err, logMapreduce) 
					{
						console.log( logMapreduce )
						context.insereNoCacheInformacoesDaExecucaoDoMapreduce(db, data, logMapreduce, estado);
					});
	},

	insereNoCacheInformacoesDaExecucaoDoMapreduce : function (db, data, logMapreduce, estado)
	{
		var context = this;
		console.log( logMapreduce )
		db.collection('cacheInformation').update( { '_id' : data }, {$set : { mapreduceInformation : logMapreduce }}, function(err, resposta) 
		{ 
			context.atualizaQtddElementosGeradosNoMapreduce(db, data, estado);
		});
		
	},

	atualizaQtddElementosGeradosNoMapreduce : function(db, data, estado)
	{

		db.collection('mapReduceDetalhado').count({'_id.key' : data}, function(err, counts) 
		{
			db.collection('cacheInformation').update( { '_id' : data }, {$set : {'mapreduceInformation.counts.output' : counts,  'status' : 'FINALIZADA'}}, function(err, resultado) 
			{ 
				if (err) throw err;
			});
		});
	},

	retornaMapreduceDetalhado : function(db, paginaAtual, data, res, estado)
	{
		var context = this;
		var limite = 1000;
		var skipPage = paginaAtual * limite;
		var campos = [];
		var qtddPaginas;

		db.collection('cacheInformation').find({"_id" : data}).toArray(function (err, resultado){

			campos = resultado[0].keys;
			qtddPaginas = (resultado[0].mapreduceInformation.counts.output) / limite;
		
			context.defineRespostaDeAcordoComParametro(db, res, limite, skipPage, campos, qtddPaginas, paginaAtual, data,estado);
		
		});
	},


	defineRespostaDeAcordoComParametro : function(db, res, limite, skipPage, campos, qtddPaginas, paginaAtual, data, estado)
	{
		var context = this;

		context.retornaRespostaOrdenadaPeloQuantidade(db, data, skipPage, limite, res, campos, qtddPaginas, paginaAtual, estado);
		/*if( (context.verificaSeExisteCidadeNosParametros(campos)) ){
			context.retornaRespostaOrdenadaPelaCidade(db, data, skipPage, limite, res, campos, qtddPaginas, paginaAtual, estado);
		}else{
			console.log('sem ordenacao por cidade')
			context.retornaRespostaOrdenadoPeloPrimeiroItem(db, data, skipPage, limite, res, campos, qtddPaginas, paginaAtual, estado);
		}*/

	},


	retornaRespostaOrdenadaPeloQuantidade : function (db, data, skipPage, limite, res, campos, qtddPaginas, paginaAtual, estado){
		
		var context = this;

		db.collection('mapReduceDetalhado').find({"_id.key" : data}).sort({ "value" : -1}).skip(skipPage).limit(limite).toArray(
					function (err, resultado){

							array_result  = []
							for( result in resultado ){
								resultMapReduce = resultado[result]['_id'];
								if( context.isTodosCamposPreenchidos(resultMapReduce,campos )){
									array_result.push( resultado[result] );
								}
							}
				
							res.render('mapreduceDetalhado',{
								"resultado" : array_result,
								"campos" : campos,
								"qtddPaginas" : qtddPaginas,
								"paginaAtual" : paginaAtual,
								"estado" : estado,
							});
						
					});
	},


	isTodosCamposPreenchidos : function( resultMapReduce, camposPesquisa){
		for( i in camposPesquisa){
			if(resultMapReduce[camposPesquisa[i]]  == undefined ){
				return false;
			}
		}
		return true;
	},

	/*

	verificaSeExisteCidadeNosParametros : function (campos)
	{
		for(i = 0; i <= campos.length; i++)
		{
			if (campos[i] == "cidade") 
			{ 
				return true;
				break;
			};
		}
		return false;
	},


	retornaRespostaOrdenadaPelaCidade : function (db, data, skipPage, limite, res, campos, qtddPaginas, paginaAtual, estado)
	{
		db.collection('mapReduceDetalhado').find({"_id.key" : data}).sort({ "_id.cidade" : 1}).skip(skipPage).limit(limite).toArray(function (err, resultado)
		{	

			res.render('mapreduceDetalhado', 
			{
				"resultado" : resultado,
				"campos" : campos,
				"qtddPaginas" : qtddPaginas,
				"paginaAtual" : paginaAtual,
				"estado" : estado,
			});
		});
	},



	retornaRespostaOrdenadoPeloPrimeiroItem : function (db, data, skipPage, limite, res, campos, qtddPaginas, paginaAtual, estado)
	{

		console.log({"_id.key" : data})
		db.collection('mapReduceDetalhado').find({"_id.key" : data}).sort({ "_id" : 1}).skip(skipPage).limit(limite).toArray(function (err, resultado)
		{	

			res.render('mapreduceDetalhado', 
			{
				"resultado" : resultado,
				"campos" : campos,
				"qtddPaginas" : qtddPaginas,
				"paginaAtual" : paginaAtual,
				"estado" : estado,
			});
		});
	},


	*/


	ordernaResultadosMapReduce : function( resultado ){
		var resposta = {};
		list_sort=[];
		for (var r in resultado) 
			list_sort.push( r )
		list_sort.sort()

		for(var r in list_sort){
			for (var result in resultado) {
				if( result ==list_sort[r] ){
		 			resposta[result] = resultado[result];
		 			break;
		 		}
			}
		}
		return resposta;
	},

	transformaListaMapReduceEmJson : function( results, callback ){

		var lista = {};
		var resultado = [];

		for( j in results){
			var docs = results[j];
			for( var i in docs ) {

			   var linha = docs[ i ];
			   var uf	= linha._id.uf;
			   var chave = linha._id.chave;
			   var quantidade = linha.value;
			   if( !resultado[ uf ] ) 
				   resultado[ uf ] = [];

			   var dado = { "chave": "", "quantidade": "" };

			   dado[ "chave" ] = chave;
			   dado[ "quantidade" ] = quantidade;
			   resultado[ uf ].push( dado );
			}
		}

		resposta = this.ordernaResultadosMapReduce( resultado );
		callback( resposta );
	}
}

