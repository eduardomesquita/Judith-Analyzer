
module.exports = 
{

	salvarCarrinhoNaSessao : function(db, data, collectionName){
		carrinho.itens = [carrinho.itens]
	 	carrinho.statusCarrinho = 'ABERTO';
		db.collection( collectionName ).insert( data, function(){
			console.log('novo carrinho salvo');
		});
	},

	
	transformaEmArraySeNecessario : function ( objeto ){

		if( typeof objeto === 'string' ) {
    		objeto = [ objeto ];
		}
		
		return objeto
	},


	recebeDadosEenviaParaAdicionarNoJson : function(db, campos, itensDaPesquisa, quantidadeDeDadosSolicitados, estado, usuario, res)
	{
		var nomeArquivo = Date.now();
		var context = this;

		itensDaPesquisa = this.transformaEmArraySeNecessario( itensDaPesquisa )
		quantidadeDeDadosSolicitados = this.transformaEmArraySeNecessario( quantidadeDeDadosSolicitados ) 
		
		console.log('recebeDadosEenviaParaAdicionarNoJson');
		console.log(' -- itensDaPesquisa ', itensDaPesquisa );
		console.log(' -- quantidadeDeDadosSolicitados ', quantidadeDeDadosSolicitados );
		console.log(' -- campos ', campos );

  		for (var i = 0; i  < itensDaPesquisa.length; i++)
  		{
  			var listaItensDaPesquisa = itensDaPesquisa[i].split(",");
  			var jsonDaPesquisa = {};

  			jsonDaPesquisa = context.insereDadosNoJson(listaItensDaPesquisa, jsonDaPesquisa, campos);
  			context.pesquisaNoBancoAbaseFinal(db, estado, jsonDaPesquisa, quantidadeDeDadosSolicitados[i], nomeArquivo, campos, usuario, res);
  		}
	},


	pesquisaNoBancoAbaseFinal : function(db, estado, jsonDaPesquisa, quantidadeDeDadosSolicitados, nomeArquivo, campos, usuario, res)
	{
		var context = this;
		var fs = require('fs');

		 db.collection('parametroSistema').find({'chave':'path_arquivo_base'}).limit(1).toArray(
			function (err, resultado){

				path_folder = resultado[0]['valor'];
				limit =  parseInt(quantidadeDeDadosSolicitados);

				db.collection(estado).find(jsonDaPesquisa, {"ddd" : 1, "telefone": 1}).limit(limit).each(
					function(err, resultado) {

						if(resultado){
							var dddTelefone = resultado.ddd + resultado.telefone + '; \n';
							context.insereDadosNoArquivo( path_folder, fs, dddTelefone, nomeArquivo);
						}
						if(resultado == null){	
							context.inserePesquisaNoBanco(db, nomeArquivo, estado, campos, usuario, res);
						}
				});
		});
	},

	inserePesquisaNoBanco : function(db, nomeArquivo, estado, campos, usuario, res)
	{
		db.collection('basesRealizadas').insert({"base" : nomeArquivo, "estado" : estado, "camposPesquisados" : campos}, function()
		{
			res.render('pesquisaFinalizada', { "usuario" : usuario, "nomeArquivo" : nomeArquivo, "estado" : estado});
		});
	},


	insereDadosNoArquivo : function(path_folder, fs, dddTelefone, nomeArquivo){

		path_file = path_folder + nomeArquivo + '.csv';
		console.log(path_file);
		fs.appendFileSync( path_file, dddTelefone, encoding='utf8',function (err) {
						if (err) throw err;
		});
	},

	insereDadosNoJson : function(listaItensDaPesquisa, jsonDaPesquisa, campos){
		for(j = 0; j < campos.length; j++){
			jsonDaPesquisa[ campos[j] ] = listaItensDaPesquisa[j];			
		}
		return jsonDaPesquisa;
	},

	colocaValoresDoCarrinhoNoJson : function(db, itensDaPesquisa, campos, usuario, res, estado)
	{	

		var listaResultadoCarrinho = [];
		var context = this;
		var jsonDaPesquisa = {};
		var quantidadeElementos = 0;
		var listaItensPesquisados = [];
		
		quantidadeElementos = itensDaPesquisa.listaPesquisa.length;

		for(var i = 0; i < itensDaPesquisa.listaPesquisa.length; i++)
		{
			var itensPesquisados = itensDaPesquisa.listaPesquisa[i];

			jsonDaPesquisa = context.insereDadosNoJson(itensPesquisados, jsonDaPesquisa, campos);
		
			context.realizaCount(db, jsonDaPesquisa, usuario, res, campos, listaResultadoCarrinho, quantidadeElementos, estado, listaItensPesquisados, itensPesquisados);
			jsonDaPesquisa = {};
		}
	},

	realizaCount : function(db, jsonDaPesquisa, usuario, res, campos, listaResultadoCarrinho, quantidadeElementos, estado, listaItensPesquisados, itensPesquisados)
	{
		var context = this;

		db.collection(estado).count(jsonDaPesquisa, function(err, resultado) 
		{
			context.organizaResultadoDoCount(resultado, usuario, res, jsonDaPesquisa, campos, listaResultadoCarrinho, quantidadeElementos, estado, listaItensPesquisados, itensPesquisados);
		});
	},

	organizaResultadoDoCount : function(resultado, usuario, res, jsonDaPesquisa, campos, listaResultadoCarrinho, quantidadeElementos, estado, listaItensPesquisados, itensPesquisados)
	{
		var context = this;
		var resultadoCountCarrinho = {};
		
		resultadoCountCarrinho["pesquisa_" + listaResultadoCarrinho.length] = { 'quantidade' : resultado };
		listaItensPesquisados.push(itensPesquisados);
		listaResultadoCarrinho.push(resultadoCountCarrinho);

		context.verificaSeTerminouAPesquisa(usuario, res, listaResultadoCarrinho, campos, listaResultadoCarrinho, quantidadeElementos, estado, listaItensPesquisados);
	},

	verificaSeTerminouAPesquisa : function(usuario, res, listaResultadoCarrinho, campos, listaResultadoCarrinho, quantidadeElementos, estado, listaItensPesquisados)
	{
		var context = this;
		
		if (listaResultadoCarrinho.length == quantidadeElementos)
		{
			context.redirecionaPaginaComResultadosDoCount(usuario, res, listaResultadoCarrinho, campos, estado, listaItensPesquisados);
		}
	},

	redirecionaPaginaComResultadosDoCount : function(usuario, res, listaResultadoCarrinho, campos, estado, listaItensPesquisados)
	{
		res.render('finalizandoPesquisa',
		{
			"resultadoPesquisa" : listaResultadoCarrinho,
			"usuario" : usuario,
			"campos" : campos,
			"itensDaPesquisa" : listaItensPesquisados,
			"estado" : estado
		});
	},

}