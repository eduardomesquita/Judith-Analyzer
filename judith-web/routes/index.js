module.exports = function(app, passport)
{
	var mapreduceUtilities = require("../routesFunctions/mapreduceUtilities.js");
	var loginUtilities = require ("../routesFunctions/login.js");
	var carrinhoDePesquisaUtilities = require ("../routesFunctions/carrinhoDePesquisas.js");
	var express = require('express');
	var router = express.Router();

	app.use(loginUtilities.estarLogado,function(req, res, next){
		next();
	});

	router.get('/', function(req, res) 
	{
		res.render('index', { title: 'Express' });
	});


	router.get('/login',  function(req, res){
		res.render('login');
	});

	router.get('/pesquisaFinalizada',  function(req, res){
		res.render('pesquisaFinalizada');
	});

	/*router.post('/login',  passport.authenticate('local-login',{
	      successRedirect  : '/mapreduce',
	      failureRedirect : '/mapreduce'
	}));*/

	router.post('/login', function(req, res, next) {
  		
  		passport.authenticate('local-login', function(err, user, info) {
		    if (err) { return next(err); }
		    if (user === false) { return res.redirect('/login'); }

		    req.logIn(user, function(err) {
		      if (err) { return next(err); }

		      return res.redirect('/mapreduce');
		    });

		  })(req, res, next);
	});

	router.get('/logout', function(req, res){
		 req.logout();
  		 res.redirect('/');
	});



	router.get('/mapreduce', function(req, res) 
	{

		var db = req.db;

		mapreduceUtilities.showCollectionsMapReduceOneStep( db, function( collectionsName ){

				var async = require('async');
				var asyncController = require('../routesFunctions/AsyncController.js')();

				async.map( collectionsName, asyncController.findCollectionName, function(err, results){  
					
					var resposta = mapreduceUtilities.transformaListaMapReduceEmJson( results, 
						function( resposta ){
									res.render('mapreduce', { 
								 	"mapreduce" : resposta
						});
					});
				});
		});
	});

	
	app.use('/',router);

	router.get('/download/:file(*)', function(req, res, next)
	{
		var db = req.db;
		db.collection('parametroSistema').find({'chave':'path_arquivo_base'}).toArray(
			function (err, resultado){
				path_file = resultado[0].valor + req.params.file + '.csv';
				res.download(path_file);
			});

	});

	router.post('/extraindoDados', function(req, res) 
	{
		var db = req.db;
		var usuario = req.user.usuario;
		var campos = req.body.campos;
		var itensDaPesquisa = req.body.itensDaPesquisa;
		var quantidadeDeDadosSolicitados = req.body.quantidadeDeDadosSolicitados;
		var estado = req.body.estado;

		if( quantidadeDeDadosSolicitados == ""){
			var pesquisa = itensDaPesquisa.split(',');
			quantidadeDeDadosSolicitados =  String(pesquisa[ pesquisa.length -1 ])
		}

		console.log('Extraindo Dados ---', usuario);
		console.log('Extraindo Dados ---', campos);
		console.log('Extraindo Dados ---', itensDaPesquisa);
		console.log('Extraindo Dados ---', quantidadeDeDadosSolicitados);
		console.log('Extraindo Dados ---', estado);
		console.log('\n\n');

		carrinhoDePesquisaUtilities.recebeDadosEenviaParaAdicionarNoJson(db, campos, itensDaPesquisa, quantidadeDeDadosSolicitados, estado, usuario, res);
	});

	router.get('/finalizandoPesquisa/:listaCarrinho', function(req, res)  
	{
		var db = req.db;
		var usuario = req.user;
		usuario = usuario.usuario;
		var itensDaPesquisa = JSON.parse(req.params.listaCarrinho);
		
		var campos = itensDaPesquisa.campos;
		var estado = itensDaPesquisa.estado;

		carrinhoDePesquisaUtilities.colocaValoresDoCarrinhoNoJson(db, itensDaPesquisa, campos, usuario, res, estado);
	});

	router.get('/historicoPesquisas', function(req, res)
	{
		var db = req.db;
		var usuario = req.user;
		usuario = usuario.usuario;

		db.collection('cacheInformation').find({'user' : usuario}).toArray (function (err, result)
		{
			res.render('historicoPesquisas', 
				{  
					"cache" : result,
					"usuario" : usuario
				});
		})

	});

	router.get('/mapreduceDetalhado/:paginaAtual/:data', function(req, res) 
	{
		var db = req.db;
		var estado = req.params.estado;
		var paginaAtual = parseInt(req.params.paginaAtual);
		var data = parseInt(req.params.data);
		mapreduceUtilities.retornaMapreduceDetalhado(db, paginaAtual, data, res);
	});

	router.get('/waiting/:data/:estado', function(req, res) 
	{
		var db = req.db;
		var data = parseInt(req.params.data);
		var estado = req.params.estado;
		var campos;
		var paginaAtual = 0;

		db.collection('cacheInformation').find({ '_id' : data, 'status' : 'FINALIZADA'}).toArray(function (err, result) 
		{
			if(result == '')
				res.render('waiting', { });
	
			else
				mapreduceUtilities.retornaMapreduceDetalhado(db, paginaAtual, data, res, estado);
		})
	});


	router.get('/mapreduceUpdate', function (req, res){

		res.redirect('back');
	});

	router.post('/mapreduceUpdate', function (req, res)  
	{
		var db = req.db;
		var data = Date.now();

		var oldId = parseInt(req.body.oldId);
		var idButton = req.body.button;
		var estado = req.body.estado;
		var campos = req.body.campos;
		var usuario = req.user;
		var paginaAtual = 0;

		if (idButton == 'mapreduce')
		{
			mapreduceUtilities.insereDadosNoCacheEchamaMapreduce(db, estado, campos, data, usuario, res);

		   	db.collection('cacheInformation').remove({"_id" : oldId}, 
		   			function (err, result){
							if (err) throw err;
					});

			db.collection('mapReduceDetalhado').remove({"_id.key" : oldId},
				function (err, result){
					if(err) throw err;
				});
		}

		if (idButton == 'cache'){
			
			mapreduceUtilities.retornaMapreduceDetalhado(db, paginaAtual, oldId, res, estado);
		}
	})


	router.post('/cacheInformation', function (req, res){
		
		var db = req.db;
		var estado = req.body.estado;
		var campos = req.body.campos;
		var usuario = req.user;

		var tamanhoCampos = campos.length;
		var data = Date.now();

		db.collection('cacheInformation').find({ $and : [ { "keys" : { $all  : campos }}, { "keys" : {$size : tamanhoCampos} }, { "uf" : estado }  ]}).toArray(function (err, result) {
			if (result.length >= 1 && result[0].mapreduceInformation !== undefined){

			    console.log('esta no cacheInformation ')
				res.render('cacheInformation',{
						"cache" : result,
						"estado" : estado
					});
			}
			else{
				console.log('nao esta no cacheInformation, ')
				mapreduceUtilities.insereDadosNoCacheEchamaMapreduce(db, estado, campos, data, usuario, res);
			}
		});
	});


	app.post('/salvarCarrinho', function(req, res) {

		var db = req.db;
		carrinho = {};
		carrinho.usuario = {};
		carrinho.itens = req.body['itens']
		carrinho.usuario.login = req.user.usuario;
		carrinho.usuario._idUsuario = String(req.user._id);
		collectionsName = 'carrinhoDePesquisa';

		db.collection( collectionsName ).update({'usuario.login': carrinho.usuario.login, 'statusCarrinho' : 'ABERTO'},
				{$push: { 'itens' : carrinho.itens}}, function(err, updated) {
	  		if( err || !updated ){
	  			carrinhoDePesquisaUtilities.salvarCarrinhoNaSessao(db, carrinho, collectionsName );
	  			
	  		}else{
	  		 	console.log("carrinho atualizados");
	  		}

			res.json({'status': 'ok'});
		});

	});


	app.post('/retornarCarrinho', function(req, res) {

		var db = req.db;
		db.collection('carrinhoDePesquisa').find({'usuario.login':req.user.usuario, 'statusCarrinho' : 'ABERTO'}).toArray(
			function (err, result) {

			itens = []
			for(i in result){
				for(j in result[i].itens){
					itens.push(result[i].itens[j])
				}
			}
			
			res.json({ 'meuCarrinho' : itens });	
		});
	});


}
