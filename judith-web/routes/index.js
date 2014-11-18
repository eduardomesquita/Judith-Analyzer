
__URL_PORCENT_STUDENTS__ = 'http://0.0.0.0:5222/api/v.1/students/porcentStatus'
__URL_GET_KEYWORDS__     = 'http://0.0.0.0:5222/api/v.1/mediassocais/getkeywords'
__URL_DELETE_KEYWORDS__  = 'http://0.0.0.0:5222/api/v.1/mediassocais/excluirKeywords'
__URL_SALVAR_KEYWORDS__  = 'http://0.0.0.0:5222/api/v.1/mediassocais/salvarKeywords'
__URL_GET_MAP_REDUCE__   = 'http://0.0.0.0:5222/api/v.1/mapreduce/getmapreduces'
__URL_FIND_STUDENTS__    = 'http://0.0.0.0:5222/api/v.1/estudantes/getstatusestudantes'

module.exports = function(app, passport){
	
	var loginUtilities = require ("../routesFunctions/login.js");
	var requestUtilies = require ("../routesFunctions/requestsServer.js");
	var express = require('express');
	var router = express.Router();

	app.use('/',router);

	router.get('/', function(req, res){
		res.render('login', { title: 'Express' });
	});

	router.get('/login',  function(req, res){
		res.render('login');
	});

	router.post('/login', function(req, res, next) {
  		
  		passport.authenticate('local-login', function(err, user, info) {
		    if (err) { return next(err); }
		    if (user === false) { return res.redirect('/login'); }

			req.logIn(user, function(err) {
				if (err) { return next(err); }
					return res.redirect('/index');
				});
		})(req, res, next);
	});
	

	router.get('/logout', function(req, res){
		 req.logout();
  		 res.redirect('/');
	});

	router.get('/graphs', function(req, res){
		res.render('graphs.html');
	});

	router.get('/index', function(req, res){
		res.render('index');
	});

	router.get('/midiassociais', function(req, res){
			url = __URL_GET_KEYWORDS__;
		 	requestUtilies.request( url, function( json_resquests ){

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
			res.render('midiasSociais',{	"resultado" : response_json,
											"campos" : ['Palavras' , 'Mídia Social', 'Linguagem', 'Última Atualização'],
										});
		});
	});

	router.post('/excluirpesquisamidia' ,function(req, res){
			url = __URL_DELETE_KEYWORDS__;
			requestUtilies.requestPost( url, req.body, function( json_resquests ){
			if(json_resquests.status == 'ok'){
					res.json({ 'response' : json_resquests.status });
			 }
		    });

	});


	router.post('/salvarpesquisamidia' ,function(req, res){
		url = __URL_SALVAR_KEYWORDS__ 
		requestUtilies.requestPost( url, req.body, function( json_resquests ){
			if(json_resquests.status == 'ok'){
				res.redirect('/midiassociais')
			 }
	    });
	});


	router.get('/mapreduce', function(req, res){
		
		url = __URL_GET_MAP_REDUCE__;
		mapreduce = {};

		requestUtilies.request( url, function( json_resquests ){

			for(i in json_resquests){

				nome = json_resquests[i].emr_name;
				mapreduce[nome] = [json_resquests[i]];
				
			}
			res.render('mapreduce', {'mapreduce' : mapreduce});

		});
	});


	router.get('/estudantes', function(req, res){
		res.render('estudantes', {		
									'selected' : 'Estudantes',
									'resultado' : {},
									'campos' : ['Status' , 'Usuario', 'Cidade', 'Total tweets']});
	});


	router.get('/findestudantes', function(req, res){
		res.redirect('back');
	});

	router.post('/findestudantes', function(req, res){

		url = __URL_FIND_STUDENTS__ + '/' + req.body.status
		
		requestUtilies.request( url, function( json_resquests ){

		    resultado  = []
			for(i in json_resquests){
				json_tmp = {}
				json_tmp.usuario = json_resquests[i].userName;

				console.log(json_resquests[i].status);
				if(json_resquests[i].statusUsers == 'student')
					json_tmp.status = 'ESTUDANTE';
				else if(json_resquests[i].statusUsers == 'possible')
					json_tmp.status = 'POSSIVEL ESTUDANTE';

				json_tmp.total = json_resquests[i].totalTweet;
				json_tmp.location = json_resquests[i].location;

				resultado.push(json_tmp);
			}


			res.render('estudantes', {		
								'selected' : 'Estudantes',
								'resultado' : resultado,
								'campos' : ['Status' , 'Usuario', 'Cidade', 'Total tweets']});

		});

		
	});

	





	router.get('/porcentStudents', function(req, res){
		url = __URL_PORCENT_STUDENTS__;
		requestUtilies.request( url, function( json_resquests ){

			response=json_resquests;	
			
		 	response['total'] = parseInt(json_resquests['possible'])  +
		 						parseInt(json_resquests['student']); 
		 	res.json({ 'response' : response });	
		});
	});



	

	app.use('/',router);
	
}
