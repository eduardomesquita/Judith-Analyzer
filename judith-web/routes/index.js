IP = '0.0.0.0'
PORT = '5222'
SERVER = IP+':'+PORT

URL_GRAPHS_PORCENT_STUDENTS  = 'http://'+SERVER+'/api/v.1/graphs/estudantes/porcentStatus'
URL_GRAPHS_LOCATION_STUDENTS = 'http://'+SERVER+'/api/v.1/graphs/estudantes/location'
URL_GET_KEYWORDS         	 = 'http://'+SERVER+'/api/v.1/mediassocais/get/tweet/keywords'
URL_DELETE_KEYWORDS      	 = 'http://'+SERVER+'/api/v.1/mediassocais/delete/tweet/Keywords'
URL_SAVE_KEYWORDS        	 = 'http://'+SERVER+'/api/v.1/mediassocais/save/tweet/keywords'
URL_GET_MAP_REDUCE       	 = 'http://'+SERVER+'/api/v.1/mapreduce/get/mapreduces'
URL_FIND_STUDENTS        	 = 'http://'+SERVER+'/api/v.1/estudantes/get/status/<params>'
URL_FIND_TWEET_USER      	 = 'http://'+SERVER+'/api/v.1/estudantes/get/tweet/usersname/<params>'
URL_INSERE_BLACKLIST     	 = 'http://'+SERVER+'/api/v.1/estudantes/blacklist/usersname/'
URL_GET_BLACKLIST        	 = 'http://'+SERVER+'/api/v.1/estudantes/get/blacklist/'
URL_REMOVE_BLACKLIST     	 = 'http://'+SERVER+'/api/v.1/estudantes/remove/blacklist/'
URL_UPDATE_CONFIG		 	 = 'http://'+SERVER+'/api/v.1/configuracoes/update/'
GET_CONFIG		         	 = 'http://'+SERVER+'/api/v.1/configuracoes/get/'
EXECUTE_EMR 			 	 = 'http://'+SERVER+'/api/v.1/configuracoes/executar/'
FIND_LOGS 				 	 = 'http://'+SERVER+'/api/v.1/logs/find/'

module.exports = function(app, passport){
	
	var loginUtilities = require ("../routesFunctions/login.js");
	var requestUtilies = require ("../routesFunctions/requestsServer.js");
	var estudanteController = require ("../routesFunctions/estudantesController.js");
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

	// GRAFICOS

	router.get('/graphs', function(req, res){
		res.render('graphs.html');
	});

	router.get('/graphPorcentStudents', function(req, res){

		requestUtilies.request(URL_GRAPHS_PORCENT_STUDENTS, function( json_resquests ){
			response_object=json_resquests[0].values;
			total = parseInt(response_object['possible'])  + parseInt(response_object['student']); 
		 	response_object['total'] = total;
		 	res.json({ 'response' : response_object });	
		});
	});


	router.get('/graphLocationsStudents', function(req, res){


		requestUtilies.request(URL_GRAPHS_LOCATION_STUDENTS, function( json_resquests ){
			
				locations = json_resquests[0].values;
				for(i in locations){
					console.log(locations[i].location);
				}
			

			res.json({ 'response' : {} });
		});

	});




	// HOME

	router.get('/index', function(req, res){

		requestUtilies.request(FIND_LOGS,function( json_resquests ){
			
			scripts = {'primeiro' : 'Word-Count', 'segundo' : 'Find-Students', 'terceiro' : 'Blacklist'}
			res.render('index', {'response' : json_resquests, 'scripts': scripts});
		});		
		
	});



	// Midias sociais

	router.get('/midiassociais', function(req, res){
		
		requestUtilies.request(URL_GET_KEYWORDS, function( json_resquests ){

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
			requestUtilies.requestPost(URL_DELETE_KEYWORDS, req.body, function( json_resquests ){
			if(json_resquests.status == 'ok'){

				    
					res.json({ 'response' : json_resquests.status});
			 }
		    });

	});


	router.post('/salvarpesquisamidia' ,function(req, res){

		if(req.body.keysWords == ''){
		requestUtilies.requestPost(URL_SAVE_KEYWORDS, req.body, function( json_resquests ){
			if(json_resquests.status == 'ok'){
				res.redirect('/midiassociais')
			 }
	    });
		}
	});





	// MAPREDUCE



	router.get('/mapreduce', function(req, res){
		mapreduce = {};
		requestUtilies.request(URL_GET_MAP_REDUCE, function( json_resquests ){
			for(i in json_resquests){
				nome = json_resquests[i].emr_name;
				mapreduce[nome] = [json_resquests[i]];
			}
			res.render('mapreduce', {'mapreduce' : mapreduce});
		});
	});






	// ESTUDANTES





	router.post('/estudantesPost', function(req, res){
		res.redirect('/estudantes')
	});

	router.get('/estudantes', function(req, res){
		url = URL_FIND_STUDENTS.replace(/<params>/g,'student');
		requestUtilies.request( url, function( json_resquests ){
			resultado = estudanteController.getEstudantesByStatus( json_resquests);
		
			res.render('estudantes', {		
								'selected' : 'Estudantes',
								'resultado' : resultado,
								'campos' : ['Status' , 'Usuário', 'Cidade', 'Total tweets']});
		});
	});


	router.get('/findestudantes', function(req, res){
		res.redirect('back');
	});

	router.post('/findestudantes', function(req, res){
		url = URL_FIND_STUDENTS.replace(/<params>/g,  req.body.status);
		requestUtilies.request( url, function( json_resquests ){

			resultado = estudanteController.getEstudantesByStatus( json_resquests);
			res.render('estudantes', {		
								'selected' : req.body.status,
								'resultado' : resultado,
								'campos' : ['Status' , 'Usuário', 'Cidade', 'Total tweets']});
		});
	});

	router.post('/visualizarestudantes', function(req, res){
		
		url = URL_FIND_TWEET_USER.replace(/<params>/g, req.body.usuario);
		requestUtilies.request(url, function( json_resquests ){
			res.render('tweetsusers',{ "resultado" : json_resquests, "user" :req.body.usuario});
		});

	});



	// BLACKLIST TWEET


	router.post('/insereusuarioblacklist', function(req, res){

		requestUtilies.requestPost(URL_INSERE_BLACKLIST, req.body, function( json_resquests ){
			if(json_resquests.status == 'ok'){
				res.json( json_resquests );
			 }
	    });

	});


	router.get('/blacklist', function(req, res){
		
		requestUtilies.request(URL_GET_BLACKLIST,function( json_resquests ){
			res.render('blacklist',{ "resultado" : json_resquests, 'campos' : ['Usuário', 'Data Inserção']});
	    });

	});


	router.post('/removeblacklist', function(req, res){
		
		requestUtilies.requestPost(URL_REMOVE_BLACKLIST, req.body, function( json_resquests ){
			if(json_resquests.status == 'ok'){
				res.json( json_resquests );
			 }
	    });

	});




	// CONFIGURAÇÕES



	router.get('/configuracoes', function(req, res){
		
		requestUtilies.request(GET_CONFIG,function( json_resquests ){
			console.log(json_resquests[0])
			res.render('configuracoes',{'resposta': json_resquests[0]});

		});		
	});

	router.post('/configuracoes', function(req, res){

		requestUtilies.requestPost(URL_UPDATE_CONFIG, req.body, function( json_resquests ){
			res.redirect('/configuracoes')
	    });
	});


	router.post('/executarEmr', function(req, res){
		requestUtilies.requestPost(EXECUTE_EMR, req.body, function( json_resquests ){
			console.log( json_resquests );
	    });
	});


	router.get('/configuracoes', function(req, res){
		
		requestUtilies.request(GET_CONFIG,function( json_resquests ){
			console.log(json_resquests[0])
			res.render('configuracoes',{'resposta': json_resquests[0]});

		});		
	});





	// 

	app.use('/',router);	
}