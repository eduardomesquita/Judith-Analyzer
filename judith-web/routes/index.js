__URL_PORCENT_STUDENTS__ = 'http://0.0.0.0:5222/api/v.1/students/porcentStatus'

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
					return res.redirect('/graphs');
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
