module.exports = 
{
	estarLogado : function (req, res, next)
	{
        if( req.isAuthenticated() || req.url.indexOf('login') >= 0 )
			return next();
  
        res.redirect('/login')
	}
}