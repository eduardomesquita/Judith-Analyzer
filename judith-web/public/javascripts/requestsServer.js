function doGetServer( url_request, data, done){   
    $.get( url_request, data,  function( data ) {
          done(data);
    });
}
