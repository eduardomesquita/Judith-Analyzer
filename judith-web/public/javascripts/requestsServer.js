function doGetServer( url_request, data, done){   
    $.get( url_request, data,  function( data ) {
          done(data);
    });
}



function doPostServer( url_request, data, done){   
    $.post( url_request, data,  function( data ) {
          done(data);
    });
}

