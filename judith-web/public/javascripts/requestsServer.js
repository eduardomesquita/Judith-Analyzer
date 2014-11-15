function doJsonPost( url_request, data, done){   
    $.post( url_request, data,  function( data ) {
          done(data);
    });
}
