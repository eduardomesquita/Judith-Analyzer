function removeUsername( usuario ){
    doPostServer('/removeblacklist', {'username':usuario}, function( response ){
        location.reload();
    });
}
