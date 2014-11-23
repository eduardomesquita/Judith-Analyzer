function visualizarEstudantes( usuario ){
    doPostServer('/visualizarestudantes', {'usuario':usuario}, function( response ){
        document.write( response );
    });
}


function insereBlackList( usuario ){
    doPostServer('/insereusuarioblacklist', {'username':usuario}, function( response ){
       alert('Usuário ' + usuario + ' inserido na blacklist !');
       location.reload();
    });
}



function reload(  ){
   location.reload();
   
}