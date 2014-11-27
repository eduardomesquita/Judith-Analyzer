function execurar_emr( name ){

    alert(name);
    
    doPostServer('/executarEmr', {'name':name}, function( response ){
    
    });
}


function callAlert( msg ){
    console.log('alert');
    alert(msg);
   
}