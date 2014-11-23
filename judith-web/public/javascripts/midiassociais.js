function excluirPalavra( palavra ){
    doPostServer('/excluirpesquisamidia', {'data':palavra}, function( json ){
         location.reload();
    });   
}