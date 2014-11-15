$(document).ready(function()
{
    openCarrinhoDePesquisas();
    retornarCarrinho()
});


var listaDePesquisas = [];
var posItem = 0;

function openCarrinhoDePesquisas(){
  /*$(".mostrarCarrinhoPesquisas").hide();
  $(".ocultarCarrinhoPesquisas").click(function()
  {
    $(this).next(".mostrarCarrinhoPesquisas").slideToggle(600);
  });
*/
}


function initCarrinho( result ){

  for(var i =0; i <result.meuCarrinho.length;i++ ){
    carrinho = result.meuCarrinho[i];
    campos = [];
      
      for(key in carrinho ){
          if(key != 'UF'){
            campos.push( carrinho[key] );
          }
      }

     if(campos.length > 0){
       listaDePesquisas[ posItem ] = campos;
       posItem++;
     }
     
  }
}

function retornarCarrinho(){
    doJsonPost( '/retornarCarrinho', {}, function( result ){
        if( result.meuCarrinho.length > 0 ){
            initCarrinho(result);
        }
    })

    escreverTotalTabela();
}


function verificaItensSelecionados(indexLinha, cabecalho, UF){

  var rows = document.getElementById("myTable").rows[0].cells;
  var campos = [];
  var boxes = document.getElementsByName("linha_" + indexLinha);
  for(var i = 0; i < boxes.length; i++){
      campos.push(boxes[i].id);
  }

  addLinhaNoCarrinhoDePesquisa(campos, boxes.length, cabecalho, UF)
}

function addLinhaNoCarrinhoDePesquisa(campos, quantidade, cabecalho, UF){

  if( listaDePesquisas.length == 0){
    addRowItemCarrinho(campos, cabecalho, UF);
  }else if( metodoContemQueAPorraDoJSNaoTem( campos ) ){
    addRowItemCarrinho(campos, cabecalho, UF);
  }else{
    alert('Item já adicionado..');
  }

}

function metodoContemQueAPorraDoJSNaoTem( campos ){
   for(item in listaDePesquisas ){
     if(String(listaDePesquisas[item]) == String(campos)){
       return false;
     }
   }
  return true;
}

function escreverTotalTabela(){

  var rows = document.getElementById("tableCarrinhoDePesquisa").rows[0].cells;
  rows[0].innerHTML = "Quantidade:";
  rows[1].innerHTML =  sumItensCarrinhos();

}


function addRowItemCarrinho(campos, cabecalho, UF){

  listaDePesquisas[ posItem ] = campos;
  posItem++;
  salvarSessaoCarrinhoCompraBanco( campos, cabecalho, UF );
  escreverTotalTabela();
}

function sumItensCarrinhos(){

  totalCelulares = 0;
  console.log(listaDePesquisas);
  for(i = 0; i < listaDePesquisas.length; i++){
      itens = listaDePesquisas[i];
      totalCelulares += parseInt(itens[ itens.length -1 ]);
  }
  return totalCelulares;
}

function retornaCarrinho ( estado, campos ) {

  var camposPesquisa = campos.split(',');
  var carrinhoDePesquisa = {
    'listaPesquisa' : listaDePesquisas,
    'campos' : camposPesquisa,
    'estado' : estado
  };
  window.location = "/finalizandoPesquisa/" + JSON.stringify(carrinhoDePesquisa);
}


function salvarSessaoCarrinhoCompraBanco( itensCarrinho, cabecalho, UF){
       
     colluns = cabecalho.split(',');
     keys = []
     for(var i =0; i<colluns.length;i++){
         keys.push(colluns[i])
     }
     keys.push('Quantidade')

    requests = {'UF': UF};
    for(var i =0; i<itensCarrinho.length;i++){
        requests[ keys[ i ] ] =  itensCarrinho[i];
    }
    doJsonPost( '/salvarCarrinho', {'itens' :requests }, function( data ){ 
    }) 
}


function findCamposPesquisa(){
  var camposPesquisa = [];
  var campos = document.getElementsByName("cabecalhoPesquisa");
  
  for(var i = 0; i < campos.length; i++){
      camposPesquisa.push(campos[i].id);
  }
  return camposPesquisa;
}



