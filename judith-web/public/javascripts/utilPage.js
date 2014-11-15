function reloadPage() 
{
    setInterval(function(){location.reload()}, 15000);
}
    
function marcarEdesmarcarCheckbox(linha, context)
{
  var boxes = document.getElementsByName("linha_" + linha);
  for(var i = 0; i < boxes.length; i++)
  {
    if(boxes[i].checked)
      boxes[i].checked = false;

    else
      boxes[i].checked = true;
  }
}


function trocaTexto(context) 
{
  if (typeof context !== 'undefined' && context!='') 
    context.innerHTML = (context.innerHTML=='Marcar linha') ? 'Desmarcar ' : 'Marcar linha';
}

function mudaLinhaDeCor(context) 
{
  var tr = $(context).parent().parent();
    if($(context).is(':checked')) $(tr).addClass('selected');
    else $(tr).removeClass('selected');
}

$(function()
{
  $('table > tbody > tr > td > :checkbox').click('click change', function()
  {
    mudaLinhaDeCor(this);
  });

  $('#pesquisar').keydown(function(){
    var encontrou = false;
    var termo = $(this).val().toLowerCase();
    $('table > tbody > tr').each(function(){
      $(this).find('td').each(function(){
        if($(this).text().toLowerCase().indexOf(termo) > -1) encontrou = true;
      });
      if(!encontrou) $(this).hide();
      else $(this).show();
      encontrou = false;
    });
  });

  $("table") 
    .tablesorter({
      dateFormat: 'uk',
      headers: {
        0: {
          sorter: false
        },
        5: {
          sorter: false
        }
      }
    })
});



