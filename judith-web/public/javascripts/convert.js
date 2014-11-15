$(document).ready(function() {
    converterData();
    converterMilissegundos();
});

function converterMilissegundos () {
    if ($(".Milis"))
    {
        $(".Milis").each(function()
        {
            valor = $(this).attr('id');
            var milis = $('#'+valor).text();
            milis = new Number(milis);
            var milliseconds = parseInt((milis%1000)/100);
            var seconds = parseInt((milis/1000)%60);
            var minutes = parseInt((milis/(1000*60))%60);
            var hours = parseInt((milis/(1000*60*60))%24);

            hours = (hours < 10) ? "0" + hours : hours;
            minutes = (minutes < 10) ? "0" + minutes : minutes;
            seconds = (seconds < 10) ? "0" + seconds : seconds;

            var tempo = hours + ':' + minutes + ':' + seconds;
            $('#'+ valor).text(tempo);
        });
    }
}

function converterData(){
    if ($(".Data"))
    {
        $(".Data").each(function()
        {
            valor = $(this).attr('id');
            var dataAntiga = $('#'+valor).text();
            dataAntiga = new Number(dataAntiga);
            var today = new Date(dataAntiga);
            var ss = today.getDay();
            var dd = today.getDate();
            var hr = today.getHours();
            var mn = today.getMinutes();
            var mm = today.getMonth()+1; //January is 0!

            if(dd<10){
                dd='0'+dd
            }

            if(mm<10){
                mm='0'+mm
            } 

            if(mn < 10){
                mn = '0'+mn
            }

            if(hr < 10){
                hr = '0'+hr
            }

            var diaSemana= '';
            if(ss == 0)
            diaSemana = " Domingo, "
             
            else if(ss == 1)
            diaSemana = " Segunda - Feira, "
             
            else if(ss == 2)
            diaSemana = " Terça - Feira, "
             
            else if(ss == 3)
            diaSemana = " Quarta - Feira, "
             
            else if(ss == 4)
            diaSemana = " Quinta - Feira, "
             
            else if(ss == 5)
            diaSemana = " Sexta - Feira, "
             
            else if(ss == 6)
            diaSemana = " Sábado, "

            var novaData = dd+'/'+mm + ' às '+ hr+':'+mn;
            $('#'+valor).text(novaData);
        });
    }
}