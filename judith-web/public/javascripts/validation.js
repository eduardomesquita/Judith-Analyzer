function verificaChecks() {

    var aChk = document.getElementsByName("campos"); 
    var cont = 0

    for (var i=0;i<aChk.length;i++){ 

        if (aChk[i].checked == true){ 
            cont++;
    	}
    }

    if(cont <= 1)
    {
    	alert("Ops, é preciso que estejam selecionados no mínimo dois campos para que sua pesquisa seja feita.");
    	return false;
    }

    if(cont > 1)
    {	
    	return true;
    }
}
