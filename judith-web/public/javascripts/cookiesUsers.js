function setCookie(name,exdays){   //função universal para criar cookie
    var expires;
    var date; 
    var value;

    date = new Date(); //  criando o COOKIE com a data atual
    date.setTime(date.getTime()+(exdays*24*60*60*1000));
    expires = date.toUTCString();
    value = "TESTE123";
    document.cookie = name+"="+value+"; expires="+expires+"; path=/";
}


function getCookie()
{
    var c_name = document.cookie; // listando o nome de todos os cookies
    if(c_name!=undefined && c_name.length > 0) {
        
        var posCookie = c_name.indexOf(cookieSeuNome); // checando se existe o cookieSeuNome 
        if (posCookie >= 0){
            alert("Cookie Existe!!!" +posCookie);
        }
    else
    alert("Cookie não existe!!!");
}