
module.exports = {


    getEstudantesByStatus : function( json_resquests) {

         resultado  = []
         for(i in json_resquests){
                json_tmp = {}
                json_tmp.usuario = json_resquests[i].userName;
                
                if(json_resquests[i].statusUsers == 'student')
                    json_tmp.status = 'ESTUDANTE';
                else if(json_resquests[i].statusUsers == 'possible')
                    json_tmp.status = 'POSSIVEL ESTUDANTE';

                json_tmp.total = json_resquests[i].totalTweet;
                json_tmp.location = json_resquests[i].location;
                resultado.push(json_tmp);
            }
           
        return resultado;

   },

}
