$(function () {

            doGetServer('/porcentStudents', {}, function(data){

                

                        $('#container-estudantes').highcharts({
                                chart: {
                                    plotBackgroundColor: null,
                                    plotBorderWidth: null,//null,
                                    plotShadow: false
                                },
                                title: {
                                    text: 'Estudantes: ( monitorando ' + data['response']['total'] + ' tweets )'
                                },
                                tooltip: {
                                    pointFormat: '{series.name}: <b>{point.percentage:.0f}%</b>'
                                },
                                plotOptions: {
                                    pie: {
                                       allowPointSelect: true,
                                       cursor: 'pointer',
                                        dataLabels: {
                                            enabled: false
                                        },
                                        showInLegend: true
                                    }
                                },
                            series: [{
                            type: 'pie',
                            name: 'Students',
                            data: [
                                 {
                                    name: 'Alunos',
                                    y: data['response']['possible'],
                                    sliced: true,
                                    selected: true
                                },
                                ['Possíveis Alunos',   data['response']['student']],
                               
                            ]
                        }]
                    });

                    
            });    

});