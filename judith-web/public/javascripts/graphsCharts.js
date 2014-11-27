$(function () {

            doGetServer('/graphPorcentStudents', {}, function(data){

                        $('#container-estudantes').highcharts({
                                chart: {
                                    plotBackgroundColor: null,
                                    plotBorderWidth: null,//null,
                                    plotShadow: false
                                },
                                title: {
                                    text: 'Tipos de Usuários: ( total ' + data['response']['total'] + ' usuários)'
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





$(function () {


        
        doGetServer('/graphLocationsStudents', {}, function(data){
            
            $('#container-teste').highcharts({
                chart: {
                    type: 'pie',
                    options3d: {
                        enabled: true,
                        alpha: 45,
                        beta: 0
                    }
                },
                title: {
                    text: 'Browser market shares at a specific website, 2014'
                },
                tooltip: {
                    pointFormat: '{series.name}: <b>{point.percentage:.1f}%</b>'
                },
                plotOptions: {
                    pie: {
                        allowPointSelect: true,
                        cursor: 'pointer',
                        depth: 35,
                        dataLabels: {
                            enabled: true,
                            format: '{point.name}'
                        }
                    }
                },
                series: [{
                    type: 'pie',
                    name: 'Browser share',
                    data: [
                        ['Firefox',   45.0],
                        ['IE',       26.8],
                        {
                            name: 'Chrome',
                            y: 12.8,
                            sliced: true,
                            selected: true
                        },
                        ['Safari',    8.5],
                        ['Opera',     6.2],
                        ['Others',   0.7]
                    ]
                }]
            });
    });   


});