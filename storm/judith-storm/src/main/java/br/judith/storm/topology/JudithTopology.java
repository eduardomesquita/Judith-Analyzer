/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package br.judith.storm.topology;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.topology.TopologyBuilder;
import br.judith.storm.bouts.FilterPossibleStudents;
import br.judith.storm.bouts.SaveTwitters;
import br.judith.storm.bouts.SaveTypeOfStudents;
import br.judith.storm.spouts.TwitterSpout;

/**
 *
 * @author eduardo
 */
public class JudithTopology {
    
    private TopologyBuilder builder = null;
    private LocalCluster cluster = null;
    private Config config = null;

    public JudithTopology(boolean debug, int qts_paralelismo) {

        builder = new TopologyBuilder();
        cluster = new LocalCluster();
        config = new Config();
        
        config.setDebug(debug);
        config.setMaxTaskParallelism(qts_paralelismo);
   
    }

    public void start() {
       
        builder.setSpout("twiiter_feeds_spouts", new TwitterSpout(), 1);
        builder.setBolt("processa_twitters", new SaveTwitters())
                .shuffleGrouping("twiiter_feeds_spouts");
        
        builder.setBolt("filter_possible_students_twitter", 
                        new FilterPossibleStudents(), 1)
                        .shuffleGrouping("processa_twitters");
        
        builder.setBolt("save_type_of_students", new SaveTypeOfStudents(), 1)
                           .shuffleGrouping("filter_possible_students_twitter");
//        
//        builder.setBolt("processa_log", new ProcessaLog(), 1)
//                      .shuffleGrouping("conexao_sebastiana");
        
        cluster.submitTopology("judith-strom", config,
                builder.createTopology());
    }

    public void shutdown() {
        cluster.shutdown();
    }
    
    
    
    
    
}
