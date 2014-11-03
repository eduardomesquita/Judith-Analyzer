/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package br.judith.storm.topology;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.topology.TopologyBuilder;
import br.judith.storm.bouts.SaveUsersTwitters;
import br.judith.storm.bouts.SaveTwitters;
import br.judith.storm.bouts.FilterTwitter;
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
       
        builder.setSpout("EMIT_TWEET", new TwitterSpout(), 1);
        
        builder.setBolt("FILTER_TWEET", new  FilterTwitter())
                        .shuffleGrouping("EMIT_TWEET");
        
        builder.setBolt("SAVE_TWEET",  new SaveTwitters(), 1)
                        .shuffleGrouping("FILTER_TWEET");
        
        builder.setBolt("SAVE_USERS_TWEET", new SaveUsersTwitters(), 1)
                           .shuffleGrouping("SAVE_TWEET");
//        
//        builder.setBolt("processa_log", new ProcessaLog(), 1)
//                      .shuffleGrouping("conexao_sebastiana");
        
        cluster.submitTopology("JUDITH-TOPOLOGY", config,
                builder.createTopology());
    }

    public void shutdown() {
        cluster.shutdown();
    }
    
    
    
    
    
}
