/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package br.judith.storm.spouts;

import backtype.storm.spout.ShellSpout;
import backtype.storm.topology.IRichSpout;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import br.judith.storm.utils.ScriptsPyhton;
import java.util.Map;

/**
 *
 * @author eduardo
 */
public class TwitterSpout  extends ShellSpout implements IRichSpout {
    
    public TwitterSpout() {
       super("python", ScriptsPyhton.getPathSpouts() + "twitterspout.py");
    }
    
    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("json"));
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        return null;
    }
    
}
