/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package br.judith.storm.bouts;

import backtype.storm.task.ShellBolt;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import br.judith.storm.utils.ScriptsPyhton;
import java.util.Map;

/**
 *
 * @author eduardo
 */
public class SaveTypeOfStudents extends ShellBolt implements IRichBolt {

    public SaveTypeOfStudents() {
        super("python", ScriptsPyhton.getPathBouts() + "savetypeofstudents.py");
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
