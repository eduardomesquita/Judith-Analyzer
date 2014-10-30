/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package br.judith.storm.utils;

/**
 *
 * @author eduardo
 */
public class ScriptsPyhton {

    private static final String localPath = System.getProperty("user.dir");
    
    public static String getPathSpouts() {
        return localPath + "/python/spouts/";
    }

    public static  String getPathBouts() {
        return localPath + "/python/bouts/";
    }

}
