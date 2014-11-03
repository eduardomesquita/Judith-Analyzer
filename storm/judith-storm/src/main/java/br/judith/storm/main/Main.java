package br.judith.storm.main;

import br.judith.storm.topology.JudithTopology;
public class Main {
    public static void main(String[] args) throws Exception {
        JudithTopology topology = new JudithTopology(true, 1);
        topology.start();   
    }
}
