/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.sai.storm;

/**
 *
 * @author VedaShree
 */
import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.topology.TopologyBuilder;

/**
 *
 * @author Sai Jami
 * mvn exec:java -Dexec.mainClass="com.sai.storm.Topology"
 */
public class Topology {

    public static void main(String[] args) throws InterruptedException, AlreadyAliveException, InvalidTopologyException {
        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("weather-collector", new ApiStreamingSpout(), 1);
        builder.setBolt("data-extractor", new DataExtractor()).
                shuffleGrouping("weather-collector");
        builder.setBolt("data-saver", new DataSaver()).
                shuffleGrouping("data-extractor");

        Config conf = new Config();
        int i = 0;
        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology("weather-summarizer", conf, builder.createTopology());
    }
}
