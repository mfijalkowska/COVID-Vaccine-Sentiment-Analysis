package com.covid.storm;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.topology.TopologyBuilder;

/**
 * @author Magdalena Fijalkowska
 * 
 * Topology class that sets up the Storm topology in local mode. Please note that
 * Twitter API credentials file - twitter4j.properties - has to be placed in the root directory
 * 
 * @link http://twitter4j.org/en/configuration.html#systempropertyconfiguration
 *
 */
public class Topology {

	static final String TOPOLOGY_NAME = "apache-storm-covid-vaccine-twitter-sentiment-analysis";

	public static void main(String[] args) {
		Config config = new Config(); //create the default config object
		config.setMessageTimeoutSecs(120);
		TopologyBuilder b = new TopologyBuilder(); //create the topology object
		
		//define new Spout
		b.setSpout("TwitterSpout", new TwitterSpout());
		//define new Bolt with parallelism of 1 thread
        b.setBolt("SortingBolt", new SortingBolt()).shuffleGrouping("TwitterSpout"); 
        //define new Bolt with parallelism of 1 thread
        b.setBolt("PositiveBolt", new PositiveBolt()).shuffleGrouping("SortingBolt");
        //define new Bolt with parallelism of 1 thread
        b.setBolt("NegativeBolt", new NegativeBolt()).shuffleGrouping("PositiveBolt");
        //define new Bolt with parallelism of 1 thread
        b.setBolt("ScoreBolt", new ScoreBolt()).shuffleGrouping("NegativeBolt");

        //create an in-process cluster object
		final LocalCluster cluster = new LocalCluster();
		//create the topology and submit with config
		cluster.submitTopology(TOPOLOGY_NAME, config, b.createTopology());

		Runtime.getRuntime().addShutdownHook(new Thread() {
			@Override
			public void run() {
				//kill the topology
				cluster.killTopology(TOPOLOGY_NAME);
				//shutdown the local cluster
				cluster.shutdown();
			}
		});

	}

}

