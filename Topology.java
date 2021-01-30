package com.kaviddiss.storm;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.topology.TopologyBuilder;

/**
 * Topology class that sets up the Storm topology Please note that Twitter
 * credentials have to be provided as VM args, otherwise you'll get an
 * Unauthorized error.
 * 
 * @link http://twitter4j.org/en/configuration.html#systempropertyconfiguration
 */
public class Topology {

	static final String TOPOLOGY_NAME = "storm-twitter-covid-analysis";

	public static void main(String[] args) {
		Config config = new Config();
		config.setMessageTimeoutSecs(120);

		TopologyBuilder b = new TopologyBuilder();
		b.setSpout("TwitterSpout", new TwitterSpout());
		b.setBolt("SortingBolt", new SortingBolt(4)).shuffleGrouping("TwitterSpout");
		b.setBolt("PositiveBolt", new PositiveBolt()).shuffleGrouping("SortingBolt");
		b.setBolt("NegativeBolt", new NegativeBolt()).shuffleGrouping("PositiveBolt");
		b.setBolt("ScoringBolt", new ScoringBolt()).shuffleGrouping("NegativeBolt");

		final LocalCluster cluster = new LocalCluster();
		cluster.submitTopology(TOPOLOGY_NAME, config, b.createTopology());

		Runtime.getRuntime().addShutdownHook(new Thread() {
			@Override
			public void run() {
				cluster.killTopology(TOPOLOGY_NAME);
				cluster.shutdown();
			}
		});

	}

}
