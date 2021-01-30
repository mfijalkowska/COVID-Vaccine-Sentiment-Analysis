package com.kaviddiss.storm;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import twitter4j.Status;

import java.io.File;
import java.io.FileNotFoundException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Scanner;
import java.util.Set;

public class PositiveBolt extends BaseRichBolt {

	/**
	 * Filters for positive words in a tweet
	 * 
	 * @author Ali Abod
	 */
	private static final long serialVersionUID = 12332;

	private Set<String> positiveList = new HashSet<String>(Arrays.asList(new String[] {}));
	private OutputCollector collector;

	public void wordFileReader() {
		if (positiveList.isEmpty()) {
			File positiveFile = new File("positive-words.txt");
			Scanner sc = new Scanner(positiveFile);

			while (sc.hasNextLine()) {
				String word = sc.nextLine();
				positiveList.add(word);
			}
			sc.close();
		}
	}

	@Override
	public void prepare(Map map, TopologyContext topologyContext, OutputCollector collector) {
		this.collector = collector;
	}

	@Override
	public void execute(Tuple input) {
		if (positiveList.isEmpty()) {
			this.wordFileReader();
		}
		int positiveCount = 0;
		String text = (String) input.getValueByField("text");
		String[] tweetWords = text.split(" ");

		for (String word : tweetWords) {
			if (positiveList.contains(word)) {
				positiveCount += 1;
			}
		}
		collector.emit(new Values(input.getValue(1), input.getValue(2), input.getValue(3), text, positiveCount));
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("tweetID", "screenName", "dateTime", "text", "positiveCount"));
	}
}