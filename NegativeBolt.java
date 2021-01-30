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

public class NegativeBolt extends BaseRichBolt {

	/**
	 * Filters out for negative words in a tweet
	 * 
	 * @author Ali Abod
	 */

	private static final long serialVersionUID = 1L;
	private OutputCollector collector;
	private Set<String> negativeList = new HashSet<String>(Arrays.asList(new String[] {}));

	public void wordFileReader() {

		if (negativeList.isEmpty()) {
			File negativeFile = new File("negative-words.txt");
			Scanner sc = new Scanner(negativeFile);

			while (sc.hasNextLine()) {
				String word = sc.nextLine();
				negativeList.add(word);
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
		if (negativeList.isEmpty()) {
			this.wordFileReader();
		}
		int negativeCount = 0;
		String text = (String) input.getValueByField("text");
		String[] tweetWords = text.split(" ");

		for (String word : tweetWords) {
			if (negativeList.contains(word)) {
				negativeCount += 1;
			}
			collector.emit(new Values(input.getValue(1), input.getValue(2), input.getValue(3), text, input.getValue(5),
					negativeCount));
		}
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("tweetID", "screenName", "dateTime", "text", "positiveCount", "negativeCount"));
	}
}
