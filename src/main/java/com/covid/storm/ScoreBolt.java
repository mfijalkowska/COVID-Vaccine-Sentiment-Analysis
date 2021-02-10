package com.covid.storm;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * @author Magdalena Fijalkowska
 * Receives tweets and emits its words over a certain length.
 */

public class ScoreBolt extends BaseRichBolt {
 
	private int[] scoreBoard= {0,0}; //create an array for storing the scores
	private OutputCollector collector; 
	private int neutral = 0;
	private int gap =0;
	private int oldNum =0;
	private int sentiment =0;
	public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
		this.collector = collector;
	}

	public void execute(Tuple input) {
		int[] scores = (int[]) input.getValueByField("scores");
		int numOfTweets = (Integer) input.getValueByField("numOfTweets");
		gap = numOfTweets - oldNum;
		oldNum = numOfTweets;
		if(scores[0] > scores[1]) { //if positive score > negative
			scoreBoard[0]++; //increment the number of positive tweets
			sentiment = 1;
		}
		else if(scores[1] > scores[0]) {
			scoreBoard[1]++; //otherwise increment the num of negative tweets
			sentiment = -1;
		}
		else {
			neutral++; //otherwise increment the num of negative tweets
			sentiment = 0;
		}
		System.out.println( "Sentiment: " + sentiment + "        " + "Num of Tweets: " + numOfTweets + "     " + "Gap: " + gap);
		System.out.println("Positive tweets: " + scoreBoard[0] + "         " + "Negative tweets: " + scoreBoard[1]+ "        "   + "Neutral: "  + neutral);		
	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) { //no next tuple to be emited
	}
	
}
