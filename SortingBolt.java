package com.kaviddiss.storm;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

import twitter4j.HashtagEntity;
import twitter4j.Status;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.lang.reflect.Array;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Recieves tweet, and filters out incorrect tweetHashtags, none-english tweets.
 */
public class SortingBolt extends BaseRichBolt {

	private Set<String> ignoreList = new HashSet<String>(Arrays.asList(new String[] { "http", "https" }));
	private OutputCollector collector;

	private List<String> HASH_TAGS = Arrays.asList("covid19", "coronavirus", "corona", "covid", "covidvaccine",
			"vaccine", "vaccination", "covid19vaccine");
	private long prevTweetID;

	public SortingBolt() {
	}

	@Override
	public void prepare(Map map, TopologyContext topologyContext, OutputCollector collector) {
		this.collector = collector;
	}

	@Override
	public void execute(Tuple input) {
		// Pull data from tweet into local variables
		Status tweet = (Status) input.getValueByField("tweet");
		String screenName = tweet.getUser().getScreenName();
		String text = tweet.getText().toLowerCase();
		String lang = tweet.getLang();
		Date dateTime = tweet.getCreatedAt();
		long tweetID = tweet.getUser().getId();
		// Pull Hashtags contained in tweet into list
		List<HashtagEntity> tweetHashtags = Arrays.asList(tweet.getHashtagEntities());

		if ("en".equals(lang) && prevTweetID != tweetID && tweetHashtags != null && tweetHashtags.size() > 0) {
			// Filter relevance of tweet by hashtags
			for (HashtagEntity hashtag : tweetHashtags) {
				if (HASH_TAGS.contains(hashtag.getText().toLowerCase())) {
					collector.emit(new Values(tweetID, screenName, dateTime, text));
				}
			}
		}
		prevTweetID = tweetID;
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("tweetID", "screenName", "dateTime", "text"));
	}
}
