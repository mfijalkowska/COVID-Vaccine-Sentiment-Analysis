package com.covid.storm;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import twitter4j.HashtagEntity;
import twitter4j.Status;
import twitter4j.UserMentionEntity;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * @author Magdalena Fijalkowska
 * Receives tweets and emits its words over a certain length.
 */
public class SortingBolt extends BaseRichBolt {

	private static final long serialVersionUID = 5151173513759399636L;
    private OutputCollector collector;
    private final int minWordLength = 0;
    private int numOfTweets = 0;
    //set of words to ignore
    private Set<String> IGNORE_LIST = new HashSet<String>(Arrays.asList(new String[] {"http", "https"}));
    //set of hashtags to look for
	private Set<String> HASHTAG = new HashSet<String>(Arrays.asList(new String[] {
			"covid", "covid19", "vaccine", "covidvaccine", "covid19vaccine", "coronavirus",
	}));
	//set of languages to only look at - eng only
	private Set<String> languages = new HashSet<String>(Arrays.asList(new String[] {"en"}));
    
    
    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector collector) {
        this.collector = collector;
    }

    @Override
    public void execute(Tuple input) {
        Status tweet = (Status) input.getValueByField("tweet"); //get the passed input tweet
        String lang = tweet.getUser().getLang(); //extract lang
        String text = tweet.getText().replaceAll("\\p{Punct}", " ").replaceAll("\\r|\\n", "").toLowerCase(); //extract tweet text
        String[] words = text.split(" "); //separate words and put them into an array
        String newTweet = ""; //initialize new tweet without the irrelevant words
        numOfTweets++; //increment the count
        
        if (lang != "en") {
            return;
        }
       
        // create the new tweet with filtered out irrelevant words
        for (String word : words) {
            if (word.length() >= minWordLength && !IGNORE_LIST.contains(word)) {
                newTweet = newTweet + word + " ";
            }
        }
        //get hsashtags from the tweet
        HashtagEntity hashtags[] = tweet.getHashtagEntities();
        //check if the tweet contains covid related hashtags
        for (HashtagEntity hashtag : hashtags) {
        	String hashtagText = hashtag.getText().toLowerCase();
        	//if hashtag doesn't match move to the next
        	if (!this.HASHTAG.contains(hashtagText)) {
        		continue;
        	}
        	//emit newTweet if covid related hashtag found
            collector.emit(new Values(newTweet,numOfTweets));
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("newTweet","numOfTweets"));
    }
}




