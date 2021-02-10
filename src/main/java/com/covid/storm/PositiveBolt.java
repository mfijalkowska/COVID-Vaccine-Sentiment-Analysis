package com.covid.storm;


import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

import twitter4j.Status;

import java.util.Arrays;
import java.util.Map;
import java.util.Set;
import java.util.HashSet;

import java.lang.*;

/**
 * @author Magdalena Fijalkowska
 * Receives tweets and emits its words over a certain length.
 */

public class PositiveBolt extends BaseRichBolt {
	private static final long serialVersionUID = 1L;
	private OutputCollector collector;
	private final int minWordLength = 0;
	
	//set of positive words
	private Set<String> POSITIVE_LIST = new HashSet<String>(Arrays.asList(new String[] {
			"accepted", "acclaimed", "accomplish", "accomplishment", "achievement", "action", "active", 
			"admire", "affirmative", "affluent", "agree", "agreeable", "amazing", "angelic", "appealing", "approve", 
			"aptitude", "attractive", "awesome", "beaming", "beautiful", "believe", "beneficial", "bliss", "bountiful", "bounty", 
			"brave", "bravo", "brilliant", "bubbly", "calm", "celebrated", "certain", "champ", "champion", "charming", "cheery", 
			"choice", "classic", "classical", "clean", "commend", "composed", "congratulation", "constant", "cool", "courageous", 
			"creative", "cute", "dazzling", "delight", "delightful", "distinguished", "divine", "earnest", "easy", "ecstatic", 
			"effective", "effervescent", "efficient", "effortless", "electrifying", "elegant", "enchanting", "encouraging", "endorsed", 
			"energetic", "energized", "engaging", "enthusiastic", "essential", "esteemed", "ethical", "excellent", "exciting", 
			"exquisite", "fabulous", "fair", "familiar", "famous", "fantastic", "favorable", "fetching", "fine", "fitting", 
			"flourishing", "fortunate", "free", "fresh", "friendly", "fun", "funny", "generous", "genius", "genuine", "giving", 
			"glamorous", "glowing", "good", "gorgeous", "graceful", "great", "green", "grin", "growing", "handsome", "happy", 
			"harmonious", "healing", "healthy", "hearty", "heavenly", "honest", "honorable", "honored", "hug", "idea", "ideal", 
			"imaginative", "imagine", "impressive", "independent", "innovate", "innovative", "instant", "instantaneous", "instinctive", 
			"intuitive", "intellectual", "intelligent", "inventive", "jovial", "joy", "jubilant", "keen", "kind", "knowing", 
			"knowledgeable", "laugh", "legendary", "light", "learned", "lively", "lovely", "lucid", "lucky", "luminous", "marvelous", 
			"masterful", "meaningful", "merit", "meritorious", "miraculous", "motivating", "moving", "natural", "nice", "novel", 
			"now", "nurturing", "nutritious", "okay", "one", "one-hundred percent", "open", "optimistic", "paradise", "perfect", 
			"phenomenal", "pleasurable", "plentiful", "pleasant", "poised", "polished", "popular", "positive", "powerful", 
			"prepared", "pretty", "principled", "productive", "progress", "prominent", "protected", "proud", "quality", "quick", 
			"quiet", "ready", "reassuring", "refined", "refreshing", "rejoice", "reliable", "remarkable", "resounding", "respected", 
			"restored", "reward", "rewarding", "right", "robust", "safe", "satisfactory", "secure", "seemly", "simple", "skilled", 
			"skillful", "smile", "soulful", "sparkling", "special", "spirited", "spiritual", "stirring", "stupendous", "stunning", 
			"success", "successful", "sunny", "super", "superb", "supporting", "surprising", "terrific", "thorough", "thrilling", 
			"thriving", "tops", "tranquil", "transforming", "transformative", "trusting", "truthful", "unreal", "unwavering", "up", 
			"upbeat", "upright", "upstanding", "valued", "vibrant", "victorious", "victory", "vigorous", "virtuous", "vital", 
			"vivacious", "wealthy", "welcome", "well", "whole", "wholesome", "willing", "wonderful", "wondrous", "worthy", 
			"wow", "yes", "yummy", "zeal", "zealous", 	
	}));
	
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector collector) {
        this.collector = collector;
    } 

    public void execute(Tuple input) {
    	String tweet = (String) input.getValueByField("newTweet"); // read incoming tuple data - new Tweet without irrelevant words
    	int numOfTweets = (Integer) input.getValueByField("numOfTweets"); //read incoming tuple data - number of tweets
    	int[] scores = new int[2]; //create an array for storing scores
    	String text = tweet;
        String[] words = text.split(" "); //split the tweet and store each word separately in an array
        int positiveCount = 0; //set the count to 0 for every incoming tweet
        for (String word : words) {
        	//increment the positive score if the LIST contains any word from the tweet
            if (word.length() >= minWordLength && POSITIVE_LIST.contains(word)) {
                positiveCount++;
            }
        }
        scores[0] = positiveCount; 
        collector.emit(new Values(tweet, scores, numOfTweets)); //emit tweet, scores and num of tweets to the next bol
    }

    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("tweet", "scores","numOfTweets")); //declare the output columns names
    }
}