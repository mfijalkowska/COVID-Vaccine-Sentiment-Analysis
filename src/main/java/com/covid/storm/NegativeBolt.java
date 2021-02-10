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

public class NegativeBolt extends BaseRichBolt {
	private static final long serialVersionUID = 1L;
	private OutputCollector collector;
	private final int minWordLength = 0;
	//set of positive words
	private Set<String> NEGATIVE_LIST = new HashSet<String>(Arrays.asList(new String[] {
			"abysmal", "adverse", "alarming", "angry", "annoy", "anxious", "apathy", "appalling", "atrocious", "awful", "bad", 
			"banal", "barbed", "belligerent", "bemoan", "beneath", "boring", "broken", "callous", "can't", "clumsy", "coarse", 
			"cold", "cold-hearted", "collapse", "confused", "contradictory", "contrary", "corrosive", "corrupt", "crazy", "creepy",
			"criminal", "cruel", "cry", "cutting", "dead", "decaying", "damage", "damaging", "dastardly", "deplorable", "depressed", 
			"deprived", "deformed", "deny", "despicable", "detrimental", "dirty", "disease", "disgusting", "disheveled", "dishonest", 
			"dishonorable", "dismal", "distress", "don't", "dreadful", "dreary", "enraged", "eroding", "evil", "fail", "faulty", 
			"fear", "feeble", "fight", "filthy", "foul", "frighten", "frightful", "gawky", "ghastly", "grave", "greed", "grim", 
			"grimace", "gross", "grotesque", "gruesome", "guilty", "haggard", "hard", "hard-hearted", "harmful", "hate", "hideous", 
			"homely", "horrendous", "horrible", "hostile", "hurt", "hurtful", "icky", "ignore", "ignorant", "ill", "immature", 
			"imperfect", "impossible", "inane", "inelegant", "infernal", "injure", "injurious", "insane", "insidious", "insipid", 
			"jealous", "junky", "lose", "lousy", "lumpy", "malicious", "mean", "menacing", "messy", "misshapen", "missing", 
			"misunderstood", "moan", "moldy", "monstrous", "naive", "nasty", "naughty", "negate", "negative", "never", "no", 
			"nobody", "nondescript", "nonsense", "not", "noxious", "objectionable", "odious", "offensive", "old", "oppressive", 
			"pain", "perturb", "pessimistic", "petty", "plain", "poisonous", "poor", "prejudice", "questionable", "quirky", "quit", 
			"reject", "renege", "repellant", "reptilian", "repulsive", "repugnant", "revenge", "revolting", "rocky", "rotten", "rude", 
			"ruthless", "sad", "savage", "scare", "scary", "scream", "severe", "shoddy", "shocking", "sick", "sickening", "sinister", 
			"slimy", "smelly", "sobbing", "sorry", "spiteful", "sticky", "stinky", "stormy", "stressful", "stuck", "stupid", 
			"substandard", "suspect", "suspicious", "tense", "terrible", "terrifying", "threatening", "ugly", "undermine", "unfair", 
			"unfavorable", "unhappy", "unhealthy", "unjust", "unlucky", "unpleasant", "upset", "unsatisfactory", "unsightly", 
			"untoward", "unwanted", "unwelcome", "unwholesome", "unwieldy", "unwise", "upset", "vice", "vicious", "vile", "villainous", 
			"vindictive", "wary", "weary", "wicked", "woeful", "worthless", "wound", "yell", "yucky", "zero",
	}));
	
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector collector) {
        this.collector = collector;
    } 

    public void execute(Tuple input) {
    	String tweet = (String) input.getValueByField("tweet"); //read incoming tuple data - new tweet without the irrelevant words
    	int[] scores = (int[]) input.getValueByField("scores"); //read incoming tuple data  - scores
    	int numOfTweets = (Integer) input.getValueByField("numOfTweets"); // read incoming tuple data - num of tweets
    	System.out.println("Tweet: " + tweet); 
    	String text = tweet;
   	
        String[] words = text.split(" "); //split the tweet and store each word in an array
        int negativeCount = 0; // set the count for negative score to 0
        for (String word : words) {
        	//increment the negative score if the LIST contains any word from the tweet
            if (word.length() >= minWordLength && NEGATIVE_LIST.contains(word)) {
                negativeCount++;
            }
        }
        scores[1] = negativeCount;
        collector.emit(new Values(scores, numOfTweets)); //emit tuple
    }

    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("scores","numOfTweets")); //declare output columns' names
    }
}
