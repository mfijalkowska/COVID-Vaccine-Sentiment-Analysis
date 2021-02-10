package com.covid.storm;

import backtype.storm.Config;
import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;
import twitter4j.*;

import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;


	
/**
 * @author Magdalena Fijalkowska
 * A Spout that uses Twitter streaming API to read incoming tweets in real time
 * 
 */

@SuppressWarnings("rawtypes") 
public class TwitterSpout extends BaseRichSpout {
	//this output collector exposes the API for emitting tuples to the next bolt
	private SpoutOutputCollector collector; 
	private LinkedBlockingQueue<Status> queue; //shared queue for getting buffering tweets
	private TwitterStream twitterStream; //

	public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
		queue = new LinkedBlockingQueue<Status>(1000); //buffeer to block the tweets
		this.collector = collector;
		//listening on the tweet stream
		StatusListener listener = new StatusListener() {

			public void onException(Exception ex) {
			}

			public void onStatus(Status status) {
				queue.offer(status);
			}

			public void onDeletionNotice(StatusDeletionNotice statusDeletionNotice) {				
			}

			public void onTrackLimitationNotice(int numberOfLimitedStatuses) {				
			}

			public void onScrubGeo(long userId, long upToStatusId) {			
			}

			public void onStallWarning(StallWarning warning) {				
			}
			
		};
		
		TwitterStreamFactory factory = new TwitterStreamFactory(); //create twitter stream factory with config
		twitterStream = factory.getInstance(); //get an instance of twitter stream
		twitterStream.addListener(listener); //handler for the twitter stream
		twitterStream.sample();
	}

	public void nextTuple() {
		//try to pick a tweet from the buffer
		Status ret = queue.poll();
		if (ret == null) { //if no tweet available, wait
			Utils.sleep(50);
		}
		else {
			collector.emit(new Values(ret));
		}
	}
	
	public void close() {
		twitterStream.shutdown(); //shutdown the stream
	}
	
	public Map<String, Object> getComponentConfiguration() {
		Config ret = new Config(); //create config component
		ret.setMaxTaskParallelism(1); //set the parallelism for this spout to 1
		return ret;
	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("tweet")); //the output tuple consists of a column "tweet"
	}
}





