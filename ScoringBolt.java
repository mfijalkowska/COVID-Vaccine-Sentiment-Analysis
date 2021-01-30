package com.kaviddiss.storm;

import java.io.BufferedWriter;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;

import java.util.Map;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import twitter4j.Status;

public class ScoringBolt extends BaseRichBolt {

	/**
	 * @author Ali Abod
	 */
	private static int totalTweets;

	@Override
	public void prepare(Map map, TopologyContext topologyContext, OutputCollector collector) {
	}

	public void scoreCounter(int positiveWords, int negativeWords, String username, String dateTime, long tweetID) {

		final int neutralScore = 0;
		final int positiveScore = 1;
		final int negativeScore = -1;
		String whereToWrite = "output.csv";

		FileWriter fw = new FileWriter(whereToWrite, true);
		BufferedWriter bw = new BufferedWriter(fw);
		PrintWriter pw = new PrintWriter(bw);

		if (totalTweets < 1) {
			pw.append("Date" + "," + "Tweet ID" + "," + "UserName" + "," + "Positive/Negative");
			System.out.println("Date" + "," + "Tweet ID" + "," + "Positive/Negative");
			pw.append("\n");
		}

		if (positiveWords != negativeWords) {

			if (positiveWords > negativeWords) {
				pw.append(dateTime + "," + tweetID + "," + username + "," + positiveScore);
				pw.append("\n");
				totalTweets += 1;
			} else {
				pw.append(dateTime + "," + tweetID + "," + username + "," + negativeScore);
				pw.append("\n");
				totalTweets += 1;
			}
		} else {
			pw.append(dateTime + "," + tweetID + "," + username + "," + neutralScore);
			pw.append("\n");
			totalTweets += 1;
		}
		pw.flush();
		pw.close();
	}

	@Override
	public void execute(Tuple input) {
		String location = (String) input.getValueByField("location");
		int positiveWords = (int) input.getValueByField("positiveWords");
		int negativeWords = (int) input.getValueByField("negativeWords");
		long tweetID = (long) input.getValue(2);
		String username = (String) input.getValueByField("username");
		Date creationDate = (Date) input.getValueByField("dateTime");
		String dateTime = creationDate.toString();

		SimpleDateFormat parser = new SimpleDateFormat("EEE MMM d HH:mm:ss zzz yyyy");
		Date date;
		try {
			date = parser.parse(dateTime);
			SimpleDateFormat formatter = new SimpleDateFormat("dd/MM/yyyy HH:mm");
			dateTime = formatter.format(date);
		} catch (ParseException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		this.scoreCounter(positiveWords, negativeWords, username, dateTime, tweetID);
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("tweetID", "screenName", "dateTime", "text", "positiveWords", "negativeWords"));
	}
}
