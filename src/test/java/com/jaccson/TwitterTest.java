package com.jaccson;


import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.TableExistsException;
import org.apache.accumulo.core.client.TableNotFoundException;

import twitter4j.Status;
import twitter4j.StatusDeletionNotice;
import twitter4j.StatusListener;
import twitter4j.TwitterStream;
import twitter4j.TwitterStreamFactory;
import twitter4j.conf.ConfigurationBuilder;

import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;



/**
 * <p>This is a code example of Twitter4J Streaming API - sample method support.<br>
 * Usage: java twitter4j.examples.PrintSampleStream<br>
 * </p>
 *
 * based on work by Twitter4j author  Yusuke Yamamoto - yusuke at mac.com
 */


public class TwitterTest {


	public static class TWriter implements StatusListener {


		private DBCollection coll;
		private int numTweets = 0;

		public TWriter(DBCollection c)  {
			coll = c;
		}

		public void onStatus(Status status) {

			BasicDBObject tweet = new BasicDBObject();

			tweet.put("screenname", status.getUser().getScreenName());
			tweet.put("text", status.getText());

			coll.insert(tweet.toString());
			numTweets++;
			if(numTweets % 100 == 0) {
				coll.flush();
				System.out.println(numTweets);
			}

			//System.out.println("@" + status.getUser().getScreenName() + " - " + status.getText());
		}

		public void onDeletionNotice(StatusDeletionNotice statusDeletionNotice) {
			System.out.println("Got a status deletion notice id:" + statusDeletionNotice.getStatusId());
		}

		public void onTrackLimitationNotice(int numberOfLimitedStatuses) {
			System.out.println("Got track limitation notice:" + numberOfLimitedStatuses);
		}

		public void onScrubGeo(long userId, long upToStatusId) {
			System.out.println("Got scrub_geo event userId:" + userId + " upToStatusId:" + upToStatusId);
		}

		public void onException(Exception ex) {
			ex.printStackTrace();
		}
	}

	/**
	 * Main entry of this application.
	 *
	 * @param args
	 * @throws AccumuloSecurityException 
	 * @throws AccumuloException 
	 * @throws TableExistsException 
	 * @throws TableNotFoundException 
	 */
	public static void main(String[] args) throws AccumuloException, AccumuloSecurityException {

		Jaccson conn = new Jaccson("localhost:2181", "test", "root", "secret", null);

		DB db = conn.getDB("test");
		DBCollection coll = db.getCollection("tweets");

		ConfigurationBuilder cb = new ConfigurationBuilder();

		cb.setDebugEnabled(true)
		.setUser(args[0])
		.setPassword(args[1]);


		TwitterStream twitterStream = new TwitterStreamFactory(cb.build()).getInstance();

		twitterStream.addListener(new TWriter(coll));
		twitterStream.sample();

		for(DBObject o : coll.find("{\"word\": \"love\"}", null)) {
			System.out.println(o);
		}
	}
}

