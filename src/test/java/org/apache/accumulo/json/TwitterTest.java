package org.apache.accumulo.json;


import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.MutationsRejectedException;
import org.apache.accumulo.core.client.TableExistsException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.json.JSONException;
import org.json.JSONObject;

import twitter4j.Status;
import twitter4j.StatusDeletionNotice;
import twitter4j.StatusListener;
import twitter4j.TwitterException;
import twitter4j.TwitterStream;
import twitter4j.TwitterStreamFactory;
import twitter4j.conf.ConfigurationBuilder;

import com.jaccson.JAccSONConnection;
import com.jaccson.JAccSONTable;

/**
 * <p>This is a code example of Twitter4J Streaming API - sample method support.<br>
 * Usage: java twitter4j.examples.PrintSampleStream<br>
 * </p>
 *
 * based on work by Twitter4j author  Yusuke Yamamoto - yusuke at mac.com
 */


public class TwitterTest {


	public static class TWriter implements StatusListener {


		private JAccSONTable table;
		private int numTweets = 0;
		
		public TWriter(JAccSONTable t) throws AccumuloException, AccumuloSecurityException, TableNotFoundException, TableExistsException {
			table = t;
		}

		public void onStatus(Status status) {

			JSONObject tweet = new JSONObject();
			try {
				

				tweet.put("screenname", status.getUser().getScreenName());
				tweet.put("text", status.getText());
				

				try {
					table.insert(tweet.toString());
					numTweets++;
					if(numTweets % 100 == 0) {
						table.flush();
						System.out.println(numTweets);
					}
				} catch (MutationsRejectedException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				} catch (TableNotFoundException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}

			} catch (JSONException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
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
	 * @throws JSONException 
	 */
	public static void main(String[] args) throws TwitterException, AccumuloException, AccumuloSecurityException, TableExistsException, TableNotFoundException, JSONException {

		JAccSONConnection conn = new JAccSONConnection("new-host.home:2181", "test", "root", "secret", null);

		JAccSONTable table = null;
		try {
			table = conn.getTable("tweets");
		} 
		catch (TableNotFoundException e) {
			conn.createTable("tweets");
			table = conn.getTable("tweets");
		}
		
		
		ConfigurationBuilder cb = new ConfigurationBuilder();
		
		cb.setDebugEnabled(true)
		  .setUser("aaroncordova")
		  .setPassword("KungF00");
		
		
		TwitterStream twitterStream = new TwitterStreamFactory(cb.build()).getInstance();

		twitterStream.addListener(new TWriter(table));
		twitterStream.sample();
		
		//for(JSONObject o : table.find("{word: 'love'}", null)) {
		//	System.out.println(o);
		//}
	}
}

