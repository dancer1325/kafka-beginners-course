package kafka.tutorial2;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import com.google.common.collect.Lists;
import com.twitter.hbc.ClientBuilder;
import com.twitter.hbc.core.Client;
import com.twitter.hbc.core.Constants;
import com.twitter.hbc.core.Hosts;
import com.twitter.hbc.core.HttpHosts;
import com.twitter.hbc.core.endpoint.StatusesFilterEndpoint;
import com.twitter.hbc.core.processor.StringDelimitedProcessor;
import com.twitter.hbc.httpclient.auth.Authentication;
import com.twitter.hbc.httpclient.auth.OAuth1;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * To do the 1º step in the creation about the Twitter producer : To create just the TwitterClient
 */
public class TwitterClient {

	private static Logger logger = LoggerFactory.getLogger(TwitterClient.class.getName());

	private static Properties properties;
	private static String consumerKey;
	private static String consumerSecret;
	private static String token;
	private static String secret;

	//To describe the terms to follow in Twitter
	private static List<String> terms = Lists.newArrayList("bitcoin", "usa", "politics", "sport", "soccer");

	public TwitterClient() throws IOException {
		//How to indicate the file's path
		//1º Relative path (It doesn't work, why?)
		String rootRelativePath = "src/main/resources/";
		//2º By the thread
		String rootThreadPath = Thread.currentThread().getContextClassLoader().getResource("").getPath();

		String secretPropertiesPath = rootThreadPath + "application-secret.properties";
		FileInputStream fileInputStream = new FileInputStream(secretPropertiesPath);
		properties = new Properties();
		properties.load(fileInputStream);

		consumerKey = properties.getProperty("consumerkey");
		consumerSecret = properties.getProperty("consumerSecret");
		token = properties.getProperty("token");
		secret = properties.getProperty("secret");
	}

	public static void main(String[] args) throws IOException {
		new TwitterClient().run();
	}

	public void run() {
		logger.info("Setup");

		BlockingQueue<String> msgQueue = new LinkedBlockingQueue<String>(1000);

		Client client = createTwitterClient(msgQueue);

		client.connect();

		while (!client.isDone()) {
			String msg = null;
			try {
				//Why to do a poll instead of take?
				//Poll. To indicate how many seconds to wait for
				msg = msgQueue.poll(5, TimeUnit.SECONDS);
			}
			catch (InterruptedException e) {
				e.printStackTrace();
				client.stop();
			}
			if (msg != null) {
				logger.info(msg);
			}
		}
		logger.info("End of application");
	}

	public Client createTwitterClient(BlockingQueue<String> msgQueue) {

		/** Declare the host you want to connect to, the endpoint, and authentication (basic auth or oauth) */
		Hosts hosebirdHosts = new HttpHosts(Constants.STREAM_HOST);

		StatusesFilterEndpoint hosebirdEndpoint = new StatusesFilterEndpoint();
		hosebirdEndpoint.trackTerms(terms);

		// These secrets should be read from a config file
		Authentication hosebirdAuth = new OAuth1(consumerKey, consumerSecret, token, secret);

		ClientBuilder builder = new ClientBuilder()
				.name("Hosebird-Client-01")                              // optional: mainly for the logs
				.hosts(hosebirdHosts)
				.authentication(hosebirdAuth)
				.endpoint(hosebirdEndpoint)
				.processor(new StringDelimitedProcessor(msgQueue));
		//To process eventMessages
//        .eventMessageQueue(eventQueue);

		return builder.build();
	}
}
