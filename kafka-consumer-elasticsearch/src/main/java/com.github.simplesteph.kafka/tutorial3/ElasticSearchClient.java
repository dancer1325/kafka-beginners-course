package com.github.simplesteph.kafka.tutorial3;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.Properties;

import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.nio.client.HttpAsyncClientBuilder;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ElasticSearchClient {

	private static Properties properties;
	private static String hostName;
	private static String userName;
	private static String password;

	public static RestHighLevelClient createClient() throws IOException {
		//1º) Using local ES
		//  String hostname = "localhost";
		//  RestClientBuilder builder = RestClient.builder(new HttpHost(hostname,9200,"http"));

		//2º) Using BONSAI / HOSTED ES
		//Replace with your own credentials
		String rootThreadPath = Thread.currentThread().getContextClassLoader().getResource("").getPath();
		String secretPropertiesPath = rootThreadPath + "application-secret.properties";
		FileInputStream fileInputStream = new FileInputStream(secretPropertiesPath);
		properties = new Properties();
		properties.load(fileInputStream);
		//Bonsai's url:=https://userName:password@hostname
		hostName = properties.getProperty("hostname"); // localhost or bonsai url
		userName = properties.getProperty("userName"); // URL's 1º part
		password = properties.getProperty("password"); // needed only for bonsai

		// credentials provider help supply username and password
		final CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
		credentialsProvider.setCredentials(AuthScope.ANY,
				new UsernamePasswordCredentials(userName, password));

		RestClientBuilder builder = RestClient.builder(
				new HttpHost(hostName, 443, "https")) //403. Port of https
				.setHttpClientConfigCallback(new RestClientBuilder.HttpClientConfigCallback() {
					@Override
					public HttpAsyncClientBuilder customizeHttpClient(HttpAsyncClientBuilder httpClientBuilder) {
						return httpClientBuilder.setDefaultCredentialsProvider(credentialsProvider);
					}
				});

		RestHighLevelClient client = new RestHighLevelClient(builder);
		return client;
	}

	public static void main(String[] args) throws IOException {
		Logger logger = LoggerFactory.getLogger(ElasticSearchConsumer.class.getName());
		RestHighLevelClient client = createClient();

		String jsonString = "{ \"foo\":\"bar\"}";
		IndexRequest indexRequest = new IndexRequest(
				"twitter", //Index
				"tweets" //Type
		).source(jsonString, XContentType.JSON);
		IndexResponse indexResponse = client.index(indexRequest, RequestOptions.DEFAULT);
		logger.info(indexResponse.getId());

		//To close the client gracefully
		client.close();
	}
}
