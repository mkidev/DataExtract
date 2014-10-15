package edu.mki.bachelor.dataimport;

import java.util.Date;
import java.util.TreeMap;

import org.apache.commons.configuration.ConfigurationException;
import org.apache.http.impl.client.DefaultHttpClient;
import org.freaknet.gtrends.api.GoogleAuthenticator;
import org.freaknet.gtrends.api.GoogleTrendsClient;
import org.freaknet.gtrends.api.GoogleTrendsCsvParser;
import org.freaknet.gtrends.api.GoogleTrendsRequest;
import org.freaknet.gtrends.api.exceptions.GoogleTrendsClientException;
import org.freaknet.gtrends.api.exceptions.GoogleTrendsRequestException;

public class GoogleTrendsGetter {
	private String user = "marcelkisilowski@gmail.com";
	private String pass = "Qesfyc01";
	private DefaultHttpClient httpClient = new DefaultHttpClient();
	GoogleAuthenticator authenticator;
	GoogleTrendsClient client;

	public GoogleTrendsGetter() {
		authenticator = new GoogleAuthenticator(user, pass, httpClient);
		client = new GoogleTrendsClient(authenticator, httpClient);
	}

	public TreeMap<Date, WordAtDateData> getTrend(String word) {
		TreeMap<Date, WordAtDateData> result = new TreeMap<>();
		try {
			GoogleTrendsRequest request = new GoogleTrendsRequest(word);
			String content = client.execute(request);
			GoogleTrendsCsvParser csvParser = new GoogleTrendsCsvParser(content);
			String section = csvParser.getSectionAsString("Interest over time",
					true);
			// System.out.println(csvParser.getCsv());
			// System.out.println(section);
			result = parseDataString(section);

		} catch (GoogleTrendsRequestException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (GoogleTrendsClientException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (ConfigurationException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		return result;
	}

	private TreeMap<Date, WordAtDateData> parseDataString(String section) {
		TreeMap<Date, WordAtDateData> result = new TreeMap<>();
		String[] lines = section.split("\n");
		System.out.println(lines.length);
		return result;
	}

	public static void main(String[] args) {
		GoogleTrendsGetter gtg = new GoogleTrendsGetter();
		gtg.getTrend("google");
	}
}
