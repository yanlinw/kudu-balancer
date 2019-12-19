package org.apache.kudu.balancer.parsers;

import java.io.IOException;
import java.net.URL;
import java.util.Iterator;

import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;

public class MasterParser {

	static int masterIndex = 0;
	static int leadershipIndex = 1;
	static int hostIndex = 0;
	static String LEADER = "LEADER";
	public static String getleader(String url) throws IOException {

		Document doc = Jsoup.connect(url).get();
         Elements tables = doc.getElementsByTag("tbody");
		
		Elements rows = tables.get(masterIndex).getElementsByTag("tr");
		Iterator<Element> itr = rows.iterator();
		while(itr.hasNext()) {
			Element col = itr.next();
			String content = col.getElementsByTag("td").get(leadershipIndex).text();
			if(content.equals(LEADER)) {
				Element hostElement = col.getElementsByTag("td").get(hostIndex);
				String host = hostElement.getElementsByTag("a").attr("href");
				
				return host;
			}
			
			
		}


		return "";

	}


}
