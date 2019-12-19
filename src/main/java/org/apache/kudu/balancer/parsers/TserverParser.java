package org.apache.kudu.balancer.parsers;

import java.io.IOException;
import java.net.URL;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;

public class TserverParser {
	static int mappingIndex=1;
	static int uuidIndex=0;
	static int hostIndex=4;
	
	public static Map<String, String> getTserverMapping(String url) throws IOException {
		Map<String, String>result =new HashMap<String,String>();
		
		Document doc = Jsoup.connect(url).get();
		
		Elements tables = doc.getElementsByTag("tbody");
		
		Elements rows = tables.get(mappingIndex).getElementsByTag("tr");
		
		Iterator<Element> itr = rows.iterator();
		while(itr.hasNext()) {
			Element col = itr.next();
			Element content = col.getElementsByTag("td").get(uuidIndex);
			String uuid = content.text();
			String host = content.getElementsByTag("a").attr("href");
			URL hostUrl= new URL(host);
			String hostName = hostUrl.getHost()+":"+hostUrl.getPort();

			
			result.put(hostName, uuid);
		}
		
		return result;
	}
	
	
}
