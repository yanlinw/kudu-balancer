package org.apache.kudu.balancer.parsers;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;

public class TableParser {
	
	static int tableIndex = 0;
	static int tableNameIndex = 0;
	static int uuidIndex = 0;

	
	public static Map<String, String> getTableMapping(String url) throws IOException {
		Map<String, String> tableMapping = new HashMap<String,String>();
		
		Document doc = Jsoup.connect(url).get();

        Elements tables = doc.getElementsByTag("tbody");
		
		Elements rows = tables.get(tableIndex).getElementsByTag("tr");
		
		Iterator<Element> itr = rows.iterator();
		while(itr.hasNext()) {
			Element col = itr.next();
			String uuid = col.getElementsByTag("td").get(uuidIndex).text();
			String tableName = col.getElementsByTag("th").get(tableNameIndex).text();
			tableMapping.put(tableName, uuid);
		}


		return tableMapping;
		
	}

}
