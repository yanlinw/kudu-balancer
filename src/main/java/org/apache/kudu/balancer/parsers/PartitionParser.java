package org.apache.kudu.balancer.parsers;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.kudu.balancer.Arranger;
import org.apache.kudu.balancer.model.Movement;
import org.apache.kudu.balancer.model.Node;
import org.apache.kudu.balancer.model.Tablet;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

public class PartitionParser {

	public PartitionParser(String masters) {
		super();
		this.leaderHist =  new HashMap<String, Map<String,Integer>>();
		this.allHist = new HashMap<String, Map<String,Integer>>();
		movementPlan = new HashMap<String, List<String>> ();
		this.masters = masters;

	}



	static int tabletId=0;
	static int hashId=1;
	static int rangePartition=2;
	static int leadership=5;
	static int leaderIndex=0;
	static Gson gson = new GsonBuilder().disableHtmlEscaping().setPrettyPrinting().create();
	static Map<String, String> tserver;
	static Set<String> node;

	Map<String, Map<String,Integer>> leaderHist;
	Map<String, Map<String,Integer>> allHist;

	Map<String, List<String>> movementPlan;
	private String masters;

	public String printLeaderHist(String[] ranges) {
		if(ranges!=null && ranges.length>0) {
			Map<String, Map<String,Integer>> newleaderHist = new HashMap<String, Map<String,Integer>>();
			for(String range:ranges) {
				if(leaderHist.containsKey(range)) {
					newleaderHist.put(range, leaderHist.get(range));
				}
			}
			return gson.toJson(newleaderHist);
		} else
			return gson.toJson(leaderHist);
	}

	public String printAllHist(String[] ranges) {
		if(ranges!=null && ranges.length>0) {
			Map<String, Map<String,Integer>> newAllHist = new HashMap<String, Map<String,Integer>>();
			for(String range:ranges) {
				if(allHist.containsKey(range)) {
					newAllHist.put(range, allHist.get(range));
				}
			}
			return gson.toJson(newAllHist);
		} else
			return gson.toJson(allHist);
	}

	public String printMovementPlan(String[] ranges) {
		if(ranges!=null && ranges.length>0) {
			Map<String, List<String>> newMovementPlan = new HashMap<String, List<String>>();
			for(String range:ranges) {
				if(movementPlan.containsKey(range)) {
					newMovementPlan.put(range, movementPlan.get(range));
				}
			}
			return gson.toJson(newMovementPlan);
		} else
			return gson.toJson(movementPlan);
	}


	public void parse(String leaderMasteUrl, String table, String mode) throws IOException {

		Map<String, String> tableMapping = TableParser.getTableMapping(leaderMasteUrl+"tables");

		String tableUrl = leaderMasteUrl + "table?id=" + tableMapping.get(table);


		tserver = TserverParser.getTserverMapping(leaderMasteUrl+"tablet-servers");

		Map<String, Map<String, Node>> rangePartitions =new HashMap<String, Map<String, Node>>();


		Map<String, Tablet> tablets =new HashMap<String,Tablet>();


		Document doc = Jsoup.connect(tableUrl).get();

		//System.out.println(doc);
		Elements links = doc.select("div[id=detail]");
		Elements tables = links.get(0).getElementsByTag("tbody");
		Elements rows = tables.get(0).getElementsByTag("tr");
		if(rows.size()<6) {
			System.out.println("error: no range partition for this table");
			System.exit(1);
		}
		Iterator<Element> itr = rows.iterator();

		while(itr.hasNext()) {
			Element row = itr.next();

			Elements cols = row.getElementsByTag("td");

			String rp = cols.get(rangePartition).text();

			Element ls = cols.get(leadership);

			Elements nodes = ls.getElementsByTag("li");

			String leaderName = nodes.get(leaderIndex).getElementsByTag("a").text();

			String tabletName = cols.get(tabletId).text();

			Tablet tab = tablets.containsKey(tabletName)?tablets.get(tabletName):
				new Tablet(tabletName, new HashSet<String>(), leaderName);

			if(!tablets.containsKey(tabletName))
				tablets.put(tabletName, tab);

			if(leaderHist.containsKey(rp)) {
				Map<String,Integer> oneHist = leaderHist.get(rp);
				if(oneHist.containsKey(leaderName)) {
					oneHist.put(leaderName, oneHist.get(leaderName)+1);
				} else {
					oneHist.put(leaderName, 1);
				}

			} else {
				Map<String,Integer> oneHist = new HashMap<String,Integer>();
				oneHist.put(leaderName, 1);
				leaderHist.put(rp, oneHist);
			}


			for(int i=leaderIndex; i<nodes.size();i++) {

				String nodeName = nodes.get(i).getElementsByTag("a").text();
				if(allHist.containsKey(rp)) {
					Map<String,Integer> oneHist = allHist.get(rp);
					if(oneHist.containsKey(nodeName)) {
						oneHist.put(nodeName, oneHist.get(nodeName)+1);
					} else {
						oneHist.put(nodeName, 1);
					}

				} else {
					Map<String,Integer> oneHist = new HashMap<String,Integer>();
					oneHist.put(nodeName, 1);
					allHist.put(rp, oneHist);
				}

				Node node = new Node(nodeName, tserver.get(nodeName));

				//add follower
				if(i>leaderIndex) {
					tab.addFollwer(nodeName);		    		
				}

				node.addTablets(tab);

				if(rangePartitions.containsKey(rp)) {
					Map<String, Node> nodelist = rangePartitions.get(rp);
					if(nodelist.containsKey(nodeName)) {
						nodelist.get(nodeName).addTablets(tab);
					} else
						nodelist.put(nodeName, node);


				} else {
					Map<String, Node> nodelist = new HashMap<String, Node>();
					nodelist.put(nodeName,node);
					rangePartitions.put(rp, nodelist);

				}

			}






		}
		//System.out.println(gson.toJson(leaderHist));

		//System.out.println(gson.toJson(rangePartitions));
		if(mode.equals("move")) {

			for(Entry<String, Map<String, Node>> m: rangePartitions.entrySet()) {

				List<Node> nodes = new ArrayList<Node>(m.getValue().values());
				//List<Movement> moves = Arranger.arrange(nodes);
				List<Movement> moves = Arranger.arrange(nodes, tserver);
				List<String> moveCmds = new ArrayList<String>();
				for(Movement move:moves) {
					moveCmds.add(move.getCmd(masters));
				}
				movementPlan.put(m.getKey(), moveCmds);

			}
		}

	}

}
