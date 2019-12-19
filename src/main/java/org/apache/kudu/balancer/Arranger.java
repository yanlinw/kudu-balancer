package org.apache.kudu.balancer;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;

import org.apache.kudu.balancer.model.Movement;
import org.apache.kudu.balancer.model.Node;
import org.apache.kudu.balancer.model.Tablet;


public class Arranger {

	
	
	public static int getAverage( List<Node> nodes) {
		int count = 0;
		for(Node node:nodes) {
			count+=node.getTabletSize();
		}
		return (int)Math.ceil((double)count/(double)nodes.size());
	}
	
	
	public static List<Movement> arrange (List<Node> nodes,  Map<String, String> tserver) {
		
		Set<String> allNodes = tserver.keySet();
		Set<String> exist = new HashSet<String>();
		for(Node node:nodes) {
			exist.add(node.getName());
		}
		for(String allNode:allNodes) {
			if(!exist.contains(allNode)) {
				Node newNode = new Node(allNode, tserver.get(allNode));
				nodes.add(newNode);
			}
		}
			
		return arrange(nodes);
	}

	
	
	public static List<Movement> arrange (List<Node> nodes) {
        
		int average = getAverage(nodes);
		//System.out.println("Node numbers: " + nodes.size());
		//System.out.println("Average is: " + average);
		List<Tablet> moveOut= new ArrayList<Tablet>();
		List<Node> moveOutNodes= new ArrayList<Node>();
		List<Movement> result = new ArrayList<Movement>();
		
		
		for(Node node : nodes) {
			if(node.getTabletSize()>average) {
				List<Tablet> list = node.removeTablets(node.getTabletSize()- average);
				moveOut.addAll(list);
				for(int i=0;i<list.size();i++) {
					moveOutNodes.add(node);
				}
			}


		}
		Random rand = new Random();
		for(Node node : nodes) {

			if(node.getTabletSize()<average) {
				for(int i=0;i<(average - node.getTabletSize()); i++){

					for(int retry=0;retry<100; retry++) {
						int j = 0;
						if(moveOut.size()>1)
						 j = rand.nextInt(moveOut.size()-1);
						Tablet tab = moveOut.get(j);
						if(isValidMove(node, tab)) {
							Movement move = new Movement(moveOutNodes.get(j), node, tab);
							result.add(move);
							moveOut.remove(j);
							moveOutNodes.remove(j);
							break;
						} else
							continue;
					}
					
					if(moveOut.isEmpty()) {
						//System.out.println("running out of tablets, exiting");
						return result;
					}

				}

			}

		}
		
		return result;
	}


	public static boolean isValidMove(Node to, Tablet tablet) {
		if(tablet.getLeader().equals(to.getName()))
			return false;
		if(tablet.getFollwers().contains(to.getName()))
			return false;
		return true;

	}

}
