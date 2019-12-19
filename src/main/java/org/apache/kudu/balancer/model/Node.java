package org.apache.kudu.balancer.model;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class Node implements Comparable <Node>{
	
	public Node(String name, String uuid) {
		super();
		this.name = name;
		this.uuid = uuid;
		this.tablets = new ArrayList<Tablet>();
	}
	private String name;
	private List<Tablet> tablets;
	private List<Tablet> tabletsAsLeader;
	private List<Tablet> tabletsAsFollower;
	private String uuid;
	public String getName() {
		return name;
	}
	public void setName(String name) {
		this.name = name;
	}
	
	public void addTablets(String tabletName, Set<String> followers, String leader) {
		Tablet t = new Tablet(tabletName, followers, leader);
		tablets.add(t);
	}
	
	public void addTablets(Tablet t) {
		tablets.add(t);
	}
	
	public Integer getTabletSize() {
		return tablets.size();
	}
	
	
	public List<Tablet> removeTablets(int i) {
		List<Tablet> list=new ArrayList<Tablet>();
		for(int j=0;j<i;j++) {
			list.add(tablets.remove(0));
		}
		return list;
	}
	
	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((name == null) ? 0 : name.hashCode());
		return result;
	}
	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		Node other = (Node) obj;
		if (name == null) {
			if (other.name != null)
				return false;
		} else if (!name.equals(other.name))
			return false;
		return true;
	}
	public int compareTo(Node o) {
		return getTabletSize()-o.getTabletSize();
	}
	@Override
	public String toString() {
		return "Leader [name=" + name + ", tablets=" + tablets + "]";
	}
	public List<Tablet> getTablets() {
		return tablets;
	}
	public void setTablets(List<Tablet> tablets) {
		this.tablets = tablets;
	}
	public String getUuid() {
		return uuid;
	}
	public void setUuid(String uuid) {
		this.uuid = uuid;
	}

	

}
