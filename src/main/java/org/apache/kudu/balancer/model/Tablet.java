package org.apache.kudu.balancer.model;

import java.util.HashSet;
import java.util.Set;

public class Tablet {
	
	public Tablet(String name, Set<String> follwers, String leader) {
		super();
		this.name = name;
		this.follwers = follwers;
		this.leader = leader;
		
	}

	private String name;
	private Set<String> follwers;
	private String leader;


	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public Set<String> getFollwers() {
		return follwers;
	}
	
	public void addFollwer(String follower) {
		follwers.add(follower);
	}

	public void setFollwers(Set<String> follwers) {
		this.follwers = follwers;
	}

	public String getLeader() {
		return leader;
	}

	public void setLeader(String leader) {
		this.leader = leader;
	}

	@Override
	public String toString() {
		return "Tablet [name=" + name + ", follwers=" + follwers + ", leader=" + leader + "]";
	}


	
	
}
