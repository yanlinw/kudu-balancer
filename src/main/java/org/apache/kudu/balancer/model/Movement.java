package org.apache.kudu.balancer.model;

public class Movement {
	
	public Movement(Node from, Node to, Tablet tablet) {
		super();
		this.from = from;
		this.to = to;
		this.tablet = tablet;
	}

	private Node from;
	
	private Node to;
	
	private Tablet tablet;

	public Node getFrom() {
		return from;
	}

	public void setFrom(Node from) {
		this.from = from;
	}

	public Node getTo() {
		return to;
	}

	public void setTo(Node to) {
		this.to = to;
	}

	public Tablet getTablet() {
		return tablet;
	}

	public void setTablet(Tablet tablet) {
		this.tablet = tablet;
	}
	
	public String getCmd(String masterAddress) {
		String res = "sudo -u kudu kudu tablet change_config move_replica";
		res+=" " + masterAddress + " " + tablet.getName() + " " + from.getUuid() + " " + to.getUuid() ;
		return res;
	}

	@Override
	public String toString() {
		return "Movement [from=" + from.getName() + ", to=" + to.getName() + ", tablet=" + tablet.getName() + "]";
	}
	

}
