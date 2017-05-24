package cqelsplus.execplan.windows;


import cqelsplus.execplan.data.RoutingMessage;


public abstract class AWBBase implements AWB, RoutingMessage {
	protected PhysicalWindow from;
	public void setFrom(PhysicalWindow from) { 
		this.from=from;
	}
	
	public PhysicalWindow getFrom() {
		return from;
	}
	
	
}
