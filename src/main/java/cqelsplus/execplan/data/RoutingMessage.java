package cqelsplus.execplan.data;

import cqelsplus.execplan.windows.PhysicalWindow;


public interface RoutingMessage {

	public PhysicalWindow getFrom();
	public void setFrom(PhysicalWindow from);
}
