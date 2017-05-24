package cqelsplus.execplan.mjoinnetwork;

public class ProbingInfo {
	int pId;//physical window Id;
	int col;//which column of this window will be probed
	public ProbingInfo(int pId, int col) {
		this.pId = pId;
		this.col = col;
	}
	
	public ProbingInfo() {}
	
	public void set(int pId, int col) {
		this.pId = pId;
		this.col = col;
	}
	
	public int getProbedPid() {
		return pId;
	}
	
	public int getProbedCol() {
		return col;
	}
	
}
