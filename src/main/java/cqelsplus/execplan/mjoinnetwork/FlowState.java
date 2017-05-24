package cqelsplus.execplan.mjoinnetwork;

public enum FlowState {
	/**
	 * There are at least 1 data flow treated as main flow. 
	 * */
	MAIN,
	/**The minor flow can be removed when the main flow is full-filled */
	MINOR, 
	DEPRICATED;
}
