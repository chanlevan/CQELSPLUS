package cqelsplus.launch;


import cqelsplus.engine.ExecContext;


public class FlexStreamPlayer extends N3StreamPlayer {
	public FlexStreamPlayer (ExecContext context,String iri, String streamSourcePath, long streamSize, 
			long noOfQueries, long windowSize, long startCountPoint, String expOutput, long throughput){
		super(context, iri, streamSourcePath, streamSize, noOfQueries, windowSize, startCountPoint, expOutput);
		streamHandler = new FlexN3StreamElementHandler(throughput, streamSize, startCountPoint, this);
	}
}
