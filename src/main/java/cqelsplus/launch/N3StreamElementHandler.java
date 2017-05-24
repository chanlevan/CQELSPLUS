package cqelsplus.launch;

import com.hp.hpl.jena.graph.Triple;
import com.hp.hpl.jena.n3.turtle.TurtleEventHandler;

import cqelsplus.engine.RDFStream;

public class N3StreamElementHandler implements TurtleEventHandler {

	protected RDFStream stream;	
	protected long streamSize;
	static String expOutput;
	static long noOfQueries;
	static long windowSize;
	/**Starting count point*/
	long startCount;

	static long timeStart;
	static long timeEnd;
	static long tripleNum = 0;
	
	static long consumedMem;
	static boolean doneStream = false;
	int mb = 1024 * 1024;
	static long startTime = 0;
	static long finishTime = 0;
	
	long streamedTriple = 0;
	
	long count=0,start=System.currentTimeMillis(),throughput=10000,count2=0;
	
	public N3StreamElementHandler(long streamSize, long startCount, RDFStream stream){
		this.startCount = startCount;
		this.streamSize = streamSize;
		this.stream=stream;
	}
	
	public static void setGlobalParams(String to, long nq, long ws) {
		expOutput = to;
		noOfQueries = nq;
		windowSize = ws;
	}
	
	public void triple(int line, int col, Triple triple) {
		stream.stream(triple);
		streamedTriple++;
	}
	
	
	public void prefix(int line, int col, String prefix, String iri) {
		// TODO Auto-generated method stub

	}

	public void startFormula(int line, int col) {
		// TODO Auto-generated method stub

	}

	public void endFormula(int line, int col) {
		// TODO Auto-generated method stub

	}

	public long getStreamedTriples() {
		// TODO Auto-generated method stub
		return streamedTriple;
	}

}
