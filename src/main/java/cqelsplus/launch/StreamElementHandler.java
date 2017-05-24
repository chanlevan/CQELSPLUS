package cqelsplus.launch;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.PrintWriter;
import java.text.DecimalFormat;
import java.text.NumberFormat;

import com.hp.hpl.jena.graph.Triple;
import com.hp.hpl.jena.n3.turtle.TurtleEventHandler;

import cqelsplus.engine.RDFStream;

public class StreamElementHandler implements TurtleEventHandler {

	RDFStream stream;	
	long streamSize;
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
	public StreamElementHandler(long streamSize, long startCount, RDFStream stream){
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
		if (!doneStream  && tripleNum >= streamSize ) {
			try {
	    		File file =new File(expOutput);
	   		 
	    		//if file doesnt exists, then create it
	    		if(!file.exists()){
	    			file.createNewFile();
	    		}
	    		PrintWriter pw = new PrintWriter(new BufferedWriter(new FileWriter(file, false)));
	    		finishTime = System.nanoTime();
	    		NumberFormat formatter = new DecimalFormat("#0.00"); 
	    		pw.println(noOfQueries + " " + windowSize + " " + formatter.format((streamSize - startCount) * 1E9 / ((finishTime - startTime) * 1.0)));
		        pw.close();
		        
		        System.out.println("Done stream");
		        doneStream = true;
		        stream.stop();
			} catch (Exception e) {
				e.printStackTrace();
			}
		}

		tripleNum++;
		if (tripleNum == startCount) {
			startTime = System.nanoTime();
		}
//		if (tripleNum % 10000 == 0) {
//			System.out.println("streamed: " + tripleNum + "triples");
//			System.out.flush();
//		}
		
		stream.stream(triple);
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

}
