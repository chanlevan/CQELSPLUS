package cqelsplus.launch;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.PrintWriter;

import org.apache.log4j.Logger;

import com.hp.hpl.jena.graph.Triple;
import com.hp.hpl.jena.n3.turtle.TurtleEventHandler;

import cqelsplus.engine.RDFStream;

public class CountStreamElementHandler implements TurtleEventHandler {

	RDFStream stream;	
	long dataSize = 10000000;
	static String expOutput;
	static long windowSize;
	/**Starting count point*/
	long sCP;

	static long timeStart;
	static long timeEnd;
	static long tripleNum = 0;
	
	static long consumedMem;
	static boolean doneStream = false;
	int mb = 1024 * 1024;
	
	public CountStreamElementHandler(long dataSize, long sCP, RDFStream stream){
		this.sCP = sCP;
		this.dataSize = dataSize;
		this.stream=stream;
	}
	
	public static void setGlobalParams(String to, long ws) {
		expOutput = to;
		windowSize = ws;
	}
	
	public void triple(int line, int col, Triple triple) {
		if (!doneStream  && tripleNum >= dataSize ) {
			try {
	    		File file =new File(expOutput);
	   		 
	    		//if file doesnt exists, then create it
	    		if(!file.exists()){
	    			file.createNewFile();
	    		}
	    		
	    		Runtime runtime = Runtime.getRuntime();
	    		
	    		PrintWriter pw = new PrintWriter(new BufferedWriter(new FileWriter(file, false)));
		        pw.print((runtime.totalMemory() - runtime.freeMemory()) / mb);
		        pw.close();
		        
		        System.out.println("Done stream");
		        doneStream = true;
		        stream.stop();

			} catch (Exception e) {
				e.printStackTrace();
			}
		}

		tripleNum++;
		if (tripleNum % 1000000 == 0) {
			Logger.getLogger(CountStreamElementHandler.class).info("Streamed " + tripleNum + " triples");
		}
		stream.stream(triple);
	}
	
	public static void saveOutputInfo(String output) {
		try {
			BufferedWriter bw = new BufferedWriter(new FileWriter(output));
			bw.write(Long.toString(tripleNum));
			bw.flush();
			bw.close();
		
		} catch (Exception e) {
			e.printStackTrace();
		}
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
