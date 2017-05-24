package cqelsplus.launch;


import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.PrintWriter;
import java.text.DecimalFormat;
import java.text.NumberFormat;
import java.util.ArrayList;

import org.apache.log4j.Logger;

import cqelsplus.engine.CqelsplusExecContext;
import cqelsplus.engine.RDFStream;

public class CsvTextStreamer extends RDFStream implements Runnable{
	boolean stop=false;
	long sleep=0;
	static long startTime = 0;
	static long finishTime = 0;
	static long noOfQueries;
	static long windowSize;
	
	final static Logger logger = Logger.getLogger(StreamPlayer.class);
	
	String filestream;
	ArrayList<String> fileNames;
	long streamSize;
	/**sCP = starting count point*/
	long startCountPoint;
	
	String expOutput;
	boolean stopped;
	
	
	public CsvTextStreamer(CqelsplusExecContext context,String iri, String filestream, long streamSize, 
			long nOq, long ws, long startCountPoint, String expOutput) {
		super(context, iri);
		this.filestream = filestream;
		this.streamSize = streamSize;
		this.startCountPoint = startCountPoint;
		noOfQueries  = nOq;
		windowSize = ws;
		stopped = false;
		this.expOutput = expOutput;
	}
	
	@Override
	public void stop() {
		stop=true;
	}
	
	public void run() {
			try {
				boolean broken = false;
				BufferedReader reader = new BufferedReader(new FileReader(filestream));
				String strLine;
				//System.out.println("started");
				int t = 0;
				for (int i = 0; i < 13; i++) {
						while ((strLine = reader.readLine()) != null &&(!stop))   {
							//System.out.println(strLine);
						    String[] data=strLine.split(";");
						    if (data.length != 3) continue;
							stream(n(data[0]),n(data[1]),n(data[2]));
							t++;
							if (t == this.streamSize) {
								broken = true;
								break;
							}
							if (t == startCountPoint) {
								startTime = System.nanoTime();
							}
		//					if (t % 10000 == 0) {
		//						System.out.println("At streamed triples: " + t);
		//					}
						}
						if (broken) break;
					}
					reader.close();
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			try {
	    		File file =new File(expOutput);
	   		 
	    		//if file doesnt exists, then create it
	    		if(!file.exists()){
	    			file.createNewFile();
	    		}
	    		PrintWriter pw = new PrintWriter(new BufferedWriter(new FileWriter(file, false)));
	    		finishTime = System.nanoTime();
	    		NumberFormat formatter = new DecimalFormat("#0.00"); 
	    		pw.println(noOfQueries + " " + windowSize + " " + formatter.format((streamSize - startCountPoint) * 1E9 / ((finishTime - startTime) * 1.0)));
		        pw.close();
		        
		        System.out.println("Done stream");
			} catch (Exception e) {
				e.printStackTrace();
			}
	}
}
