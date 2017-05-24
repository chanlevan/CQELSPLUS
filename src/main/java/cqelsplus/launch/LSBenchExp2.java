package cqelsplus.launch;


import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.text.DecimalFormat;
import java.text.NumberFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Scanner;

import org.apache.log4j.Logger;

import cqelsplus.engine.Config;
import cqelsplus.engine.CqelsplusExecContext;
import cqelsplus.engine.ExecContext;
import cqelsplus.engine.ExecContextFactory;
import cqelsplus.execplan.oprouters.QueryRouter;
import cqelsplus.run.masterthesis.exp.SelectOutputListener;

public class LSBenchExp2 
{
    final static Logger logger = Logger.getLogger(LSBenchExp2.class);
    static int outAmount = 0;
	
	public static void main(String[] args) throws IOException, InterruptedException {
		/**read input*/
	    List<String> streamIds = new ArrayList<String>();
		List<String> streamSourcePaths = new ArrayList<String>();
		List<String> staticGraphIds = new ArrayList<String>();
		List<String> staticSources = new ArrayList<String>();
		
		String paramsSource = args[0];
		Map<String, String> paramTables = new HashMap<String, String>();
		BufferedReader reader = new BufferedReader(new FileReader(paramsSource));
        
		String param = null;
        while ((param = reader.readLine()) != null) {
        	String[] paramElms = param.split(" ");
        	if (paramElms.length == 2) {
        		paramTables.put(paramElms[0], paramElms[1]);
        	} else if (paramElms.length > 2) {
        		paramTables.put(paramElms[0], paramElms[1]);
        		if (paramElms[0].equals("STREAM_SOURCE")) {
					for (int i = 1; i < paramElms.length; i+=2) {
						streamIds.add(paramElms[i]);
						streamSourcePaths.add(paramElms[i+1]);
					}
        		} else if (paramElms[0].equals("STATIC_SOURCE")) {
    				for (int i = 1; i < paramElms.length; i+=2) {
    					staticGraphIds.add(paramElms[i]);
    					staticSources.add(paramElms[i+1]);
    				}
        		}
        	}
        }
        reader.close();
        InputParams.CQELS_HOME = paramTables.get("CQELS_HOME");
        if (InputParams.CQELS_HOME == null) {
        	logger.error("CQELS_HOME == null");
        }
        
        InputParams.QUERY_NAME = paramTables.get("QUERY_NAME");
        if (InputParams.QUERY_NAME == null) {
        	logger.error("QUERY_NAME == null");
        }

        try {
        	InputParams.STATIC_INVOLVED = Boolean.parseBoolean(paramTables.get("STATIC_INVOLVED"));
        }
        catch (Exception e) {
        	logger.error("Invalid static involving condition parameter");
        }
        
        if (InputParams.STATIC_INVOLVED) {
        	InputParams.STATIC_SOURCE = paramTables.get("STATIC_SOURCE");
	        if (InputParams.STATIC_SOURCE == null) {
	        	logger.error("STATIC_SOURCE == null");
	        }
        }
     
        InputParams.RESULTLOG = paramTables.get("RESULTLOG");;
        if (InputParams.RESULTLOG == null) {
        	logger.error("RESULTLOG == null");
        }

        InputParams.EXPOUT = paramTables.get("EXPOUT");;
        if (InputParams.EXPOUT == null) {
        	logger.error("EXPOUT == null");
        }
        
        InputParams.OUTPUT_DES = paramTables.get("OUTPUT_DES");;
        if (InputParams.OUTPUT_DES == null) {
        	logger.error("OUTPUT_DES == null");
        }
        
        try {
        	InputParams.STREAM_SIZE = Long.parseLong(paramTables.get("STREAM_SIZE"));
        }
        catch (Exception e) {
        	logger.error("Invalid STREAM_SIZE parameter");
        }

        try {
        	InputParams.STARTING_COUNT = Integer.parseInt(paramTables.get("STARTING_COUNT"));
        }
        catch (Exception e) {
        	logger.error("Invalid STARTING_COUNT parameter");
        }
        
        try {
        	InputParams.WINDOW_SIZE = Integer.parseInt(paramTables.get("WINDOW_SIZE"));
        }
        catch (Exception e) {
        	logger.error("Invalid WINDOW_SIZE parameter");
        }
        
        try {
        	InputParams.NUMBER_OF_QUERIES = Integer.parseInt(paramTables.get("NUMBER_OF_QUERIES"));
        }
        catch (Exception e) {
        	logger.error("Invalid WINDOW_SIZE parameter");
        }
		
        /**Initialize necessary internal flags to trace the correction of results*/
		Config.MJOIN_NORMALIZE = false;
		Config.MEMORY_REUSE = false;
		Config.PRINT_LOG = false;
		Config.INDEX_SETUP_OPTION = 0;
		
    	Scanner queryScanner = new Scanner(new File(InputParams.QUERY_NAME));
    	String query = queryScanner.useDelimiter("\\Z").next();
    	queryScanner.close();

    	/**Initialize engine*/
        final ExecContext context = new CqelsplusExecContext(InputParams.CQELS_HOME, true);
        ExecContextFactory.setExecContext(context);
        
        /**Load static source if needed*/
        if(InputParams.STATIC_INVOLVED && !InputParams.STATIC_LOADED) {
        	for (int i = 0; i < staticGraphIds.size(); i++) {
				if (staticGraphIds.get(i).toUpperCase().equals("DEFAULT")) {
	        		context.loadDefaultDataset(staticSources.get(i));
	        	} else {
	        		context.loadDataset(staticGraphIds.get(i), staticSources.get(i));
	        	}
        	}
			InputParams.STATIC_LOADED = true;
			System.out.print("Static data loaded");
        }

        /**register query*/
		/**Init output log*/
       // final PrintStream out = new PrintStream(new FileOutputStream(InputParams.OUTPUT_DES + InputParams.RESULTLOG, false));
		final QueryRouter[] qrs = new QueryRouter[InputParams.NUMBER_OF_QUERIES];

        try {
				for (int i = 0; i < InputParams.NUMBER_OF_QUERIES; i++) {
					//System.out.println("Registration turn: " + i);
					if (query.toUpperCase().contains("SELECT")) {
						qrs[i] = context.engine().registerSelectQuery(generateQuery(query, 
								Long.toString(InputParams.WINDOW_SIZE)));
						qrs[i].addListener(new SelectOutputListener(context, qrs[i]));
					} 
				}
        } catch (Exception e) {
        	e.printStackTrace();
        }
        List<N3StreamPlayer> streamPlayers = new ArrayList<N3StreamPlayer>();
        List<Thread> streamThreads = new ArrayList<Thread>();
        
        long StartingTime = System.currentTimeMillis();
        for (int i = 0; i < streamIds.size(); i++) {
        	N3StreamPlayer player = null;
        	if (i == 0) {
		    	player = new N3StreamPlayer(context, streamIds.get(i), 
						 streamSourcePaths.get(i), InputParams.STREAM_SIZE, 
						 InputParams.NUMBER_OF_QUERIES, InputParams.WINDOW_SIZE, 
						 InputParams.STARTING_COUNT, InputParams.OUTPUT_DES + InputParams.EXPOUT);
        	} else {
		    	player = new FlexStreamPlayer(context, streamIds.get(i), 
						 streamSourcePaths.get(i), InputParams.STREAM_SIZE, 
						 InputParams.NUMBER_OF_QUERIES, InputParams.WINDOW_SIZE, 
						 InputParams.STARTING_COUNT, InputParams.OUTPUT_DES + InputParams.EXPOUT, 1000);        		
        	}
	    	streamPlayers.add(player);
			Thread t1 = new Thread(player);
			streamThreads.add(t1);
			t1.start();
        }
        
        while(isAlive(streamThreads)) {
        	try{
        		Thread.sleep(100);
        	} catch (Exception e) {
        		e.printStackTrace();
        	}
        }
        long finishedTime = System.currentTimeMillis();
        long totalStreamTriples = 0;
        for (N3StreamPlayer player : streamPlayers) {
        	totalStreamTriples += player.getStreamedTriples();
        }
        
		try {
    		File file =new File(InputParams.OUTPUT_DES + InputParams.EXPOUT);

    		if(!file.exists()){
    			file.createNewFile();
    		}
    		PrintWriter pw = new PrintWriter(new BufferedWriter(new FileWriter(file, false)));
    		NumberFormat formatter = new DecimalFormat("#0.00"); 
    		pw.println("NoQ" + InputParams.NUMBER_OF_QUERIES);
    		pw.println("Total input triples: "+ " " + totalStreamTriples);
    		pw.println("Execution time: " + formatter.format((finishedTime - StartingTime)/1000));
    		pw.println("Processed triple/second: " + formatter.format(totalStreamTriples * 1000 / (finishedTime - StartingTime)));
    		pw.println("Output Number: " + SelectOutputListener.count);
	        pw.close();

		} catch (Exception e) {
			e.printStackTrace();
		}
		System.exit(0);
	}
	
	private static boolean isAlive(List<Thread> streamThreads) {
		for (Thread t : streamThreads) {
			if (t.isAlive()) 
				return true;
		}
		return false;
	}
	private static String generateQuery(String template, String windowSize) {
		String q = template.replaceAll("COUNTSIZE", windowSize);
    	logger.info("query " + q);
    	return q;
	}
 }
