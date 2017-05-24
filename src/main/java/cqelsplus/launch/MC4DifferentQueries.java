package cqelsplus.launch;


import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintStream;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Scanner;

import org.apache.log4j.Logger;

import com.hp.hpl.jena.graph.Triple;
import com.hp.hpl.jena.sparql.core.Var;

import cqelsplus.engine.ConstructListener;
import cqelsplus.engine.CqelsplusExecContext;
import cqelsplus.engine.ExecContextFactory;
import cqelsplus.engine.Config;
import cqelsplus.engine.SelectListener;
import cqelsplus.execplan.data.IMapping;
import cqelsplus.execplan.oprouters.QueryRouter;

public class MC4DifferentQueries 
{
    final static Logger logger = Logger.getLogger(MC4DifferentQueries.class);
    static int outAmount = 0;
    
	public static void main(String[] args) throws IOException, InterruptedException {
		/**read input*/
		String paramsSource = args[0];
		Map<String, String> paramTables = new HashMap<String, String>();
		BufferedReader reader = new BufferedReader(new FileReader(paramsSource));
        String param = null;
        while ((param = reader.readLine()) != null) {
        	String[] paramPair = param.split(" ");
        	if (paramPair.length >= 2) {
        		paramTables.put(paramPair[0], paramPair[1]);
        	}
        }
        reader.close();
        Config.OUTPUT_DES = args[1];
        Config.CQELS_HOME = paramTables.get("CQELS_HOME");
        if (Config.CQELS_HOME == null) {
        	logger.error("CQELS_HOME == null");
        }
        
        Config.QUERY_LIST_FILE_NAME = paramTables.get("QUERY_LIST_FILE_NAME");
        if (Config.QUERY_LIST_FILE_NAME == null) {
        	logger.error("QUERY_NAME == null");
        }

        try {
        	Config.STATIC_INVOLVED = Boolean.parseBoolean(paramTables.get("STATIC_INVOLVED"));
        }
        catch (Exception e) {
        	logger.error("Invalid static involving condition parameter");
        }
        
        if (Config.STATIC_INVOLVED) {
		Config.STATIC_SOURCE = paramTables.get("STATIC_SOURCE");
        if (Config.STATIC_SOURCE == null) {
        	logger.error("STATIC_SOURCE == null");
        }
        }
        
		Config.STREAM_SOURCE = paramTables.get("STREAM_SOURCE");;
        if (Config.STREAM_SOURCE == null) {
        	logger.error("STREAM_SOURCE == null");
        }
		
//        Params.OUTPUT_DES = paramTables.get("OUTPUT_DES");;
//        if (Params.OUTPUT_DES == null) {
//        	logger.error("OUTPUT_DES == null");
//        }
//        
        Config.RESULTLOG = paramTables.get("RESULTLOG");;
        if (Config.RESULTLOG == null) {
        	logger.error("RESULTLOG == null");
        }

        Config.EXPOUT = paramTables.get("EXPOUT");;
        if (Config.EXPOUT == null) {
        	logger.error("EXPOUT == null");
        }

        
        try {
        	Config.STREAM_SIZE = Long.parseLong(paramTables.get("STREAM_SIZE"));
        }
        catch (Exception e) {
        	logger.error("Invalid STREAM_SIZE parameter");
        }

        try {
        	Config.STARTING_COUNT = Integer.parseInt(paramTables.get("STARTING_COUNT"));
        }
        catch (Exception e) {
        	logger.error("Invalid STARTING_COUNT parameter");
        }
        
        try {
        	Config.WINDOW_SIZE = Integer.parseInt(paramTables.get("WINDOW_SIZE"));
        }
        catch (Exception e) {
        	logger.error("Invalid WINDOW_SIZE parameter");
        }
        
        try {
        	Config.NUMBER_OF_QUERIES = Integer.parseInt(paramTables.get("NUMBER_OF_QUERIES"));
        }
        catch (Exception e) {
        	logger.error("Invalid NUMBER_OF_QUERIES parameter");
        }
		
        /**Initialize necessary internal flags serving for tracing algorithm correction*/
		Config.MJOIN_NORMALIZE = false;
		Config.MEMORY_REUSE = false;
		Config.PRINT_LOG = false;
		Config.INDEX_SETUP_OPTION = 0;
		
		/**Read input queries*/
		reader = new BufferedReader(new FileReader(Config.QUERY_LIST_FILE_NAME));
		List<String> queries = new ArrayList<String>();
		String queryPath = null;
		int i = 0;
        while ((queryPath = reader.readLine()) != null) {
        	Scanner queryScanner = new Scanner(new File(queryPath));
        	String queryContent = queryScanner.useDelimiter("\\Z").next();
        	//logger.info("query " + i + " " + queryContent);
        	queries.add(queryContent);
        	queryScanner.close();
        }
        reader.close();
		/**Read input query*/
/**    	Scanner queryScanner = new Scanner(new File(Params.QUERY_NAME));
    	String query = queryScanner.useDelimiter("\\Z").next();
    	queryScanner.close();*/

    	/**Initialize engine*/
        final CqelsplusExecContext context = new CqelsplusExecContext(Config.CQELS_HOME, true);
        ExecContextFactory.setExecContext(context);
        
        /**Load static source if needed*/
        if(Config.STATIC_INVOLVED && !Config.STATIC_LOADED) {
			context.loadDataset("http://www.cwi.nl/SRBench/sensors", Config.STATIC_SOURCE);
            Config.STATIC_LOADED = true;
			System.out.print("Static data loaded");
        }

        /**register query*/
		/**Init output log*/
        final PrintStream out = new PrintStream(new FileOutputStream(Config.OUTPUT_DES + Config.RESULTLOG, false));
		final QueryRouter[] qrs = new QueryRouter[Config.NUMBER_OF_QUERIES * queries.size()];
		int k = -1;
        try {
        	for (int j = 0; j < queries.size(); j++) { 
				for (i = 0; i < Config.NUMBER_OF_QUERIES; i++) {
					k++;
					//System.out.println("Registration turn: " + i);
					if (queries.get(j).contains("SELECT")) {
						qrs[k] = context.engine().registerSelectQuery(generateQuery(queries.get(j), 
								Long.toString(Config.WINDOW_SIZE)));
						final int qId = qrs[i].getId(); 
						qrs[k].addListener(new SelectListener() {
							
							@Override
							public void update(IMapping mapping) {
								String result = "";
								//out.print("+ q id: " + qId + ": " );
								for (Var var : mapping.getVars()) {
									long value = mapping.getValue(var);
									if (value > 0) {
										result += context.engine().decode(value) + " ";
										//out.print(context.engine().decode(value) + " ");
									} else {
										result += Long.toString(-value) + " ";
										//out.print(-value + " ");
									}
								}
								//System.out.println(result);
								//out.println(result);
								//out.flush();
								outAmount++;
							}
							
							public void expire(IMapping mapping) {
								//out.print("- q id: " + qId + ": " );
								String result = "";
								for (Var var : mapping.getVars()) {
									long value = mapping.getValue(var);
									if (value > 0) {
										//out.print(context.engine().decode(value) + " ");
										result += context.engine().decode(value) + " ";
									} else {
										result += Long.toString(-value) + " ";
										//out.print(-value + " ");
									}
								}
								//out.println(result);
								//out.flush();
							}
						});
					} else {
						QueryRouter qr = context.engine().registerConstructQuery(generateQuery(queries.get(j), 
								Long.toString(Config.WINDOW_SIZE)));
						qr.addListener(new ConstructListener(qr.getQuery(), context) {
							
							public void expire(IMapping mapping) {
								// TODO Auto-generated method stub
								
							}
							
							@Override
							public void update(List<Triple> graph) {
								// TODO Auto-generated method stub
								
							}
						});
					}
					//Thread.sleep(2000);
				}
        	}
        } catch (Exception e) {
        	e.printStackTrace();
        }
        
    	StreamPlayer stream=new StreamPlayer(context, "http://www.cwi.nl/SRBench/observations", 
				 Config.STREAM_SOURCE, Config.STREAM_SIZE, 
				 Config.NUMBER_OF_QUERIES * queries.size(), Config.WINDOW_SIZE, 
				 Config.STARTING_COUNT, Config.OUTPUT_DES + Config.EXPOUT);
		Thread streamT=new Thread(stream);
		streamT.start();
		while (!stream.isStopped()) {
			Thread.sleep(1000);
		}
		try {
    		File file =new File(Config.OUTPUT_DES + Config.EXPOUT);
   		 	PrintWriter pw = new PrintWriter(new BufferedWriter(new FileWriter(file, true)));
    		int arrLength = 100;
    		long memories[] = new long[arrLength];
    		for (i = 0; i < arrLength; i++) {
    			int mb = 1024 * 1024;
	    		Runtime runtime = Runtime.getRuntime();
	    		memories[i] = (runtime.totalMemory() - runtime.freeMemory()) / mb;
	    		try {
	    			Thread.sleep(100);
	    		} catch(Exception e) {
	    			e.printStackTrace();
	    		}
    		}
    		long avgMem = 0;
    		for (i = 0; i < arrLength; i++) {
    			avgMem += memories[i];
    		}
    		avgMem = avgMem/arrLength;
    		
	        pw.print(Config.NUMBER_OF_QUERIES * queries.size() + " " + Config.WINDOW_SIZE + " " + avgMem);
	        pw.close();
		} catch (Exception e) {
			e.printStackTrace();
		}
		
		out.flush();
		out.print("Outputs amount: " + outAmount);
		out.close();
		System.exit(0);
	}
	
	private static String generateQuery(String template, String windowSize) {
		String q = template.replaceAll("COUNTSIZE", windowSize);
    	logger.info("query " + q);
    	return q;
	}
 }
