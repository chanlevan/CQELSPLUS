package cqelsplus.launch;


import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Scanner;

import org.apache.log4j.Logger;

import com.hp.hpl.jena.graph.Triple;

import cqelsplus.engine.ConstructListener;
import cqelsplus.engine.CqelsplusExecContext;
import cqelsplus.engine.ExecContextFactory;
import cqelsplus.engine.Config;
import cqelsplus.execplan.data.IMapping;
import cqelsplus.execplan.oprouters.QueryRouter;

public class QueryMigration 
{
    final static Logger logger = Logger.getLogger(QueryMigration.class);
    
	public static void main(String[] args) throws IOException {
		/**read input*/
		String paramsSource = args[0];
    	try {
    		Config.NUMBER_OF_QUERIES = Integer.parseInt(args[1]);
    		Config.OUTPUT_DES = args[2];
    		Config.EXPOUT = args[3];
    		Config.RESULTLOG = args[4];
		} catch (Exception e) {
			logger.error(e);
		}
    	
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
        
        /**Transfer to system's parameters*/
        
        Config.CQELS_HOME = paramTables.get("CQELS_HOME");
        if (Config.CQELS_HOME == null) {
        	logger.error("CQELS_HOME == null");
        }
        
		Config.QUERY_NAME = paramTables.get("QUERY_NAME");
        if (Config.QUERY_NAME == null) {
        	logger.error("QUERY_NAME == null");
        }
        
		Config.QUERY_LIST_FILE_NAME = paramTables.get("QUERY_LIST_FILE_NAME");
        if (Config.QUERY_LIST_FILE_NAME == null) {
        	logger.error("QUERY_LIST_FILE_NAME == null");
        }
//        
//        
//        try {
//        	Configs.NUMBER_OF_QUERIES = Integer.parseInt(paramTables.get("NUMBER_OF_QUERIES"));
//        }
//        catch (Exception e) {
//        	logger.error("Invalid NUMBER_OF_QUERIES parameter");
//        }
//        
        try {
        	Config.STATIC_INVOLVED = Boolean.parseBoolean(paramTables.get("STATIC_INVOLVED"));
        }
        catch (Exception e) {
        	logger.error("Invalid static involving condition parameter");
        }
        
        
		Config.STATIC_SOURCE = paramTables.get("STATIC_SOURCE");
        if (Config.STATIC_SOURCE == null) {
        	logger.error("STATIC_SOURCE == null");
        }
        
		Config.STREAM_SOURCE = paramTables.get("STREAM_SOURCE");;
        if (Config.STREAM_SOURCE == null) {
        	logger.error("STREAM_SOURCE == null");
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
		
        /**Initialize necessary internal flags serving for tracing algorithm correction*/
		Config.MJOIN_NORMALIZE = false;
		Config.MEMORY_REUSE = false;
		Config.PRINT_LOG = true;
		Config.INDEX_SETUP_OPTION = 0;
		
//		reader = new BufferedReader(new FileReader(Settings.QUERY_LIST_FILE_NAME));
//		List<String> queries = new ArrayList<String>();
//		String queryPath = null;
//		int i = 0;
//        while ((queryPath = reader.readLine()) != null) {
//        	Scanner queryScanner = new Scanner(new File(queryPath));
//        	String queryContent = queryScanner.useDelimiter("\\Z").next();
//        	logger.info("query " + i + " " + queryContent);
//        	queries.add(queryContent);
//        	queryScanner.close();
//        }
//        reader.close();
		/**Read input query*/
    	Scanner queryScanner = new Scanner(new File(Config.QUERY_NAME));
    	String query = queryScanner.useDelimiter("\\Z").next();
    	queryScanner.close();
   		logger.info("query " + query);

    	/**Initialize engine*/
        final CqelsplusExecContext context = new CqelsplusExecContext(Config.CQELS_HOME, true);
        ExecContextFactory.setExecContext(context);
        
        /**Load static source if needed*/
        if(Config.STATIC_INVOLVED && !Config.STATIC_LOADED) {
			context.loadDataset("http://www.cwi.nl/SRBench/sensors", Config.STATIC_SOURCE);
            Config.STATIC_LOADED = true;
			System.out.print("Static data loaded");
        }
        
        /**Start stream data first because of the requirement of query migration experiment*/
    	StreamPlayer stream=new StreamPlayer(context, "http://www.cwi.nl/SRBench/observations", 
				 Config.STREAM_SOURCE, Config.STREAM_SIZE, Config.NUMBER_OF_QUERIES, 
				 Config.WINDOW_SIZE, Config.STARTING_COUNT, 
				 Config.OUTPUT_DES + "/" + Config.EXPOUT);
		Thread streamT=new Thread(stream);
		streamT.start();
		try {
		Thread.sleep(2000);
		} catch (Exception e) {
			
		}
		
		/**Finally, register query*/
		//final QueryRouter[] qrs = new QueryRouter[queries.size()];
		final QueryRouter[] qrs = new QueryRouter[Config.NUMBER_OF_QUERIES];
		
        try {
        		for (int i = 0; i < Config.NUMBER_OF_QUERIES; i++) {
				//for (int i = 0; i < queries.size(); i++) {
					logger.info("Registration turn: " + i);
					if (query.contains("SELECT")) {
					//if (queries.get(i).contains("SELECT")) {
						qrs[i] = context.engine().registerSelectQuery(generateQuery(query,
						//qrs[i] = context.engine().registerSelectQuery(generateQuery(queries.get(i), 
								Long.toString(Config.WINDOW_SIZE)));
						qrs[i].addListener(new SelectOutputListener(context));
					} else {
						QueryRouter qr = context.engine().registerConstructQuery(generateQuery(query,
						//QueryRouter qr = context.engine().registerConstructQuery(generateQuery(queries.get(i), 
								Long.toString(Config.WINDOW_SIZE)));
						qrs[i].addListener(new ConstructListener(qrs[i].getQuery(), context) {
							
							
							public void expire(IMapping mapping) {
								// TODO Auto-generated method stub
								
							}
							
							@Override
							public void update(List<Triple> graph) {
								// TODO Auto-generated method stub
								
							}
						});
					}
					Thread.sleep(2000);
				}
        } catch (Exception e) {
        	e.printStackTrace();
        }
        try {
        	Thread.sleep(3000);
        } catch (Exception e) {
        	e.printStackTrace();
        }
	}
	
	private static String generateQuery(String template, String windowSize) {
		return template.replaceAll("COUNTSIZE", windowSize);
	}
 }
