package cqelsplus.launch;


import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
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

public class CountData 
{
    final static Logger logger = Logger.getLogger(CountData.class);
    
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
        
        /**Transfer to system's parameters*/
    	try {
    		Config.WINDOW_SIZE = Integer.parseInt(args[1]);
    		Config.OUTPUT_DES = args[2];
    		Config.EXPOUT = args[3];
    		Config.RESULTLOG = args[4];
    		
		} catch (Exception e) {
			logger.error(e);
		}
    	
    	String patternsSource = args[5];
		    	
        Config.CQELS_HOME = paramTables.get("CQELS_HOME");
        if (Config.CQELS_HOME == null) {
        	logger.error("CQELS_HOME == null");
        }
        
		Config.QUERY_NAME = paramTables.get("QUERY_NAME");
        if (Config.QUERY_NAME == null) {
        	logger.error("QUERY_NAME == null");
        }

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
        
//        try {
//        	Configs.WINDOW_SIZE = Integer.parseInt(paramTables.get("WINDOW_SIZE"));
//        }
//        catch (Exception e) {
//        	logger.error("Invalid WINDOW_SIZE parameter");
//        }
//		
        /**Initialize necessary internal flags serving for tracing algorithm correction*/
		Config.MJOIN_NORMALIZE = false;
		Config.MEMORY_REUSE = false;
		Config.PRINT_LOG = true;
		Config.INDEX_SETUP_OPTION = 0;

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

        /**register query*/
		/**Init output log*/
        final BufferedWriter out = new BufferedWriter(new FileWriter(Config.OUTPUT_DES + "/" + Config.RESULTLOG, false));
		
        /**Read input patterns*/
		reader = new BufferedReader(new FileReader(patternsSource));
        String pattern = null;
        List<String> patterns = new ArrayList<String>();
        while ((pattern = reader.readLine()) != null) {
        	patterns.add(pattern);
        }
        reader.close();
        
		final QueryRouter[] qrs = new QueryRouter[patterns.size()];
		final int[] count = new int[patterns.size()];
		for (int i = 0; i < count.length; i++) count[i] = 0;
        try {
				for (int i = 0; i < patterns.size(); i++) {
					final int t = i;
					final String p = patterns.get(t);
					//System.out.println("Registration turn: " + i);
					if (query.contains("SELECT")) {
						qrs[i] = context.engine().registerSelectQuery(generateQuery(query, patterns.get(i),
								Long.toString(Config.WINDOW_SIZE)));
						final int qId = qrs[i].getId(); 
						qrs[i].addListener(new SelectListener() {
							
							@Override
							public void update(IMapping mapping) {
								//out.print("+ q id: " + qId + ": " );
								try {
								String result = "";
								for (Var var : mapping.getVars()) {
										long value = mapping.getValue(var);
										if (value > 0) {
											result += context.engine().decode(value) + " ";
											//context.engine().decode(value);
										} else {
											result += Long.toString(-value) + " ";
										}
									}
								
									//out.write(result + "\n");
									//out.flush();
								}
								catch (Exception e) {
									e.printStackTrace();
								}

								if (count[t] > Config.WINDOW_SIZE) {
									return;
								}
								count[t]++;
								if (count[t] == Config.WINDOW_SIZE) {
									CountStreamElementHandler.saveOutputInfo(Config.OUTPUT_DES + 
											Config.WINDOW_SIZE + "_" + t);
									checkStop();
								}
							}

							private void checkStop() {
								boolean stopped = true;
								for (int k = 0; k < count.length; k++) {
									if (count[k] < Config.WINDOW_SIZE) {
										stopped = false;
										break;
									}
								}
								if (stopped) {
									System.exit(1);
								}
							}

							public void expire(IMapping mapping) {
								try {
									String result = "";
									for (Var var : mapping.getVars()) {
											long value = mapping.getValue(var);
											if (value > 0) {
												result += context.engine().decode(value) + " ";
												//context.engine().decode(value);
											} else {
												result += Long.toString(-value) + " ";
											}
										}
									
										//out.write(result + "\n");
										//out.flush();
								} catch (Exception e) {
									e.printStackTrace();
								}
							}
						});
					} else {
						QueryRouter qr = context.engine().registerConstructQuery(generateQuery(query, patterns.get(i),
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
        } catch (Exception e) {
        	e.printStackTrace();
        }


    	CountStreamPlayer stream=new CountStreamPlayer(context, "http://www.cwi.nl/SRBench/observations", 
											 Config.STREAM_SOURCE, Config.STREAM_SIZE, 
											 Config.WINDOW_SIZE, Config.STARTING_COUNT, 
											 Config.OUTPUT_DES + "/" + Config.EXPOUT);
		Thread streamT=new Thread(stream);
		streamT.start();

		while (!stream.isStopped()) {
			Thread.sleep(1000);
		}
		out.flush();
		out.close();
		
	}
	
	private static String generateQuery(String template, String pattern, String windowSize) {
		String query = template.replaceAll("COUNTSIZE", windowSize);
		query = query.replaceAll("TOKENPARAMISPUTHERE", pattern);
		logger.info(query);
		return query;
	}
 }
