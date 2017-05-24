package cqelsplus.launch;


import java.util.List;

import com.hp.hpl.jena.util.FileManager;

import cqelsplus.engine.CqelsplusExecContext;
import cqelsplus.engine.ExecContextFactory;
import cqelsplus.engine.Config;
import cqelsplus.execplan.oprouters.QueryRouter;

public class GraphBuilding {

	static final String logTag="_newversion.log";
	static final String queryTag=".cqels";
	static CqelsplusExecContext context;
	static long count=0;
	
	public static void main(String[] args) throws InterruptedException {
		final String HOME = args[0];
		final long windowSize = Long.parseLong(args[1]);
		final String INPUT = HOME + "/input/CQELS";
		final String QUERYLIST = INPUT + "/involved_queries.txt";
		
		Config.MJOIN_NORMALIZE = false;
		Config.MEMORY_REUSE = false;
		Config.PRINT_LOG = true;
		Config.INDEX_SETUP_OPTION = 0;
		
		final CqelsplusExecContext context = new CqelsplusExecContext(HOME, true);
		
		ExecContextFactory.setExecContext(context);
	
		FileManager filemanager = FileManager.get();
        String queryFileNames = filemanager.readWholeFileAsUTF8(QUERYLIST);
        String[] queryNames = queryFileNames.split("\n");
        String[] queries = new String[queryNames.length];
        for (int i = 0; i < queryNames.length; i ++) {
        	queries[i] = filemanager.readWholeFileAsUTF8(queryNames[i]);
        	queries[i] = generateQuery(queries[i], windowSize);
        }
			
		List<QueryRouter> cqs = context.engine().registerMultipleQueries(queries);
	}
	
	private static String generateQuery(String template, long windowSize)
	{
		return template.replaceAll("COUNTSIZE", Long.toString(windowSize));
	}

}
