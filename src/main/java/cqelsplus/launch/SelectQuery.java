package cqelsplus.launch;


import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;

import com.hp.hpl.jena.graph.Node;
import com.hp.hpl.jena.sparql.core.Var;
import com.hp.hpl.jena.util.FileManager;

import cqelsplus.engine.CqelsplusExecContext;
import cqelsplus.engine.ExecContext;
import cqelsplus.engine.ExecContextFactory;
import cqelsplus.engine.Config;
import cqelsplus.engine.SelectListener;
import cqelsplus.execplan.data.IMapping;
import cqelsplus.execplan.oprouters.QueryRouter;

public class SelectQuery 
{
	static Node rfid = Node.createURI("http://deri.org/streams/rfid");
	static String CQELSHOME, CQELSDATA, QUERYNAME, STATICFILE, STREAMFILE;
    static int outAmount = 0;
    
	public static void main(String[] args) throws IOException {
		if(args.length < 6) {
			System.out.println("Not enough argument");
			System.exit(-1);
		}
		CQELSHOME = args[0];
		CQELSDATA = args[1];
		QUERYNAME = args[2];
		boolean STATICLOAD = Boolean.valueOf(args[3]);
		STATICFILE = args[4];
		STREAMFILE = args[5];
		
		Config.PRINT_LOG = true;
		FileManager filemanager = FileManager.get();
        String queryname = filemanager.readWholeFileAsUTF8(CQELSDATA+QUERYNAME);

        final ExecContext context = new CqelsplusExecContext(CQELSHOME, true);
        
        ExecContextFactory.setExecContext(context);

        if(STATICLOAD) {
        	context.loadDefaultDataset(CQELSDATA + "/" + STATICFILE);
            context.loadDataset("http://deri.org/floorplan/", CQELSDATA+"/floorplan.rdf");
        }

        BufferedReader reader = new BufferedReader(new FileReader(CQELSDATA + "/authors.text"));
        String name = reader.readLine();
        reader.close();
        QueryRouter qr = context.engine().registerSelectQuery(generateQuery(queryname, name));
        qr.addListener(new SelectListener() {
			
			@Override
			public void update(IMapping mapping) {
				String result = "";
				for (Var var : mapping.getVars()) {
					long value = mapping.getValue(var);
					if (value > 0) {
						result += context.engine().decode(value) + " ";
					} else {
						result += Long.toString(-value) + " ";
					}
				}
				System.out.println(result);
				outAmount++;
			}
		});

        TextStream ts1 = new TextStream(context, rfid.toString(), CQELSDATA + "/stream/" + STREAMFILE);
        new Thread(ts1).start();
//        try {
//        Thread.sleep(4000);
//        } catch (Exception e) {
//        	e.printStackTrace();
//        }
//
//        TextStream ts2 = new TextStream(context, rfid.toString(), CQELSDATA + "/stream/s2.stream");
//        new Thread(ts2).start();

	}

	private static String generateQuery(String template, String name) {
		return template.replaceAll("AUTHORNAME", name);
	}
		
	public static  Node n(String st) {
		return Node.createURI(st);
	}
}
