package cqelsplus.launch;


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

public class LSBenchTest 
{
	static Node postStream = Node.createURI("http://deri.org/streams/poststream");
	static Node likePostStream = Node.createURI("http://deri.org/streams/likedpoststream");
	static String CQELSHOME, CQELSDATA, QUERYNAME, STATICFILE, POSTSTREAM, LIKEPOSTSTREAM;
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
		POSTSTREAM = args[5];
		LIKEPOSTSTREAM = args[6];
		
		Config.PRINT_LOG = false;
		FileManager filemanager = FileManager.get();
        String query = filemanager.readWholeFileAsUTF8(QUERYNAME);

        final ExecContext context = new CqelsplusExecContext(CQELSHOME, true);
        
        ExecContextFactory.setExecContext(context);

        if(STATICLOAD) {
        	context.loadDefaultDataset(CQELSDATA + "/" + STATICFILE);
        }

        QueryRouter qr = context.engine().registerSelectQuery(query);
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

        TextStream rdfPostStream = new TextStream(context, postStream.toString(), CQELSDATA  + POSTSTREAM);
 
        new Thread(rdfPostStream).start();
        
        TextStream rdfLikePostStream = new TextStream(context, likePostStream.toString(), CQELSDATA + LIKEPOSTSTREAM);
        new Thread(rdfLikePostStream).start();
	}
		
	public static  Node n(String st) {
		return Node.createURI(st);
	}
}
