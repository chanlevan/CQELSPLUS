# Summary
This is a repo containing Java-based CQELS+ application, an RDF Stream Processing engine(RSP engine) processes input Linked Stream data(a time-dependent data streams following the principles of [Linked Data](http://linkeddata.org/)) and provides the answers for the registered continous queries(following the syntax of [CQELS query language](https://code.google.com/archive/p/cqels/wikis/CQELS_language.wiki)).

This application is an extension of the [original CQELS engine](https://code.google.com/archive/p/cqels/). It focuses on increasing the performance by optimizing join operations of involved registered queries.
# What in this folder
This folder contains:

**pom.xml**: The maven configuration file to compile the code.

**src/main/java/cqelsplus**: folder path contains the Java code.
# System Requirements
  [Java 1.7](http://www.oracle.com/technetwork/java/javase/downloads/index.html)
 
  [Maven 3.3](https://maven.apache.org/download.cgi)
# Application Compilation
Checkout the repo; navigate to the folder CQELSPLUS and type ```mvn clean compile```.
After this command is finished, there will be a target folder contains 2 jar files. The first one is ```cqelsplus-0.0.1-SNAPSHOT.jar``` and the second one is ```cqelsplus-0.0.1-SNAPSHOT-jar-with-dependencies.jar```. We usually use the second one to run the engine as itself has been included all the dependencies.

# Application Structure
The application includes 4 folders:
1. **queryparser:** contains the code related to parsing the String queries to Java objects.
2. **logicplan:** contains the code dealing with how an operator-tree of parsed query is formed.
3. **execplan:** contains the code dealing with operators in the physical level, i.e, how an operator in the query is implemented and how all the operators of the involved queries are connected together to evaluate the data.
4. **engine:** contains Java classes for the user to install and run the engine.
5. **launch:** contains several concrete examples to run the engine.

# How to work with the code

This example demonstrates how to work with the code: Initialize the execution context, register a query and listen to the output. It processes the Linked Stream data and evaluates CQELS-based queries from the [conference senario](https://link.springer.com/chapter/10.1007%2F978-3-642-33158-9_7). The important line of codes are annotated.

Example command: Navigate to the target folder and type ```java -classpath ./cqelsplus-0.0.1-SNAPSHOT-jar-with-dependencies.jar cqelsplus.launch.SelectQuery /media/chanlevan/D/Dev/CQELSHOME /media/chanlevan/D/Dev/cqels_data/ query1.cqels true 10k.rdf rfid_1000.stream```

```
package cqelsplus.launch;


import java.io.IOException;

import com.hp.hpl.jena.graph.Node;
import com.hp.hpl.jena.sparql.core.Var;
import com.hp.hpl.jena.util.FileManager;

import cqelsplus.engine.Config;
import cqelsplus.engine.CqelsplusExecContext;
import cqelsplus.engine.ExecContext;
import cqelsplus.engine.ExecContextFactory;
import cqelsplus.engine.SelectListener;
import cqelsplus.execplan.data.IMapping;
import cqelsplus.execplan.oprouters.QueryRouter;

public class Demo 
{
	static Node rfid = Node.createURI("http://deri.org/streams/rfid");
	
	static String CQELSHOME, CQELSDATA, QUERYNAME, STATICFILE, STREAMFILE;
  static int outAmount = 0;
    
	public static void main(String[] args) throws IOException {
		if(args.length < 6) {
			System.out.println("Not enough argument");
			System.exit(-1);
		}
		//specify the home directory for the engine
		CQELSHOME = args[0];
		
		//specify the data folder: input queries, stream and static data.
		CQELSDATA = args[1];
		
		//name of the involved query
		QUERYNAME = args[2];
		
		//specify if the static data is needed in the context of this query
		boolean STATICLOAD = Boolean.valueOf(args[3]);
		
		//specify name of the static data file
		STATICFILE = args[4];
		
		//specify name of the stream data file
		STREAMFILE = args[5];
		
		//turn on the log output
		Config.PRINT_LOG = true;
		
		//read the input query
    FileManager filemanager = FileManager.get();
    String queryname = filemanager.readWholeFileAsUTF8(CQELSDATA+QUERYNAME);

    //Initialize the execution context in these next 2 lines	
    final ExecContext context = new CqelsplusExecContext(CQELSHOME, true);
    ExecContextFactory.setExecContext(context);

    //Load the static data in case they are involved
    if(STATICLOAD) {
      context.loadDefaultDataset(CQELSDATA + "/" + STATICFILE);
        context.loadDataset("http://deri.org/floorplan/", CQELSDATA+"/floorplan.rdf");
    }

    //Register the query to the engine
    QueryRouter qr = context.engine().registerSelectQuery(queryname);

    //add the listener to get the answer
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

    //Start the input data by invoking another thread
    TextStream ts1 = new TextStream(context, rfid.toString(), CQELSDATA + "/stream/" + STREAMFILE);
    new Thread(ts1).start();
	}
}
```
# Work with the Input Data
The above example works with the [conference senario data](https://code.google.com/archive/p/cqels/downloads)

For RSP engine data, there are some other popular benchmarks: 

[SRBench](https://www.w3.org/wiki/SRBench)

[CityBench](https://github.com/CityBench/Benchmark)

