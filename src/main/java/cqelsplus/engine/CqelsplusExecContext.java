package cqelsplus.engine;

import java.io.File;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import org.apache.log4j.Logger;

import com.hp.hpl.jena.graph.Node;
import com.hp.hpl.jena.sparql.algebra.Algebra;
import com.hp.hpl.jena.sparql.algebra.Op;
import com.hp.hpl.jena.sparql.core.DatasetGraph;
import com.hp.hpl.jena.sparql.engine.ExecutionContext;
import com.hp.hpl.jena.sparql.engine.QueryIterator;
import com.hp.hpl.jena.tdb.TDBFactory;
import com.hp.hpl.jena.tdb.base.file.Location;
import com.hp.hpl.jena.tdb.solver.OpExecutorTDB;
import com.hp.hpl.jena.tdb.store.DatasetGraphTDB;
import com.hp.hpl.jena.tdb.store.bulkloader.BulkLoader;

public class CqelsplusExecContext implements ExecContext {
	CqelsplusEngine engine;
	Properties config;
	HashMap<String, Object> hashMap;
	DatasetGraphTDB dataset;
	Location location;
	ExecutionContext arqExCtx;
	Map<String,QueryIterator> map = new HashMap<String, QueryIterator>();
	Logger logger = Logger.getLogger(CqelsplusExecContext.class);
    /**
	 * @param path home path containing dataset
	 * @param cleanDataset a flag indicates whether the old dataset will be cleaned or not
	 */
	public CqelsplusExecContext(String path, boolean cleanDataset) {
		this.hashMap = new HashMap<String, Object>();
		//combine cache and disk-based dictionary
		this.engine = new CqelsplusEngine(this);
		createCache(path + "/cache");
		if (cleanDataset) {
			cleanNCreate(path + "/datasets");
		}
		createDataSet(path + "/datasets");
	}
	
	static void cleanNCreate(String path) {
		deleteDir(new File(path));
		if(!(new File(path)).mkdir()) {
			System.out.println("can not create working directory"+path);
		}
	}
	
	/**
	 * to delete a directory
	 * @param dir directory will be deleted
	 */
	public static boolean deleteDir(File dir) {
        if (dir.isDirectory()) {
            String[] children = dir.list();
            for (int i=0; i<children.length; i++) {
                boolean success = deleteDir(new File(dir, children[i]));
                if (!success) {
                	System.out.println("can not delete" +dir);
                    return false;
                }
            }
        }
        return dir.delete();
	}
	/**
	 * get the ARQ context
	 */
	public ExecutionContext getARQExCtx() {
		return this.arqExCtx;
	}
	/**
	 * create a dataset with a specified location
	 * @param location the specified location string
	 */
	public void createDataSet(String location) {
		this.dataset = TDBFactory.createDatasetGraph(location);
		this.arqExCtx = new ExecutionContext(this.dataset.getContext(), 
											this.dataset.getDefaultGraph(), 
											this.dataset, OpExecutorTDB.OpExecFactoryTDB);
	}
	
	/**
	 * load a dataset with the specified graph uri and data uri
	 * @param graphUri
	 * @param dataUri 
	 */
	public void loadDataset(String graphUri, String dataUri) {
		BulkLoader.loadNamedGraph(this.dataset, 
					Node.createURI(graphUri), Arrays.asList(dataUri) , false);
	}
	
	/**
	 * load a dataset with the specified data uri
	 * @param dataUri 
	 */
	public void loadDefaultDataset(String dataUri) {
		BulkLoader.loadDefaultGraph(this.dataset, Arrays.asList(dataUri) , false);
	}
	
	/**
	 * get the dataset
	 * @param dataUri 
	 */
	public DatasetGraph getDataset() { 
		return dataset;
	}
	
	/**
	 * create cache with the specified path
	 * @param cachePath path string 
	 */
	public void createCache(String cachePath) {
		cleanNCreate(cachePath);
	}
	
	public void put(String key, Object value) { this.hashMap.put(key, value); }
	
	/**
	 * get the value with the specified key
	 * @param key 
	 */
	public Object get(String key) { return this.hashMap.get(key); }
	
	/**
	 * init TDB graph with the specified directory
	 * @param directory  
	 */
	public void initTDBGraph(String directory) { 
		this.dataset = TDBFactory.createDatasetGraph(directory);
	}
	
	/**
	 * load graph pattern
	 * @param op operator
	 * @return query iterator  
	 */
	public QueryIterator loadGraphPattern(Op op) { 
		return Algebra.exec(op, this.dataset); 
	}
	
	/**
	 * load graph pattern with the specified dataset
	 * @param op operator
	 * @param ds specified dataset
	 * @return query iterator  ExecContext
	 */
	@Override	
	public QueryIterator loadGraphPattern(Op op, DatasetGraph ds) {
		QueryIterator qi = map.get(op.toString());
		qi = Algebra.exec(op, ds);
		return qi;
	}
	
	/**
	 * get the cache location
	 * @return cache location
	 */
	public Location cacheLocation() { return this.location; }
	
	/**
	 * get cache configuration
	 * @return cache configuration  
	 */
	public Properties cacheConfig() { return  this.config; }


	@Override
	public ExecutionContext getFilterCtx() {
		// TODO Auto-generated method stub
		return getARQExCtx();
	}

	@Override
	public CqelsplusEngine engine() {
		// TODO Auto-generated method stub
		return engine;
	}
}
