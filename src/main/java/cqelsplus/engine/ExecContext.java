package cqelsplus.engine;

import com.hp.hpl.jena.sparql.algebra.Op;
import com.hp.hpl.jena.sparql.core.DatasetGraph;
import com.hp.hpl.jena.sparql.engine.ExecutionContext;
import com.hp.hpl.jena.sparql.engine.QueryIterator;

public interface ExecContext {
	/**
	 * get the dataset
	 * @param dataUri 
	 */	
	public DatasetGraph getDataset();
	/**
	 * load graph pattern with the specified dataset
	 * @param op operator
	 * @param ds specified dataset
	 * @return query iterator  ExecContext
	 */	
	public QueryIterator loadGraphPattern(Op op, DatasetGraph ds);
	
	/**get the execution context for the filter operator*/
	public ExecutionContext getFilterCtx();
	
	/**get the Cqels engine*/
	public CqelsplusEngine engine();
	/**load the graph dataset*/
	public void loadDataset(String graphUri, String dataUri);
	/**load the default graph dataset*/
	public void loadDefaultDataset(String dataUri);
}
