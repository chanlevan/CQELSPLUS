package cqelsplus.engine;


import java.math.BigInteger;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.List;
import java.util.logging.Logger;

import com.hp.hpl.jena.graph.Node;
import com.hp.hpl.jena.graph.Triple;
import com.hp.hpl.jena.query.Query;
import com.hp.hpl.jena.sparql.algebra.Op;
import com.hp.hpl.jena.sparql.core.Quad;
import com.hp.hpl.jena.tdb.base.file.FileFactory;
import com.hp.hpl.jena.tdb.base.file.FileSet;
import com.hp.hpl.jena.tdb.index.IndexBuilder;
import com.hp.hpl.jena.tdb.nodetable.NodeTable;
import com.hp.hpl.jena.tdb.nodetable.NodeTableNative;
import com.hp.hpl.jena.tdb.store.NodeId;
import com.hp.hpl.jena.tdb.sys.SystemTDB;

import cqelsplus.execplan.ExecPlan;
import cqelsplus.execplan.data.EnQuad;
import cqelsplus.execplan.data.POOL;
import cqelsplus.execplan.data.PoolableObjectFactory;
import cqelsplus.execplan.indexes.DSISortedMap;
import cqelsplus.execplan.indexes.DomainIndex;
import cqelsplus.execplan.indexes.SortedMap;
import cqelsplus.execplan.oprouters.PtMatchingRouter;
import cqelsplus.execplan.oprouters.QueryRouter;
import cqelsplus.execplan.windows.PhysicalWindow;
import cqelsplus.execplan.windows.WindowManager;
import cqelsplus.logicplan.SingleQueryLogicalCompiler;
import cqelsplus.queryparser.ParserCQELS;
/** 
 * This class implements CQELS engine. It has an Esper Service provider and context belonging to
 * 
 * @author		Danh Le Phuoc
 * @author 		Chan Le Van
 * @organization DERI Galway, NUIG, Ireland  www.deri.ie
 * @email 	danh.lephuoc@deri.org
 * @email   chan.levan@deri.org
 */
public class CqelsplusEngine{
	NodeTable  dictionary;
	ExecContext context;
    PtMatchingRouter pmRouter;
	QueryMigrator queryMigrator;
	SingleQueryLogicalCompiler queryCompiler;
	ParserCQELS parser;
	/**
	 * @param context the context this engine belonging to
	 */
	Logger logger = Logger.getLogger(this.getClass().getName());
	public CqelsplusEngine(ExecContext context) {
		this.context = context;
		this.dictionary = new NodeTableNative(IndexBuilder.mem().newIndex(FileSet.mem(), 
		  		SystemTDB.nodeRecordFactory), 
		  FileFactory.createObjectFileMem());
		queryCompiler = new SingleQueryLogicalCompiler(0);
		parser = new ParserCQELS();
		pmRouter = PtMatchingRouter.getInstance();
		queryMigrator = QueryMigrator.getInstance();
	}
	
	/**
	 * send a quad to to engine
	 * @param quad the quad will be sent after encoded
	 */
	public void send(Quad quad) {
		pmRouter.process(encode(quad));
	}
	
	/**
	 * send a quad which is represented as a graph node and a triple to engine
	 * @param graph graph node
	 * @param triple
	 */
	public void send(Node graph, Triple triple) {
		pmRouter.process(encode(graph, triple));
	}
/**		while (ep.isRegistering()) {
			if (Settings.PRINT_LOG) {
				System.out.println("Stream delaying because of query is registering to the system");
			}
			try {
				synchronized (ExecPlan.monitorRegistering) {
					ExecPlan.monitorRegistering.wait();
				}

			} catch (Exception e) {
				e.printStackTrace();
			}			
		}
		synchronized (pmRouters) {		
			for (PtMatchingRouter pmRouter : pmRouters) {
				if (pmRouter.getFlowState() != FlowState.DEPRICATED) {
					pmRouter.process(encode(graph, triple));
				} else if (!pmRouter.isDeallocated()) {
					pmRouter.deallocate();
				}
			}
		}*/

//		pmRouter.process(encode(graph, triple));
	
	/**
	 * send a quad which is represented as a graph, subject, 
	 * predicate and object node to engine
	 * @param graph graph node
	 * @param s subject
	 * @param p predicate
	 * @param o object
	 */
	public void send(Node graph, Node s, Node p, Node o) {
		pmRouter.process(new EnQuad(encode(graph), encode(s), encode(p), encode(o)));
	}
       
	private EnQuad encode(Quad quad) {
		return new EnQuad(encode(quad.getGraph()), encode(quad.getSubject()), 
					      encode(quad.getPredicate()), encode(quad.getObject()));
	}
	
	private EnQuad encode(Node graph,Triple triple) {
		return new EnQuad(encode(graph), encode(triple.getSubject()), 
				          encode(triple.getPredicate()), encode(triple.getObject()));
	}

	public <T> SortedMap<T> sortedMapGen(PoolableObjectFactory object) {
		return new DSISortedMap<T>(1, object);
	}

	public <T> DomainIndex<T>[] createIndexes(String idxType, ArrayList<BitSet> idxPosSet) {
		// TODO Auto-generated method stub
		DomainIndex[] ids = new DomainIndex[idxPosSet.size()];
		for (int i = 0; i < idxPosSet.size(); i++) {
			if (idxPosSet.get(i).cardinality() == 1) {
				if(idxType.equalsIgnoreCase("hash"))
				 ids[i]=new DSISortedMap<Long>(POOL.DomRingEntry);
				else if(idxType.equalsIgnoreCase("avl"))
				  ids[i]=new DSISortedMap<Long>(1,POOL.DomRingEntry);
				else 
					ids[i]=new DSISortedMap<Long>(2,POOL.DomRingEntry);
			} else {
				if(idxType.equalsIgnoreCase("hash"))
					 ids[i]=new DSISortedMap<BigInteger>(POOL.DomRingEntry);
					else if(idxType.equalsIgnoreCase("avl"))
					  ids[i]=new DSISortedMap<BigInteger>(1,POOL.DomRingEntry);
					else 
						ids[i]=new DSISortedMap<BigInteger>(2,POOL.DomRingEntry);				
			}
		}
		return ids;
	}

	public <T> DomainIndex<T> createIndexes(String idxType, int combinedIndexLength) {
		DomainIndex ids;
			if (combinedIndexLength == 1) {
				if(idxType.equalsIgnoreCase("hash"))
				 ids=new DSISortedMap<Long>(POOL.DomRingEntry);
				else if(idxType.equalsIgnoreCase("avl"))
				  ids=new DSISortedMap<Long>(1,POOL.DomRingEntry);
				else 
					ids=new DSISortedMap<Long>(2,POOL.DomRingEntry);
			} else {
				if(idxType.equalsIgnoreCase("hash"))
					 ids=new DSISortedMap<BigInteger>(POOL.DomRingEntry);
					else if(idxType.equalsIgnoreCase("avl"))
					  ids=new DSISortedMap<BigInteger>(1,POOL.DomRingEntry);
					else 
						ids=new DSISortedMap<BigInteger>(2,POOL.DomRingEntry);				
			}
		return ids;
	}
	
	public List<QueryRouter> registerMultipleQueries(String[] queryStrs) {
		Query[] queries = new Query[queryStrs.length];
		for (int i = 0; i < queryStrs.length; i ++) {
			queries[i] = new Query();
		}
		for (int i = 0; i < queries.length; i ++) {
		     parser.parse(queries[i], queryStrs[i]);
		}

		//1. compiling each query to logical binary-join-including query plan
		//2. transforming each logical binary-join-including query plan into each logical M-join-including query plan
		//3. generating routing policy by transforming all logical M-join-including into Multiple-M-join network query plan
		//Step 1 and 2
		List<Op> queryOps = new ArrayList<Op>();
		
		for (int i = 0; i < queries.length; i++) {
			
			queryOps.add(queryCompiler.compile(queries[i]));
		}
		//Step 3
		ExecPlan ep = queryMigrator.buildExecutionPlan(queryOps);
	
		return ep.getQueryRouters();
	}
	
	static Boolean registering = false;
	static Object monitor = new Object();
	
	public QueryRouter registerSelectQuery(String queryStr) {
		Query query = new Query();
		parser.parse(query, queryStr);
		/**query arriving rate > execution plan installing speed */
		//logger.info("start register");
		
		ExecPlan ep = queryMigrator.buildExecutionPlan(queryCompiler.compile(query));

		List<QueryRouter> qrs = ep.getQueryRouters();
		QueryRouter qr = qrs.get(qrs.size() - 1);
		qr.setQuery(query);

		//logger.info("finished registration");
		return qr;
	}

	public QueryRouter registerConstructQuery(String queryStr) {
		Query query = new Query();
		parser.parse(query, queryStr);
		ExecPlan ep = queryMigrator.buildExecutionPlan(queryCompiler.compile(query));
		
		List<QueryRouter> qrs = ep.getQueryRouters();
		QueryRouter qr = qrs.get(qrs.size() - 1);
		qr.setQuery(query);
		return qr;
	}
	
	public int getCreatedBufferNum(String queryStr) {
		Query query = new Query();
		parser.parse(query, queryStr);
		int result = queryMigrator.getCreatedBufferNum(queryCompiler.compile(query));
		return result;
	}

	public boolean unregisterQuery(int queryId) {
		return queryMigrator.removeQuery(queryId);
	}

	public long encode(Node node) { 
		return this.dictionary.getAllocateNodeId(node).getId();
	}

	public Node decode(long id) {
		return this.dictionary.getNodeForNodeId(NodeId.create(id));
	}

	public long getElementSize() {
		WindowManager wm = WindowManager.getInstance();
		long size = 0;
		for (PhysicalWindow pw : wm.getPWs()) {
			size += pw.getSize();
		}
		return size;
	}
}
