package cqelsplus.execplan.windows;

import java.math.BigInteger;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.BitSet;
import java.util.Iterator;
import java.util.List;

import org.apache.log4j.Logger;

import com.hp.hpl.jena.sparql.algebra.op.OpQuadPattern;
import com.hp.hpl.jena.sparql.core.Quad;

import cqelsplus.engine.ExecContextFactory;
import cqelsplus.engine.Config;
import cqelsplus.execplan.data.Cont_Dep_ExpM;
import cqelsplus.execplan.data.DomEntry;
import cqelsplus.execplan.data.EnQuad;
import cqelsplus.execplan.data.ExpiredOpBatch;
import cqelsplus.execplan.data.ExpiringOp;
import cqelsplus.execplan.data.ITuple;
import cqelsplus.execplan.data.LeafTuple;
import cqelsplus.execplan.data.NLinks;
import cqelsplus.execplan.data.PhysicalOp;
import cqelsplus.execplan.data.iterator.NullIterator;
import cqelsplus.execplan.data.iterator.RingIterator;
import cqelsplus.execplan.indexes.DomainIndex;
import cqelsplus.execplan.mjoinnetwork.JoinGraph;
import cqelsplus.execplan.oprouters.WindowEventHandler;

public class PhysicalWindow implements AWB, PhysicalOp  { 
	/**Logger*/
	final static Logger logger = Logger.getLogger(PhysicalWindow.class);
	/**Physical window Id*/
	int windowId;
	
	/**Physical window code generated based on quad pattern*/
	public String sharedPatternsCode;
	
	/**List of virtual windows that share this physical window*/
	List<VirtualWindow> virtualWindows;
	
	/**The join graph for the join operator, this physical window is starting join candidate*/
	//protected List<JoinProbingGraph> joinProbingGraphs;
	protected JoinGraph joinGraph;
	
	/**variable id <-> varPos*/
	protected int[] varColumns = null;
	
	/**Array contains the encoded value(bit operator) of corresponding columns which are indexed.
	 Used internal inside this class*/
	List<Integer> indexedColumnsCodes;
	
	/**Array contains the actual value of columns indexed. These are stored in a bit set*/
	List<BitSet> bitIndexedColumns;
	/***/
	protected int[] columnsCode2ArrIdMap;

	protected ArrayDeque<LeafTuple> dataBuffer;

	WindowEventHandler eventHandler;
	
	/**PtMatchingRouter inputPtmr;
	/**we have to keep the last considered item in order to know when we should stop the "minor" flow
	"Minor" flow is defined in class FlowState*/
	long lastConsideredPoint = Long.MAX_VALUE;
	
	/**for expired mapping*/ 
	long lastValidTimestamp = Long.MAX_VALUE; 
	
	int lastUpdateCount;
	
	long maxTimeSize = Long.MIN_VALUE;
	
	long maxTripleSize = Long.MIN_VALUE;
	
	LeafTuple oldestValidQuad = null;
	
	/**Reengineering variables*/
	protected ArrayList<DomainIndex> indexes;
	/**Potential index composition: 
	 * 1.  g
	 * 2.  s 
	 * 3.  p
	 * 4.  o
	 * 5.  g s
	 * 6.  g p
	 * 7.  g o
	 * 8.  s p 
	 * 9.  s o
	 * 10. p o
	 * 11. g s p
	 * 12. g s o
	 * 13. s p o
	 * 14. g s p o*/
	/**Length of potential index arrays*/
	int lengthOfPotentialIndexArray = 14;
	/**array contain the column of variable in triple, e.g 
	 * g s p o
	 * 0 1 2 3
	 * 4 values if stream id is taken into account*/
	/**Length of potential states of index column combination*/
	int lenthOfPotentialIndexesState = 16;
	/**End re-engineering variables declaration*/
	/**
	 * Physical window's constructor
	 * @param code: the isoforms of all virtual windows patterns
	 * @param: id each window needs a unique id
	 */
	/**Manage update the size of the physical window using a list containing expire items*/
	List<LeafTuple> tailTuplesList;
	public PhysicalWindow(String sharedPatternsCode, int id) {
		this.windowId = id;
		this.sharedPatternsCode = sharedPatternsCode;
		this.dataBuffer = new ArrayDeque<LeafTuple>();
		this.virtualWindows = new ArrayList<VirtualWindow>();
		this.indexedColumnsCodes = new ArrayList<Integer>();
		this.bitIndexedColumns = new ArrayList<BitSet>();
		this.columnsCode2ArrIdMap = new int[lenthOfPotentialIndexesState];
		Arrays.fill(columnsCode2ArrIdMap, -1);
		/**Start Re-engineering*/
		initIndexes();
		eventHandler = WindowEventHandler.getInstance();
		tailTuplesList = new ArrayList<LeafTuple>();
		//joinProbingGraphs = new ArrayList<JoinProbingGraph>();
		/**End of Re-engineering*/
	}
	
	public synchronized void addJoinProbingGraph(JoinGraph jg) {
		joinGraph = jg;
	}

//	public List<JoinProbingGraph> getJoinProbingGraphs() {
//		return joinProbingGraphs;
//	}
	public JoinGraph getJoinGraph() {
		return joinGraph;
	}
	
	/**The size of physical window = max size(all virtual windows)*/
	private void resize() {
		/**update max size for the physical window*/
		for (VirtualWindow vW : virtualWindows) {
			if (vW instanceof CountVirtualWindow) {
				if (maxTripleSize < ((CountVirtualWindow)vW).getWindowSize()) {
					maxTripleSize = ((CountVirtualWindow)vW).getWindowSize();
				}
			} else if (vW instanceof TimeVirtualWindow){
				if (maxTimeSize < ((TimeVirtualWindow)vW).getDuration()) {
					maxTimeSize = ((TimeVirtualWindow)vW).getDuration();
				}
			}
		}
	}
	
	/**Init variable information for physical window
	 * This step is needed when the indexes on the physical window is consider*/
	boolean initedVarInfo = false;
	private void initVars() {
		if (initedVarInfo) return;
		initedVarInfo = true;
		Quad quad = ((OpQuadPattern)virtualWindows.get(0).getOp()).getPattern().get(0);
		ArrayList<Integer> tmp = new ArrayList<Integer>();
		if((quad.getGraph() != null) && quad.getGraph().isVariable()) {
			tmp.add(0);
		}
		if(quad.getSubject().isVariable()) {
			tmp.add(1);
		}
		if(quad.getPredicate().isVariable()) {
			tmp.add(2);
		}
		if(quad.getObject().isVariable()) {
			tmp.add(3);
		}
		varColumns = new int[tmp.size()];
		for (int i = 0; i < tmp.size(); i ++)
			varColumns[i] = tmp.get(i);		
	}
	/**Add or supplement a new virtual window that refers to this physical window
	 * Physical window has to resize to cover all virtual windows if needed*/

	public void addVW(VirtualWindow vW) {
		synchronized (virtualWindows) {
			virtualWindows.add(vW);
			this.resize();
			initVars();
		}
	}

	public int getId() {
		return windowId;
	}

	public List<VirtualWindow> getVWs() {
		return virtualWindows;
	}
	
	public void removeVW(VirtualWindow vW) {
		int foundIdx = -1;
		for (int i = 0; i < virtualWindows.size(); i++) {
			VirtualWindow canVW = virtualWindows.get(i);
			if (vW.getId() == canVW.getId()) {
				foundIdx = i;
				break;
			}
		}
		if (foundIdx != -1) {
			synchronized (virtualWindows) {
				virtualWindows.remove(foundIdx);
			}
		}
	}
	
	/**For debugging purpose*/
	public void printLog() {
		System.out.println("Physical window id: " + this.windowId + " string code: " + this.sharedPatternsCode);
		for (VirtualWindow vw: virtualWindows) {
			System.out.print("---Virtual window " + vw.getId() + ": " + vw.getOp().toString());
		}
		System.out.println("Indexes on PW:");
		for (int i = 0; i < indexes.size(); i++) {
			BitSet b = bitIndexedColumns.get(i);
			for (int j = b.nextSetBit(0); j >= 0; j = b.nextSetBit(j+1)) {
				System.out.print(j + " ");
			}
			System.out.print(";");
		}
		System.out.println();
/**		System.out.print("Pid: " + this.pId + ", Combined index: ");
		for (int val : combinedColsIds) {
			for (int i = 0; i < 8; i ++) {
				if (((val >> i) & 1) == 1) {
					System.out.print(i + ", ");
				}
			}
			System.out.println("...");
		}
		System.out.println();*/
	}
	
	/**in the scope of re-engineering*/
	private void initIndexes()  {
		indexes = new ArrayList<DomainIndex>();
	}
	
	/**
	 *There are 2 ring-index types: single and multiple
	 *in case multiple index is considered, it is identified by a value of
	 *the combination of variable positions in triple pattern(or quad pattern).
	 *There are at most 4 positions in triple can be combined to create a multiple index. 
	 *Therefore the highest binary-combination value is 2^4 -1 = 15
	 * => we need an array of 8 elements to map*/
	public void addIndexColumns(ArrayList<Integer> indexedColumns) {
		int columnsCode = 0;
		BitSet b = new BitSet();
		/**turn on position bit, not higher than 3*/		
		for (int column : indexedColumns) {
			columnsCode = columnsCode | (1 << column);
			b.set(column);
		}

		if (!indexedColumnsCodes.contains(columnsCode)) {
			indexedColumnsCodes.add(columnsCode);
			bitIndexedColumns.add(b);
			columnsCode2ArrIdMap[columnsCode] = bitIndexedColumns.size() - 1;
			/**Start re-engineering*/
			indexes.add(ExecContextFactory.current().engine().createIndexes("hash", b.cardinality()));
			/**End re-engineering*/
		}
	}
		
	public void handleNewEnQuad(EnQuad enQuad) {		
		/**Add to indexed buff*/
		long[] vals = new long[varColumns.length];
		for (int i = 0; i < varColumns.length; i ++) {
			switch(varColumns[i]) {
			case 0:
				vals[i] = enQuad.getGID();
				break;
			case 1:
				vals[i] = enQuad.getSID();
				break;
			case 2:
				vals[i] = enQuad.getPID();
				break;
			case 3:
				vals[i] = enQuad.getOID();
				break;
			default:
				System.out.print("enQuad out of bound");
			}
		}
		LeafTuple leafTuple = new LeafTuple();
		leafTuple.set(vals);
		leafTuple.timestamp = System.nanoTime();
		leafTuple.nextNewer = null;
		leafTuple.setFrom(this);
		insert(leafTuple);
	}
	
	public void insert(ITuple leafTuple) {
		/**add the new leaf tuple to buffer list*/
		LeafTuple lastLT = dataBuffer.peekLast();
		if (lastLT != null) {
			lastLT.nextNewer = (LeafTuple)leafTuple;
		}
		((LeafTuple)leafTuple).prevOlder = lastLT;
		dataBuffer.add((LeafTuple)leafTuple);
		
		/**add new index key*/
		addNewIndexKeys(leafTuple);
		
		/**New tuple arrival can lead to the expiration of old tuples */
		
		/**Virtual windows all need to update the latest new item*/ 
		ExpiredOpBatch eob = new ExpiredOpBatch();
		synchronized (virtualWindows ) {
			for (VirtualWindow vW : virtualWindows) {
				List<Cont_Dep_ExpM> expiredTuples = vW.insert((LeafTuple)leafTuple);
				if (!expiredTuples.isEmpty()) {
					ExpiringOp e = new ExpiringOp();
					e.set(expiredTuples, vW.getItsCurrentMJoinRouter(), vW);
					eob.add(e);
				}
			}
		}
		
		/**The physical window slides corresponding to virtual windows*/
		LeafTuple oldestValidTuple = getOldestValidTailTuple();
		/**Purging the index after updating the new data items to stream window*/
		if (oldestValidTuple != null) {
			lastValidTimestamp = oldestValidTuple.timestamp;
			/**must cut the relation between the valid and invalid items*/
			LeafTuple firstInvalidLT = oldestValidTuple.prevOlder;
			oldestValidTuple.prevOlder = null;
			if (firstInvalidLT != null) {
				firstInvalidLT.nextNewer = null;
				purge(firstInvalidLT);
			}
		}
		try {
			eventHandler.handle(leafTuple, eob);
		} catch (Exception e) {
			e.printStackTrace();
		}
//
//		/**The triple-coming event leads to expiration event*/
//		/**coreExecutor.setExecutorService(Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors()));*/
//		joinExecutor.pushExpiring(eob);
//		/**prepare the skeleton linked item for the domain index*/
////		updateIndex(leafTuple);
//	try {
//			joinExecutor.pushProbing(leafTuple);
//		} catch (InterruptedException e) {
//			// TODO Auto-generated catch block
//			e.printStackTrace();
//		}
//		joinExecutor.execute();
	}

	void addNewIndexKeys(ITuple tuple) {
		/**Start re-engineering*/
		NLinks mappingLink = new NLinks(indexes.size());
		tuple.setLink(mappingLink);
		for (int i = 0; i < indexes.size(); i++) {
			BitSet b = bitIndexedColumns.get(i);
			if (b.cardinality() == 1) {
				long key = tuple.get(b.nextSetBit(0));
				DomainIndex t = indexes.get(i);
				t.add(i, key, tuple);
			} else {
				BigInteger key = BigInteger.valueOf(0);
				int tmp = 0;
				for (int j = b.nextSetBit(0); j >= 0; j = b.nextSetBit(j+1)) {
					BigInteger bi = BigInteger.valueOf(tuple.get(j));
					key = key.shiftLeft(64 * tmp).or(bi); 
					tmp++;
				}
				DomainIndex t = indexes.get(i);
				t.add(i, key, tuple);
			}				
		}
		/**End re-engineering*/		
	}

	/**the last valid time-stamp of physical window is calculated by the formula:
	lastvalidTimestamp = minimum(lastvalidTimestamp{all triple windows}, lastvalidTimestamp{all time windows}*/
	public void updateOldestInvalidItem(LeafTuple tuple) {
		/**when 1 mapping comes, the update process of all virtual windows happens, 
		lastUpdateCount is needed to make sure the last valid timestamp is updated properly when update process happens*/
		if (lastUpdateCount == 0) {
			lastValidTimestamp = Long.MAX_VALUE;
		}
		try {
		if (lastValidTimestamp > tuple.timestamp) {
			lastValidTimestamp = tuple.timestamp;
			oldestValidQuad = tuple;
		}
		} catch (Exception e) {
			logger.error(e);
		}
		lastUpdateCount++;
	}
	
	public LeafTuple getOldestValidTailTuple() {
		synchronized(tailTuplesList) {
			if (tailTuplesList.isEmpty()) {
				return null;
			}
			LeafTuple candidate = null;
			long oldestValidTimestamp = Long.MAX_VALUE;
			for (LeafTuple tail : tailTuplesList) {
				if (tail.timestamp < oldestValidTimestamp) {
					oldestValidTimestamp = tail.timestamp;
					candidate = tail;
				}
			}
			tailTuplesList.clear();
			return candidate;
		}
	}
	
	public void updateExpTupleFromVW(LeafTuple tailTuple) {
		synchronized(tailTuplesList) {
			tailTuplesList.add(tailTuple);
		}
	}
		
	@SuppressWarnings("unchecked")
	public Iterator<ITuple> probe(int probedWinColumn, ITuple mu, ArrayList<Integer> probedMuColumns, long tick) {
		DomEntry entry = null;
		if (probedMuColumns.size() == 1) {//single index
			long key = mu.get(probedMuColumns.get(0));
			entry = indexes.get(columnsCode2ArrIdMap[probedWinColumn]).get(key);
		} else {
			BigInteger key = BigInteger.valueOf(0);
			for (int j = 0; j < probedMuColumns.size(); j++) {//combined index
				key = key.shiftLeft(64 * j).or(BigInteger.valueOf(mu.get(probedMuColumns.get(j)))); 
			}
			 entry = indexes.get(columnsCode2ArrIdMap[probedWinColumn]).get(key);
		}
		if (entry != null) {
			return new RingIterator((LeafTuple)entry.getLink(), tick, lastValidTimestamp,
					columnsCode2ArrIdMap[probedWinColumn], entry.count()); 
		}

		return NullIterator.instance();
	}

	
	public void purge(ITuple firstInvalidTuple) {
		synchronized (indexes) {
			LeafTuple p, head = (LeafTuple)firstInvalidTuple;
			while(head != null) {
				p = head;
				head = head.prevOlder;
				if (head != null) {
					head.nextNewer = null;
				}
				for(int i=0; i<indexes.size(); i++) {
					BitSet b = bitIndexedColumns.get(i);
					if (b.cardinality() == 1) {
						long key = p.get(b.nextSetBit(0));
						indexes.get(i).remove(key, p);
					} else {
						BigInteger key = BigInteger.valueOf(0);
						int tmp = 0;
						for (int j = b.nextSetBit(0); j >= 0; j = b.nextSetBit(j+1)) {
							BigInteger bi = BigInteger.valueOf(p.get(j));
							key = key.shiftLeft(64 * tmp).or(bi); 
							tmp++;
						}
						indexes.get(i).remove(key, p);
					}
				}
				dataBuffer.remove(p);
				if (Config.MEMORY_REUSE) {
					p.releaseInstance();
				}
			}
		}
	}

	@Override
	public void deallocate() {
		// TODO Auto-generated method stub
		
	}

	public long getSize() {
		// TODO Auto-generated method stub
		return dataBuffer.size();
	}
}

