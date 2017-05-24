package cqelsplus.execplan.oprouters;

/**
 * This class implements the algorithm that matches triple to corresponding basic pattern in query 
 * */
import java.util.ArrayList;
import java.util.BitSet;
import java.util.HashMap;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.log4j.Logger;

import com.hp.hpl.jena.sparql.algebra.Op;
import com.hp.hpl.jena.sparql.core.Quad;

import cqelsplus.engine.ExecContextFactory;
import cqelsplus.execplan.data.EnQuad;
import cqelsplus.execplan.data.PhysicalOp;
import cqelsplus.execplan.windows.PhysicalWindow;


public class PtMatchingRouter implements PhysicalOp, OpRouter {
	BitSet sMatch;
	long longVar = -1;
	HashMap<Long, BitSet> gIndex, sIndex, pIndex, oIndex;
	ArrayList<PhysicalWindow> obs;
	BitSet stopFlags;
	boolean deallocated = false;
	Logger logger = Logger.getLogger(PtMatchingRouter.class);
	
	Lock lock = new ReentrantLock(true);
	
	private static PtMatchingRouter pp;
	
	private PtMatchingRouter() {
		init();
		stopFlags = new BitSet();
	}
	
	public static PtMatchingRouter getInstance() {
		if (pp == null) {
			pp = new PtMatchingRouter();
		}
		return pp;
	}
	
	public Lock getLock() {
		return lock;
	}

	void init() {
		this.gIndex = new HashMap<Long, BitSet>();
		this.gIndex.put(longVar, new BitSet());
		
		this.sIndex = new  HashMap<Long, BitSet>();
		this.sIndex.put(longVar, new BitSet());
		
		this.pIndex = new  HashMap<Long, BitSet>();
		this.pIndex.put(longVar, new BitSet());
		
		this.oIndex = new  HashMap<Long, BitSet>();
		this.oIndex.put(longVar, new BitSet());
		
		this.obs = new ArrayList<PhysicalWindow>();
	}
	
	public void reset() {
		init();
	}
	public void addObserver(PhysicalWindow pW, Quad quad) {
		//if no singleton then no need to check the physical window is unique 
		//as every turn of window initialization assures this property in advanced 
//		//first, check if pw has existed inside the pt matching

		for (PhysicalWindow p : obs) {
			if (p.getId() == pW.getId()) {
				return;
			}
		}
		//logger.info("before lock.lock();");
		//logger.info("thread id before addObserver lock: " + Thread.currentThread().getId());
		//lock.lock();
		//logger.info("thread id after addObserver lock: " + Thread.currentThread().getId());
		obs.add(pW);
		//logger.info("finished obs.add(pW);");
		/**why do we need to add input pattern matching for window ?
		*pW.addInputPtmr(this);*/
		buildIndex(quad);
		//lock.unlock();
	}

	private void buildIndex(Quad quad) {
		int obsLength = obs.size() - 1;
		long var = longVar;
		
		if(!quad.getGraph().isVariable()) {
			var = ExecContextFactory.current().engine().encode(quad.getGraph());
		}
		BitSet qrList = gIndex.get(var);
		if (qrList == null) {
			qrList = new BitSet();
		} 
		qrList.set(obsLength);
		
		gIndex.put(var, qrList);
		
		var = longVar;
		if(!quad.getSubject().isVariable()) {
			var = ExecContextFactory.current().engine().encode(quad.getSubject());
		}
		qrList = sIndex.get(var);
		if (qrList == null) {
			qrList = new BitSet();
		}
		qrList.set(obsLength);
		sIndex.put(var, qrList);
		
		var = longVar;
		if(!quad.getPredicate().isVariable()) {
			var = ExecContextFactory.current().engine().encode(quad.getPredicate());
		}
		qrList = pIndex.get(var);
		if (qrList == null) {
			qrList = new BitSet();
		}
		qrList.set(obsLength);
		pIndex.put(var, qrList);
		
		var = longVar;
		if(!quad.getObject().isVariable()) {
			var = ExecContextFactory.current().engine().encode(quad.getObject());
		}
		qrList = oIndex.get(var);
		if (qrList == null) {
			qrList = new BitSet();
		}
		qrList.set(obsLength);
		oIndex.put(var, qrList);
		//logger.info("finished buildIndex");
	}

	public void process(EnQuad enQuad) {
		BitSet gs, ss, ps, os;
		//g
		//logger.info("thread id before process lock: " + Thread.currentThread().getId());
		lock.lock();
		//logger.info("thread id after process lock: " + Thread.currentThread().getId());
		try {
			gs = (BitSet)((BitSet)gIndex.get(enQuad.getGID()).clone());
		} catch(Exception e) {
			gs = (BitSet)((BitSet)gIndex.get(longVar).clone());
		}		
		
		//s
		try {
			ss = (BitSet)((BitSet)sIndex.get(enQuad.getSID())).clone();
		} catch(Exception e) {
			ss = (BitSet)((BitSet)sIndex.get(longVar).clone());
		}
		
		//p
		try {
			ps = (BitSet)((BitSet)pIndex.get(enQuad.getPID())).clone();
		} catch(Exception e) {
			ps = (BitSet)((BitSet)pIndex.get(longVar)).clone();
		}
		
		//o
		try {
			os = (BitSet)((BitSet)oIndex.get(enQuad.getOID())).clone();
		} catch(Exception e) {
			os = (BitSet)((BitSet)oIndex.get(longVar)).clone();
		}
		
		ps.and(os);
		ss.and(ps);
		gs.and(ss);
		for (int j = gs.nextSetBit(0); j >= 0; j = gs.nextSetBit(j+1)) {
			PhysicalWindow pW = obs.get(j);
			pW.handleNewEnQuad(enQuad);
		}
		lock.unlock();
	}
	
	@Override
	public void deallocate() {
		gIndex = sIndex = pIndex = oIndex = null;
		for (PhysicalWindow pW : obs) {
			pW.deallocate();
		}
		obs = null;
		deallocated = true;
	}
	
	public boolean isDeallocated() {
		return deallocated;
	}

	@Override
	public Op getOp() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void routeNewArrival(Object object) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void routeExpiration(Object object) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public int getId() {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public void addNextRouter(OpRouter nextRouter) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public OpRouter getNextRouter() {
		// TODO Auto-generated method stub
		return null;
	}
}
