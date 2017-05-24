package cqelsplus.execplan.mjoinnetwork;

import java.util.ArrayList;
import java.util.Hashtable;
import java.util.List;

import org.apache.log4j.Logger;

import com.hp.hpl.jena.sparql.algebra.Op;
import com.hp.hpl.jena.sparql.algebra.op.OpQuadPattern;

import cqelsplus.engine.Config;
import cqelsplus.execplan.oprouters.MJoinRouter;
import cqelsplus.execplan.utils.OpUtils;
import cqelsplus.execplan.windows.CountVirtualWindow;
import cqelsplus.execplan.windows.PhysicalWindow;
import cqelsplus.execplan.windows.TimeVirtualWindow;
import cqelsplus.execplan.windows.VirtualWindow;
import cqelsplus.execplan.windows.WindowManager;
import cqelsplus.logicplan.algerba.OpMJoin;
import cqelsplus.logicplan.algerba.OpStream;
import cqelsplus.queryparser.All;
import cqelsplus.queryparser.Now;
import cqelsplus.queryparser.RangeWindow;
import cqelsplus.queryparser.TripleWindow;

public class MJoinRouterBuilder {
	/**Table of corresponding class pair*/
	Hashtable<Class, Class> classMap;
	WindowManager wm;
	List<OpMJoin> newOps;
	List<OpMJoin> oldOps;
	List<MJoinRouter> mjoinRouters;
	List<MJoinRouter> newMJoinRouters;
	Logger logger  = Logger.getLogger(MJoinRouterBuilder.class);
	
/**	public MJoinRouterBuilder(List<OpMJoin> ops) {
		this.wm = WindowManager.getInstance();
		this.newMJoinRouters = new ArrayList<MJoinRouter>();
		this.newOps = ops;
		this.oldOps = null;
		init();
		createNewMJoinRouters();
	}*/
	
	public MJoinRouterBuilder(List<OpMJoin> oldOps, List<OpMJoin> newOps) {
		this.wm = WindowManager.getInstance();
		this.newMJoinRouters = new ArrayList<MJoinRouter>();
		this.oldOps = oldOps;
		this.newOps = newOps;
		init();
		if (!oldOps.isEmpty()) {
			createOldMJoinRouters();
		}
		if (!newOps.isEmpty()) {
			createNewMJoinRouters();
		}
	}

	void init() {
    	classMap = new Hashtable<Class, Class>();
    	classMap.put(RangeWindow.class, TimeVirtualWindow.class);
    	classMap.put(TripleWindow.class, CountVirtualWindow.class);
    	classMap.put(Now.class, TimeVirtualWindow.class);
    	classMap.put(All.class, CountVirtualWindow.class);

    	
    	mjoinRouters = new ArrayList<MJoinRouter>();
	}
	
	/**Plan contains both old and new queries*/
	private void createOldMJoinRouters() {
		/***/
		wm.restartIdent4VirtualWindow();
		for (int i = 0; i < oldOps.size(); i++) {
			OpMJoin opMJoin = oldOps.get(i);
		    /** separate operators into 4 groups */
	    	ArrayList<OpStream> streamOps = new ArrayList<OpStream>();
	    	ArrayList<OpQuadPattern> staticOps = new ArrayList<OpQuadPattern>();
	    	ArrayList<Op> others = new ArrayList<Op>();

	    	separateOpertors(opMJoin, streamOps, staticOps, others);

	    	/**construct virtual windows based on operator types*/
	    	List<VirtualWindow> vWs = new ArrayList<VirtualWindow>();

	    	List<VirtualWindow> tempVWs = createWindowsBasedOnStreamOps(streamOps);
	    	for (VirtualWindow vW : tempVWs) {
	    		VirtualWindow identicalVW = wm.getIdenticalVW(vW);
				if (identicalVW != null) {
		    		vWs.add(identicalVW);
		    	} else {
		    		logger.warn("Can't find old virtual window");
		    	}
	    	}
	    	
	    	tempVWs = createWindowBasedOnStaticOps(staticOps);
	    	for (VirtualWindow vW : tempVWs) {
	    		VirtualWindow identicalVW = wm.getIdenticalVW(vW);
				if (identicalVW != null) {
		    		vWs.add(identicalVW);
		    	} else {
		    		logger.warn("Can't find old virtual window");
		    	}
	    	}
	    	
	    	tempVWs = createWindowBasedOnOtherOperators(others);
	    	for (VirtualWindow vW : tempVWs) {
	    		VirtualWindow identicalVW = wm.getIdenticalVW(vW);
				if (identicalVW != null) {
		    		vWs.add(identicalVW);
		    	} else {
		    		logger.warn("Can't find old virtual window");
		    	}
	    	}
	    	/**Create the MJoin router for considering MJoin operator*/
	    	MJoinRouter mjRouter = new MJoinRouter(opMJoin);
	    	mjRouter.SetSuspend(true);
	    	/**Basically, this router is not in charge outputting result until its old execution plan is stopped*/
	    	mjRouter.setVirtualWindows(vWs);
	    	for (VirtualWindow v : vWs) { 
	    		v.setCurrentMJoinRouter(mjRouter);
	    		if (Config.PRINT_LOG) {
	    			System.out.print("vw Id: " + v.getId() + " " + v.getOp().toString());
	    		}
	    	}
	    	mjoinRouters.add(mjRouter);
	   	}
	}
	
	/**Plan contains new queries*/
	private void createNewMJoinRouters() {
		List<VirtualWindow> allVWs = new ArrayList<VirtualWindow>();
		for (int i = 0; i < newOps.size(); i++) {
			OpMJoin opMJoin = newOps.get(i);
		    /** separate operators into 4 groups */
	    	ArrayList<OpStream> streamOps = new ArrayList<OpStream>();
	    	ArrayList<OpQuadPattern> staticOps = new ArrayList<OpQuadPattern>();
	    	ArrayList<Op> others = new ArrayList<Op>();

	    	separateOpertors(opMJoin, streamOps, staticOps, others);
	    	/**construct virtual windows based on operator types*/
	    	List<VirtualWindow> vWs = new ArrayList<VirtualWindow>();
	    	List<VirtualWindow> tVWs = createWindowsBasedOnStreamOps(streamOps);
	    	vWs.addAll(tVWs);
	    	
	    	tVWs = createWindowBasedOnStaticOps(staticOps);
	    	vWs.addAll(tVWs);
	    	
	    	tVWs = createWindowBasedOnOtherOperators(others);
	    	vWs.addAll(tVWs);
	    	/**Create the MJoin router for considering MJoin operator*/
	    	MJoinRouter mjRouter = new MJoinRouter(opMJoin);
	    	mjRouter.SetSuspend(true);
	    	mjRouter.setVirtualWindows(vWs);
	    	//each virtual window is just belonging to 1 router
	    	for (VirtualWindow v : vWs) { 
	    		//v.setMJoinRouter(mjRouter);
	    		/**Start re-engineering*/
	    		v.setCurrentMJoinRouter(mjRouter);
	    		/**End re-engineering*/
	    		if (Config.PRINT_LOG) {
	    			System.out.print("vw Id: " + v.getId() + " " + v.getOp().toString());
	    		}
	    	}
	    	/**Save the information of created virtual windows*/
	    	allVWs.addAll(vWs);
	    	mjoinRouters.add(mjRouter);
	    	newMJoinRouters.add(mjRouter);
	   	}
		/**Add virtual window to its physical windows*/
		for (VirtualWindow v : allVWs) {
			v.getPW().addVW(v);
		}
		/**Save the information of all virtual windows*/
	   	wm.addVirtualWindows(allVWs);
	}
	
	/**
	 * Separate Operators into 3 groups: stream ops, static ops and other ops
	 */
	void separateOpertors(OpMJoin opMJoin, List<OpStream> streamOps, 
			List<OpQuadPattern> staticOps, List<Op> others) {
		for (Op op : opMJoin.getElements()) {
			if (op.getClass().equals(OpStream.class)) {
				streamOps.add((OpStream)op);
				continue;
			}
			if (op.getClass().equals(OpQuadPattern.class)) {
				staticOps.add((OpQuadPattern)op);
				continue;
			}
			others.add(op);
		}
	}
	
	/**
	 * Create physical windows based on stream operators 
	 */
	List<VirtualWindow> createWindowsBasedOnStreamOps(List<OpStream> streamOps) {
		List<VirtualWindow> vWs = new ArrayList<VirtualWindow>();
    	for (OpStream op : streamOps) {
    		PhysicalWindow pW = wm.getPhysicalWindow(OpUtils.ptCode(op.getQuad()));
    		VirtualWindow vW = null;
			try {
				vW = (VirtualWindow)classMap.get(op.getWindow().getClass()).newInstance();
				vW.set(pW, op);
				vW.init();
			} 
			catch (Exception e) {
				e.printStackTrace();
			}
    		vWs.add(vW);
    	}
    	return vWs;
	}
	
	/**
	 * Create physical windows based on static operators 
	 */
	List<VirtualWindow> createWindowBasedOnStaticOps(List<OpQuadPattern> staticOps) {
		List<VirtualWindow> vWs = new ArrayList<VirtualWindow>();
    	for (OpQuadPattern op : staticOps) {
      		PhysicalWindow pW = wm.getStaticWindow(OpUtils.ptCode(op.getPattern().get(0)));
      		VirtualWindow vW = new VirtualWindow(pW, op); 
    		vWs.add(vW);
    	}
    	return vWs;
	}
	
	List<VirtualWindow> createWindowBasedOnOtherOperators(List<Op> others) {
		List<VirtualWindow> vWs = new ArrayList<VirtualWindow>();
		for (Op otherOp : others) {
			PhysicalWindow pW = wm.getBufferingWindow(otherOp.getName());
			VirtualWindow vW  = new VirtualWindow(pW, otherOp);
    		vWs.add(vW);
		}
		return vWs;
	}
	
	public List<MJoinRouter> getAllMJoinRouters() {
		return mjoinRouters;
	}


	public List<MJoinRouter> getJustNewMJoinRouter() {
		return newMJoinRouters;
	}
	
	public MJoinRouter getMJoinRouterAt(int i) {
		return mjoinRouters.get(i);
	}
	
	public List<OpMJoin> getOpMJoinList() {
		if (!oldOps.isEmpty()) {
			List<OpMJoin> val = new ArrayList<OpMJoin>(oldOps);
			val.addAll(newOps);
			return val;
		}
		return this.newOps;
	}

}
