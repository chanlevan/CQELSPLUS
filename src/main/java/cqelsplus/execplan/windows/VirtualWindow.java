package cqelsplus.execplan.windows;

import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.Logger;

import com.hp.hpl.jena.sparql.algebra.Op;
import com.hp.hpl.jena.sparql.algebra.op.OpQuadPattern;

import cqelsplus.execplan.data.Cont_Dep_ExpM;
import cqelsplus.execplan.data.LeafTuple;
import cqelsplus.execplan.oprouters.MJoinRouter;
/**
 * This class is the base class of all specific virtual windows
 * 1 virtual window is belonging to 1 physical window
 * However, 1 virtual window can be belonging to more than 1 mjoin router 
 * as new query can come at another time and this results in a new mjoin execution plan is built
 */
public class VirtualWindow {
	final static Logger logger = Logger.getLogger(VirtualWindow.class);
	/**One Virtual window is belonging to 1 unique physical buffer*/
	protected PhysicalWindow pW;
	/**It represents for 1 BGP(stream or static) pattern*/
	protected Op op;
	/**It has head and tail to point to head and tail of data item*/
	protected LeafTuple newest;
	protected LeafTuple oldest;
	protected static int id = 0;
	protected int vId;
	protected boolean visited;
	protected VirtualWindow before;
	/**Start reengineering*/
	protected MJoinRouter currentMJoinRouter;
	/**List of old mjoin routers needed to be removed when 
	 * old execution plan is deprecated*/
	protected List<MJoinRouter> oldMJoinRouters;
	protected List<MJoinRouter> allMJoinRouters;
	protected boolean removed;
	protected boolean identicalized;
	
	/**End reengineering*/
	public VirtualWindow(PhysicalWindow pW, Op op) {
		this.pW = pW;
		this.op = op;
		newest = null;
		oldest = null;
		vId = id++;
		visited = false;
		before = null;
		/**Start re-engineering*/
		oldMJoinRouters = new ArrayList<MJoinRouter>();
		allMJoinRouters = new ArrayList<MJoinRouter>();
		setRemovedWindow(false);
		/**End re-engineering*/
	}
	
	public VirtualWindow() {
		newest = null;
		oldest = null;
		vId = id ++;
		visited = false;
		before = null;
		/**Start re-engineering*/
		oldMJoinRouters = new ArrayList<MJoinRouter>();
		allMJoinRouters = new ArrayList<MJoinRouter>();
		/**End re-engineering*/
	}
	/**
	 * These 3 methods will be overridden by time-based or triple-based windows 
	 */
	public void init(){};
	public List<Cont_Dep_ExpM> insert(LeafTuple mun){
		return null;
	};
	
	public boolean isExpired(LeafTuple candidate) {
		return false;
	}
	
	public void set(PhysicalWindow pW, OpQuadPattern op) {
		this.pW = pW;
		this.op = op;
	}

	public PhysicalWindow getPW() {
		return pW;
	}
	
	public Op getOp() {
		return op;
	}
	
	public int getId() {
		return vId;
	}

	@Override
	public int hashCode() {
		return this.vId;
	}
	
	@Override 
	public boolean equals(Object val) {
		return this.hashCode() == ((VirtualWindow)val).hashCode();
	}
	
	public void printLog(String deep) {
		System.out.print(deep + op.toString());
	}
	
	public void setVisited(boolean value) {
		this.visited = value;
	}
	
	public boolean isVisited() {
		return visited;
	}
	
	public void setBefore(VirtualWindow vw) {
		before = vw;
	}
	
	public VirtualWindow getBefore() {
		return before;
	}

	/**Start re-engineering*/
	public MJoinRouter getItsCurrentMJoinRouter() {
		return this.currentMJoinRouter;
	}
	
	public synchronized void setCurrentMJoinRouter(MJoinRouter newMJoinRouter) {
		oldMJoinRouters.add(currentMJoinRouter);
		allMJoinRouters.add(newMJoinRouter);
		this.currentMJoinRouter = newMJoinRouter;
	}
	
	public boolean isRemoved() {
		return removed;
	}
	
	public void setRemovedWindow(boolean val) {
		this.removed = val;
	}
	
	public List<MJoinRouter> getAllMJoinRouter() {
		return allMJoinRouters;
	}
	
	public boolean isIdentical(VirtualWindow v) {
		return op.equals(v.getOp());
	}
	
	public void setIdenticalized(boolean val) {
		identicalized = val;
	}
	/**End re-engineering*/

	public boolean isIdenticalized() {
		return identicalized;
	}
	/**Will be overriden by subclasses*/
	public void killme() {}
	
	public void reset() {
		before = null;
	}
}
