package cqelsplus.execplan.mjoinnetwork;

import java.util.ArrayList;
import java.util.BitSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;

import org.apache.log4j.Logger;

import com.hp.hpl.jena.sparql.core.Var;

import cqelsplus.execplan.oprouters.MJoinRouter;
import cqelsplus.execplan.utils.OpUtils;
import cqelsplus.execplan.windows.PhysicalWindow;
import cqelsplus.execplan.windows.VirtualWindow;


public class Vertex {
	final static Logger logger = Logger.getLogger(Vertex.class);
	PhysicalWindow w;
	static int count = 0;
	int id;
	List<AccVars> accVarsList;//variables accumulated
	List<Vertex> children;//list of childrend
	Vertex parent;
	OverlappedValue connectedOV;
	HashMap<Integer, ArrayList<Integer>> idMap;
	HashSet<MJoinRouter> satisfiedQueries;
	ArrayList<ProbingInfo> probingInfo;
	BitSet idBitSet;
	int sequenceId;
	/**
	 * Each vertex contains the information of the physical window it represents
	 * @param w
	 */
	public Vertex(PhysicalWindow w) {
		this.w = w;
		id = count++;
		accVarsList = new ArrayList<AccVars>();
		children = new ArrayList<Vertex>();
		satisfiedQueries =  new HashSet<MJoinRouter>();
		parent = null;
		connectedOV = null;
		probingInfo = new ArrayList<ProbingInfo>();
		idBitSet = new BitSet();
	}	
	
	public Vertex() {
		w = null;
		id = count++;
		children = new ArrayList<Vertex>();
		parent = null;
		connectedOV = null;
		probingInfo = new ArrayList<ProbingInfo>();
		idBitSet = new BitSet();
	}
	/**
	 * Probing information: Physical window Id and the column will be probed
	 * */
	public void initProbedInfo() {
		ArrayList<Var> vars = OpUtils.parseVarsToArray(w.getVWs().get(0).getOp());
		for (int i = 0; i  < vars.size(); i++) {
			ProbingInfo pi = new ProbingInfo(w.getId(), i);
			probingInfo.add(pi);
			idBitSet.set(i);
		}
	}
	
	@SuppressWarnings("unchecked")
	public ArrayList<ProbingInfo> cloneProbingInfo() {
		return (ArrayList<ProbingInfo>)this.probingInfo.clone();
	}
	
	public ArrayList<ProbingInfo> getProbingInfo() {
		return probingInfo;
	}
	
	public void setProbingInfo(ArrayList<ProbingInfo> pis) {
		this.probingInfo = pis; 
		for (int i = 0; i < probingInfo.size(); i++) {
			idBitSet.set(i);
		}
	}
	
	public BitSet getIdxInfo() {
		return idBitSet;
	}
	
	
	public int getId() {
		return id;
	}

	public PhysicalWindow getPW() {
		return w;
	}
	
	public OverlappedValue getConnectedInfo() {
		return this.connectedOV;
	}
	/**
	 * Arrived edge = parent vertex + corresponding overlapped value
	 */
	public void setArrivedEdge(Vertex parent, OverlappedValue connectedOV) {
		this.parent = parent;
		this.connectedOV = connectedOV;
	}
	
	public void addChild(Vertex child) {
		this.children.add(child);
	}
	
	public List<Vertex> getChildren() {
		return this.children;
	}
	
	public Vertex getLastChild() {
		return children.get(children.size() - 1);
	}
	
	public void addSatisfiedMJoinRouter(MJoinRouter q) {
		satisfiedQueries.add(q);
	}
	
	public HashSet<MJoinRouter> getSatisfiedQueries() {
		return satisfiedQueries;
	}
	
	public void setAccVarsList(List<AccVars> l) {
		this.accVarsList = l;
	}	
	
	public void addAccVarsList(List<AccVars> list) {
		for (AccVars acc : list) {
			this.accVarsList.add(acc);
		}
	}
	public List<AccVars> getAccVarsList() {
		return accVarsList;
	} 
	
	public void sortAscendingProbingIndexes() {
		if (connectedOV != null)
		connectedOV.sortIndexPos();
		for (Vertex v : children) {
			v.sortAscendingProbingIndexes();
		}
	}
	
	public boolean contains(VirtualWindow v) {
		for (AccVars accVar : accVarsList) {
			if ((accVar.contains(v))) {
				return true;
			}
		}
		return false;
	}
	
	public void setSequenceId(int value) {
		this.sequenceId = value;
	}
	
	public int getSequenceId() {
		return this.sequenceId;
	}
	
	public void printLog(String deep) {
		System.out.println(deep + "VertexId: " + id);
		for (VirtualWindow v : w.getVWs()) {
			//TODO with normalization
			//if (v.getRegNo() != regNo) continue; 
			v.printLog(deep);
		}
		System.out.println(deep + "Physical window id: " + w.getId());
		if (parent != null) {
			System.out.println(deep + "Parent Id: " + parent.getId());
			connectedOV.printLog(deep);
		}
		System.out.print("Satisfied queries: ");
		for (MJoinRouter q : satisfiedQueries) {
			System.out.print("id " + q.getId() + " ");
		}
		System.out.println();
		if (probingInfo != null) {
			System.out.println("Probing info: ");
			for (int i = 0; i < this.probingInfo.size(); i++) {
				System.out.println(i + ": Pid: " + probingInfo.get(i).getProbedPid() + ", col: " + probingInfo.get(i).getProbedCol());
			}
			System.out.print("Id: ");
			for (int i = idBitSet.nextSetBit(0); i >= 0; i = idBitSet.nextSetBit(i + 1)) {
				System.out.print(i + ", ");
			}
			System.out.println();
		}
		System.out.println("---------------------------------------------------------------------------------------------");

		deep += "----- ";
		for (Vertex v : children) {
			//v.printLog(deep, regNo);
			v.printLog(deep);
		}
	}

	List<Integer> indexesInfo;
	public void setIndexesInfo(List<Integer> indexesInfo) {
		this.indexesInfo = indexesInfo;
	}
	
	public List<Integer> getIndexesInfo() {
		return indexesInfo;
	}
}
