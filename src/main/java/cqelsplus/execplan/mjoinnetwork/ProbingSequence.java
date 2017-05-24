package cqelsplus.execplan.mjoinnetwork;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.NoSuchElementException;

import com.hp.hpl.jena.sparql.core.Var;

import cqelsplus.execplan.data.ITuple;
import cqelsplus.execplan.data.InterJoinTuple;
import cqelsplus.execplan.data.LeafTuple;
import cqelsplus.execplan.data.POOL;
import cqelsplus.execplan.utils.OpUtils;
import cqelsplus.execplan.windows.VirtualWindow;
/**This class is used in combination with a specific query. 
 * After the multiple shared join operation. The results are routed
 * into all specific queries that connect with the join. To identify 
 * the join results binding with join variable of queries, we need a mechanism.
 * This class is on that mechanism*/
public class ProbingSequence {
	List<VirtualWindow> vWs;
	List<Var> allVars;
	HashMap<Var, VirtualWindow> varAndVwMap;
	HashMap<VirtualWindow, Integer> idxVwMap;
	int id;
	private static int count = 0;
	public ProbingSequence(List<VirtualWindow> vWs, List<Var> allVars) {
		id = count++;
		this.vWs = vWs;
		this.allVars = allVars;
		this.varAndVwMap = new HashMap<Var, VirtualWindow>();
		this.idxVwMap = new HashMap<VirtualWindow, Integer>();
		init();
	}
	
	public int getNoOVw() {
		return this.vWs.size();
	}
	
	private void init() {
		//create the map between virtual window and its index for fast retrieval
		for (int i = 0; i < vWs.size(); i++) {
			idxVwMap.put(vWs.get(i), i);
		}
		//create the map between varible and one of virtual window containing it

		for (Var var : allVars) {
			for (int i = 0; i < vWs.size(); i++) {
				VirtualWindow vw = vWs.get(i);
				int varIdx = OpUtils.getVarIdx(vw.getOp(), var);
				if (varIdx != -1) {
					varAndVwMap.put(var, vw);
					break;
				}
			}
		}
	}
	
	public List<Var> getVars() {
		return this.allVars;
	}
	
	private ArrayList<ITuple> getLeafList(InterJoinTuple mapping) {
		//build the list to perform lookup mapping id
		ArrayList<ITuple> leaves = (ArrayList<ITuple>)POOL.MuList.borrowObject();
		ITuple root = mapping;
		while (root instanceof InterJoinTuple) {
			leaves.add(0, ((InterJoinTuple)root).getRightBranch());
			root = ((InterJoinTuple)root).getLeftBranch();
		}
		//return the left-most leaf of the mapping tree
		leaves.add(0, root);
		return leaves;
	}
	
	public ArrayList<ITuple> getLeafList(ITuple mapping) {
		if (mapping instanceof InterJoinTuple) {
			return getLeafList((InterJoinTuple)mapping);
		} else if (mapping instanceof LeafTuple) { 
			ArrayList<ITuple> leaves = (ArrayList<ITuple>)POOL.MuList.borrowObject();
			leaves.add(mapping);
			return leaves;
		}
		throw new ClassCastException("unidentified MU FORMAT");
	}

	public long get(Var var, InterJoinTuple mapping) {
		//start looking up the id of mapping should contain that var 
		ArrayList<ITuple> leaves = getLeafList(mapping);
		VirtualWindow vw = varAndVwMap.get(var);
		if (vw == null) {
			return -1;
		}
		int vWidx = idxVwMap.get(vw);
		
		LeafTuple leaf = (LeafTuple)leaves.get(vWidx);
		int varIdx = OpUtils.getVarIdx(vWs.get(vWidx).getOp(), var);
		if (varIdx == -1) {
			throw new NoSuchElementException("Wrong retrieve");
		}
		//TODO please return this to POOL
//		leaves.clear();
//		POOL.MuList.returnObject(leaves);
		
		return get(varIdx, leaf);
	}
	
	public long get(Var var, LeafTuple mapping) {
		VirtualWindow vw = varAndVwMap.get(var); 
		int varIdx = OpUtils.getVarIdx(vw.getOp(), var);
		return get(varIdx, mapping);
	}
	
	public long get(Var var, ITuple mapping) {
		if (mapping instanceof InterJoinTuple) {
			return this.get(var, (InterJoinTuple)mapping);
		} else if (mapping instanceof LeafTuple) {
			return this.get(var, (LeafTuple)mapping);
		}
		return -1;
	}
	
	
	private long get(int idx, LeafTuple mapping) {
		return mapping.get(idx);
	}
	
	public boolean isExpired(InterJoinTuple mapping) {
		ArrayList<ITuple> leaves = getLeafList(mapping);
		for (int i = 0; i < vWs.size(); i++) {
			try {
				VirtualWindow vw = vWs.get(i);
				LeafTuple leaf = (LeafTuple)leaves.get(i);
				if (vw.isExpired(leaf)) {
					//leaves.clear();
					//POOL.MuList.returnObject(leaves);
					return true;
				}
			}
			catch (Exception e) {
				e.printStackTrace();
			}
		}
		//TODO please return this to POOL
		//leaves.clear();
		//POOL.MuList.returnObject(leaves);
		return false;
	}
	
	
	public boolean isExpired(LeafTuple mapping) {
		/**This is the special case when the query has just 1 single basic pattern*/
		VirtualWindow vw = mapping.getFrom().getVWs().get(0);
		if (vw.isExpired(mapping)) {
			return true;
		}
		return false;
	}
	
	public boolean checkBelonging(ITuple candiateLeaf, ITuple father, VirtualWindow vw) {
		boolean result = false;
		if (father instanceof InterJoinTuple) {
			ArrayList<ITuple> leaves = getLeafList((InterJoinTuple)father);
			int vWPos = idxVwMap.get(vw);
			LeafTuple leaf = (LeafTuple)leaves.get(vWPos);
			if (((LeafTuple)leaf).timestamp == ((LeafTuple)(candiateLeaf)).timestamp) {
				result = true;
			} else {
				result = false;
			}
			//leaves.clear();
			//POOL.MuList.returnObject(leaves);
		} else {
			if (father instanceof LeafTuple) {
				if (((LeafTuple)father).timestamp == ((LeafTuple)(candiateLeaf)).timestamp) {
					result = true;
				} else {
					result = false;
				}
			}
		}
		return result;
		
	}
	
	public int getVWPosition(VirtualWindow vw) {
		return idxVwMap.get(vw);
	}
	
	public int hashCode() {
		return id;
	}
	
	@Override 
	public boolean equals(Object val) {
		return this.hashCode() == ((ProbingSequence)val).hashCode();
	}
	
	
}
