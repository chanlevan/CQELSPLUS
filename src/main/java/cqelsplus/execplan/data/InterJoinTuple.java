package cqelsplus.execplan.data;

import java.util.ArrayList;

import com.hp.hpl.jena.sparql.core.Var;

import cqelsplus.execplan.mjoinnetwork.ProbingInfo;
import cqelsplus.execplan.mjoinnetwork.ProbingSequence;
import cqelsplus.execplan.mjoinnetwork.Vertex;
import cqelsplus.execplan.windows.PhysicalWindow;

public  class InterJoinTuple extends BaseTuple {
    BaseTuple left,right;
    Vertex vLeft, vRight;
   
    public InterJoinTuple(){}
    
    public PhysicalWindow getFrom() {
    	return left.getFrom();
    }
    
    public void setBranches(BaseTuple left,BaseTuple right) {
    	this.left=left;
    	this.right=right;
    }
    
    public ITuple getLeftBranch() {
    	return left;
    }
    
    public ITuple getRightBranch() {
    	return right;
    }
    
    public void setVertexes(Vertex left, Vertex right) {
    	vLeft = left;
    	vRight = right;
    }
    
    public Vertex getLeftVertex() {
    	return vLeft;
    }
    
    public Vertex getRightVertex() {
    	return vRight;
    }
    
	public long get(int idx) {
		if (vLeft.getIdxInfo().get(idx)) { 
			return left.get(idx); 
		}
		else {
			ArrayList<ProbingInfo> pis = vRight.getProbingInfo(); 
			if (idx >= pis.size()) {
				System.out.println("Cant retrieve value with idx: " + idx);
			}
			int colIdx = pis.get(idx).getProbedCol();
			return right.get(colIdx);
		}
	}

	public long[] vals() {
		///TODO
		return null;
		
	}

	public PoolableObject newObject() {
		// TODO Auto-generated method stub
		return (PoolableObject)POOL.MUJ.borrowObject();
	}
	//final MUJ node will definately have the var information
	//so convert it into the index information and process
	//in the future, modify this messy implementation by doing var<->id outside the MUJ
	@Override
	public long getByVar(Var var, ProbingSequence ps) {
		return ps.get(var, this);
	}
	
	public boolean isExpired(ProbingSequence ps) {
		return ps.isExpired(this);
	}

	@Override
	public void releaseInstance() {
		// TODO Auto-generated method stub
		
	}
}
