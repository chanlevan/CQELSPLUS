package cqelsplus.execplan.mjoinnetwork;

import java.util.ArrayList;

import org.apache.log4j.Logger;

import com.hp.hpl.jena.sparql.core.Var;

import cqelsplus.execplan.oprouters.MJoinRouter;
import cqelsplus.execplan.utils.OpUtils;
import cqelsplus.execplan.windows.VirtualWindow;

public class AccVars {
	final static Logger logger = Logger.getLogger(AccVars.class);
	ArrayList<Var> accVars;
	ArrayList<VirtualWindow> visitedVWs;
	MJoinRouter visitedMJoinRouter;
	public AccVars(ArrayList<Var> accVars) {
		this.accVars = accVars;
		visitedVWs = new ArrayList<VirtualWindow>();
	}
	
	public AccVars(){
		accVars = new ArrayList<Var>();
		visitedVWs = new ArrayList<VirtualWindow>();
	}
	
	public void addVisitedVWs(VirtualWindow vw) {
		visitedVWs.add(vw);
	}
	
	public void setVisitedMJoinRouter(MJoinRouter q) {
		this.visitedMJoinRouter = q;
	}
	
	public void setVisitedVWs(ArrayList<VirtualWindow> vWs) {
		this.visitedVWs = vWs;
	}
	
	public MJoinRouter getVisitedMJoinRouter() {
		return this.visitedMJoinRouter;
	}
	public boolean visited(VirtualWindow v) {
		return visitedVWs.contains(v);
	}
	
	public void addVar(Var var) {
		if (!accVars.contains(var)) {
			accVars.add(var);
		}
	}
	
	public ArrayList<Var> getVars() {
		return this.accVars;
	}
	
	public void addVars(ArrayList<Var> vars) {
		for (int i = 0; i < vars.size(); i ++) {
			if (!accVars.contains(vars.get(i))) {
				accVars.add(vars.get(i));
			}
		}
	}
	
	public ArrayList<ProbingInfo> addVars(VirtualWindow vW, Vertex prevVertex) {
		ArrayList<Var> vars = OpUtils.parseVarsToArray(vW.getOp());
		ArrayList<ProbingInfo> pis = prevVertex.cloneProbingInfo();
		for (int i = 0; i < vars.size(); i++) {
			if (!accVars.contains(vars.get(i))) {
				accVars.add(vars.get(i));
				ProbingInfo pi = new ProbingInfo(vW.getPW().getId(), i);
				pis.add(pi);
			}
		}
		return pis;
	}
	
	public ArrayList<Var> getValue() {
		return accVars;
	}
	public int getVarIdx(Var var) {
		for (int i = 0; i < accVars.size(); i ++) {
			if (accVars.get(i).equals(var))
				return i;
		}
		return -1;
	}
	
	public boolean isSatifiedMJoinRouter() {
		return (visitedMJoinRouter.getVWs().size() == visitedVWs.size());
	}
	
	public AccVars clone() {
		ArrayList<Var> tmp = new ArrayList<Var>();
		for (int i = 0; i < accVars.size(); i ++) {
			tmp.add(accVars.get(i));
		}
		ArrayList<VirtualWindow> set = new ArrayList<VirtualWindow>();
		for (VirtualWindow v : visitedVWs) {
			set.add(v);
		}
		AccVars result = new AccVars(tmp);
		result.setVisitedVWs(set);
		result.setVisitedMJoinRouter(this.visitedMJoinRouter);
		return result;
	}
	
	public boolean contains(VirtualWindow v) {
		return this.visitedVWs.contains(v);
	}
	
	public VirtualWindow getLastJoinedVW() {
		if (!visitedVWs.isEmpty()) {
			return visitedVWs.get(visitedVWs.size() - 1);
		}
		return null;
	}
}
