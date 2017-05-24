package cqelsplus.logicplan;


import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;

import com.hp.hpl.jena.graph.Triple;
import com.hp.hpl.jena.sparql.algebra.Op;
import com.hp.hpl.jena.sparql.algebra.OpVars;
import com.hp.hpl.jena.sparql.algebra.op.OpDistinct;
import com.hp.hpl.jena.sparql.algebra.op.OpProject;
import com.hp.hpl.jena.sparql.algebra.op.OpQuadPattern;
import com.hp.hpl.jena.sparql.core.Quad;
import com.hp.hpl.jena.sparql.core.Var;

import cqelsplus.engine.CqelsplusExecContext;
import cqelsplus.engine.ExecContext;
import cqelsplus.engine.ExecContextFactory;

public class OpUtils {
	public static String ptCode(Quad quad) {
		ExecContext context = (CqelsplusExecContext)ExecContextFactory.current();
		String result = "";
		if((quad.getGraph() == null) || (quad.getGraph().isVariable())) {
			result += Long.toString(-1);
		} else {
			result += Long.toString(context.engine().encode(quad.getGraph()));
		}
		
		if(!quad.getSubject().isVariable()) {
			result += Long.toString(context.engine().encode(quad.getSubject()));
		} else {
			result += Long.toString(-1);
		}
		
		if(!quad.getPredicate().isVariable()) {
			result += Long.toString(context.engine().encode(quad.getPredicate()));
		} else {
			result += Long.toString(-1);
		}
		
		if(!quad.getObject().isVariable()) {
			result += Long.toString(context.engine().encode(quad.getObject()));
		} else {
			result += Long.toString(-1);
		}
		return result;
	}
	
	public static Var getOverlappedKey(Op q1, Op q2) {
		Collection<Var> c1 = OpVars.allVars(q1);
		Collection<Var> c2 = OpVars.allVars(q2);
		c1.retainAll(c2);
		if (c1.size() != 0) {
			return c1.iterator().next();
		}
		return null;
		///TODO
	}
	
	public static ArrayList<Var> getOverlappedKey(Collection<Var> c, Op q2) {
		Collection<Var> c2 = OpVars.allVars(q2);
		c2.retainAll(c);
		if (c2.size() != 0) {
			ArrayList<Var> tmp = new ArrayList<Var>();
			for (Iterator<Var> itr = c2.iterator(); itr.hasNext();
					tmp.add(itr.next()));
			return tmp;
		}
		return null;
		///TODO
	}
	
	public static int getVarIdx(Op op, Var var) {
		ArrayList<Var> tmp = parseVarsToArray(op);
		for (int i = 0; i < tmp.size(); i++){
			if (tmp.get(i).equals(var))
				return i;
		}
		return -1;
		
	}
	
	public static int getVarPos(OpQuadPattern op, Var var) {
		Quad quad = op.getPattern().get(0);
		if((quad.getGraph() != null) && quad.getGraph().isVariable() && quad.getGraph().equals(var)) {
			return 0;
		}
		if(quad.getSubject().isVariable()  && quad.getSubject().equals(var)) {
			return 1;
		}
		if(quad.getPredicate().isVariable() && quad.getPredicate().equals(var)) {
			return 2;
		}
		if(quad.getObject().isVariable() && quad.getObject().equals(var)) {
			return 3;
		}
		return -1;
		
	}
	
	
	public static ArrayList<Var> parseVarsToArray(Op op) {
		ArrayList<Var> vars = new ArrayList<Var>();
		if (op instanceof OpQuadPattern) {
			Quad quad = ((OpQuadPattern)op).getPattern().get(0);
			if((quad.getGraph() != null) && quad.getGraph().isVariable()) {
				vars.add((Var)quad.getGraph());
			}
			if(quad.getSubject().isVariable()) {
				vars.add((Var)quad.getSubject());
			}
			if(quad.getPredicate().isVariable()) {
				vars.add((Var)quad.getPredicate());
			}
			if(quad.getObject().isVariable()) {
				vars.add((Var)quad.getObject());
			}
		} else {
			//TODO with sub query operator
			if (op instanceof OpProject) {
				vars = (ArrayList<Var>)((OpProject)op).getVars();
			} else if (op instanceof OpDistinct) {
				OpProject opp = (OpProject)((OpDistinct)op).getSubOp();
				vars = (ArrayList<Var>)opp.getVars();
			}
		}
		return vars;
	}
	
	public static boolean isTheSameOp(OpQuadPattern op1, OpQuadPattern op2) {
		if (op1.getGraphNode() == null) {
			if (op2.getGraphNode() != null)
				return false;
		} else {
			if (op2.getGraphNode() == null)
				return false;
		} 
		
		Triple t1 = op1.getBasicPattern().get(0);
		Triple t2 = op2.getBasicPattern().get(0);
		return t1.equals(t2);
	}
}
