package cqelsplus.execplan.data;

import java.util.ArrayList;
import java.util.List;

import com.hp.hpl.jena.sparql.core.Var;
import com.hp.hpl.jena.sparql.core.VarExprList;
import com.hp.hpl.jena.sparql.expr.ExprVar;

import cqelsplus.execplan.mjoinnetwork.ProbingSequence;

public class ExtendMapping implements IMapping, PoolableObject {
	AggMapping mua;
	VarExprList exprs;
	public ExtendMapping() {}
	public void set(AggMapping mua, VarExprList exprs) {
		this.mua = mua;
		this.exprs=exprs;
	}
	
	@Override
	public long getValue(Var var) {
		long val = mua.getValue(var);
		if (val == -1) {
			if(exprs.contains(var)){
				if(exprs.getExpr(var) instanceof ExprVar){
					val = mua.getValue(((ExprVar)exprs.getExpr(var)).asVar());
				}
				else System.out.println("As with expression is not supported "+exprs.getExpr(var));
			}			
		}
		return val;
	}
	
	@Override
	public void releaseInstance() {
		mua.releaseInstance();
		POOL.MUE.returnObject(this);
	}
	@Override
	public List<Var> getVars() {
		// TODO Auto-generated method stub
		return null;
	}
	@Override
	public ArrayList<ITuple> getLeaveTuples() {
		// TODO Auto-generated method stub
		return null;
	}
	@Override
	public PoolableObject newObject() {
		// TODO Auto-generated method stub
		return null;
	}
	@Override
	public boolean contains(Cont_Dep_ExpM leaf) {
		// TODO Auto-generated method stub
		return mua.contains(leaf);
	}
	@Override
	public ProbingSequence getProbingSequence() {
		// TODO Auto-generated method stub
		return mua.getProbingSequence();
	}
	@Override
	public ITuple getDataItem() {
		// TODO Auto-generated method stub
		return mua.getDataItem();
	}
}
