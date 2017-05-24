package cqelsplus.execplan.data;


import com.hp.hpl.jena.sparql.expr.NodeValue;
import com.hp.hpl.jena.sparql.expr.nodevalue.XSDFuncOp;

import cqelsplus.engine.ExecContextFactory;

public class DomSumEntry extends DomAggEntry{
	//TODO with QueryContext
	NodeValue sum;
	public DomSumEntry() {
		sum = NodeValue.nvZERO;
	}
	public void update(IMapping mu){
		//TODO with query context
		//sum += mu.getValue(acc.accVar());
		NodeValue muVal = NodeValue.makeNode(ExecContextFactory.current().engine().decode(mu.getValue(acc.accVar())));
		NodeValue acc = XSDFuncOp.numAdd(muVal, sum);
		sum = NodeValue.makeNode(acc.asNode());
	}
	
	public void expire(IMapping mu) {
		NodeValue muVal = NodeValue.makeNode(ExecContextFactory.current().engine().decode(mu.getValue(acc.accVar())));
		NodeValue acc = XSDFuncOp.numSubtract(sum, muVal);
		sum = NodeValue.makeNode(acc.asNode());
	}
	
	@Override
	public void reset(IMapping mu) {
		sum=NodeValue.makeNode(ExecContextFactory.current().engine().decode(mu.getValue(acc.accVar())));
		reset();
	}
	public long accVal() {
		// TODO Auto-generated method stub
		return ExecContextFactory.current().engine().encode(sum.getNode());
	}
	
	public void updateUTest(NodeValue val) {
		NodeValue acc = XSDFuncOp.numAdd(val, sum);
		sum = NodeValue.makeNode(acc.asNode());		
	}
	
	public void expireUTest(NodeValue val) {
		NodeValue acc = XSDFuncOp.numSubtract(sum, val);
		sum = NodeValue.makeNode(acc.asNode());		
	}
	
	public NodeValue getSumAsNodeValue() {
		return sum;
	}

}
