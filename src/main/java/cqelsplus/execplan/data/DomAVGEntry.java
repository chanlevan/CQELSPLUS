package cqelsplus.execplan.data;


import com.hp.hpl.jena.sparql.expr.NodeValue;
import com.hp.hpl.jena.sparql.expr.nodevalue.XSDFuncOp;

import cqelsplus.engine.ExecContextFactory;

public class DomAVGEntry extends DomSumEntry {
	public DomAVGEntry() {}
		
	public long accVal() {
		NodeValue sum = NodeValue.makeNode(ExecContextFactory.current().engine().decode(super.accVal()));
		NodeValue acc = XSDFuncOp.numDivide(sum, NodeValue.makeInteger(count())); 
		return ExecContextFactory.current().engine().encode(acc.asNode());
	}
}
