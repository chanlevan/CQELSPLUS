package cqelsplus.logicplan.algerba;

import java.util.List;

import com.hp.hpl.jena.sparql.algebra.Op;
import com.hp.hpl.jena.sparql.algebra.OpVisitor;
import com.hp.hpl.jena.sparql.algebra.Transform;
import com.hp.hpl.jena.sparql.algebra.op.OpJoin;
import com.hp.hpl.jena.sparql.algebra.op.OpN;
import com.hp.hpl.jena.sparql.util.NodeIsomorphismMap;

import cqelsplus.execplan.oprouters.OpRouterVisitor;

public class OpMJoin extends OpN {
	static int count = 0;
	int id;
	public OpMJoin(List<Op> elts) {
		super(elts);
		id = count++;
	}
	
	public OpMJoin(OpJoin joinOp){
		addSubOp(joinOp.getLeft());
		addSubOp(joinOp.getRight());
	}
	
	private void addSubOp(Op op){
		
		if(op instanceof OpJoin) {
			addSubOp(((OpJoin)op).getLeft());
			addSubOp(((OpJoin)op).getRight());
			
		}
		else add(op);
	}
	
	public void visit(OpRouterVisitor routerVisitor) {
		routerVisitor.visit(this);
	}

	@Override
	public String getName() {
		return "MJoin";
	}

	@Override
	public Op apply(Transform transform, List<Op> elts) {
		 return ((TransformCopy4MJoin)transform).transform(this, elts) ;
	}

	@Override
	public Op copy(List<Op> elts) {
		// TODO Auto-generated method stub
		 return new OpMJoin(elts) ; 
	}

	@Override
	public boolean equalTo(Op other, NodeIsomorphismMap labelMap) {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public void visit(OpVisitor opVisitor) {
		this.visit((OpRouterVisitor)opVisitor);
		
	}
	
    @Override
    public int hashCode() {
    	return id; 
    }
    
    public boolean containsOpStream(OpStream op) {
    	List<Op> elts = super.getElements();
    	for (Op elm : elts) { 
    		if (elm instanceof OpStream)
    		if (((OpStream) elm).getSpecialId() == op.getSpecialId()) {
    			return true;
    		}
    	}
    	return false;
    }
}
