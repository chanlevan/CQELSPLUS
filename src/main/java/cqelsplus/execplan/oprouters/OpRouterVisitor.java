package cqelsplus.execplan.oprouters;

import java.util.ArrayDeque;
import java.util.Deque;
import java.util.HashMap;
import java.util.List;

import com.hp.hpl.jena.sparql.algebra.OpVisitorByType;
import com.hp.hpl.jena.sparql.algebra.op.Op0;
import com.hp.hpl.jena.sparql.algebra.op.Op1;
import com.hp.hpl.jena.sparql.algebra.op.Op2;
import com.hp.hpl.jena.sparql.algebra.op.OpDistinct;
import com.hp.hpl.jena.sparql.algebra.op.OpExt;
import com.hp.hpl.jena.sparql.algebra.op.OpExtend;
import com.hp.hpl.jena.sparql.algebra.op.OpFilter;
import com.hp.hpl.jena.sparql.algebra.op.OpGroup;
import com.hp.hpl.jena.sparql.algebra.op.OpLeftJoin;
import com.hp.hpl.jena.sparql.algebra.op.OpN;
import com.hp.hpl.jena.sparql.algebra.op.OpProject;
import com.hp.hpl.jena.sparql.algebra.op.OpService;
import com.hp.hpl.jena.sparql.algebra.op.OpUnion;

import cqelsplus.engine.ExecContext;
import cqelsplus.logicplan.algerba.OpMJoin;
import cqelsplus.logicplan.algerba.OpStream;

public class OpRouterVisitor extends OpVisitorByType {
    
	private Deque<OpRouter> stack = new ArrayDeque<OpRouter>() ;
    
	private List<MJoinRouter> mjoinRouters;
	private HashMap<OpMJoin, Integer> op2IdMap;
	ExecContext context;
	
	public OpRouterVisitor(List<MJoinRouter> mjoinRouters, HashMap<OpMJoin, Integer> op2IdMap) {
    	this.mjoinRouters = mjoinRouters;
    	this.op2IdMap = op2IdMap;
    }

	public QueryRouter getQueryRouter() {
    	QueryRouter qr = new QueryRouter();
    	OpRouter subRouter = stack.pop();
    	qr.addSubRouter(subRouter);
    	subRouter.addNextRouter(qr);
    	return qr;
    	
    }

	@Override
    public void visit(OpFilter opFilter) {//TODO
		if (opFilter.getSubOp() != null) {
			opFilter.getSubOp().visit(this);
		}
		
		FilterExprRouter router = new FilterExprRouter(opFilter);
		
		if (opFilter.getSubOp() != null) {
			OpRouter subRouter = stack.pop();
			router.addSubRouter(subRouter);
			((OpRouterBase)subRouter).addNextRouter(router);
		}
		stack.push(router);
    }

    @Override
    public void visit(OpGroup opGroup) {//TODO
    	if (opGroup.getSubOp() != null) {
    		opGroup.getSubOp().visit(this);
    	}
    	AggRouter router = new AggRouter(opGroup);

		if (opGroup.getSubOp() != null) {
			OpRouter subRouter = stack.pop();
			router.addSubRouter(subRouter);
			((OpRouterBase)subRouter).addNextRouter(router);
		}
		stack.push(router);
    }
    
    @Override
    public void visit(OpExtend opExtend) { //TODO
    	if (opExtend.getSubOp() != null) {
    		opExtend.getSubOp().visit(this);
    	}
    	ExtendRouter router = new ExtendRouter(opExtend);
		
		if (opExtend.getSubOp() != null) {
			OpRouter subRouter = stack.pop();
			router.addSubRouter(subRouter);
			((OpRouterBase)subRouter).addNextRouter(router);
		}
		stack.push(router);
    }
        
    @Override
    public void visit(OpProject opProject) {
    	if (opProject.getSubOp() != null) {
    		opProject.getSubOp().visit(this);
    	}
    	ProjectRouter router = new ProjectRouter(opProject);
		
		if (opProject.getSubOp() != null) {
			OpRouter subRouter = stack.pop();
			router.addSubRouter(subRouter);
			((OpRouterBase)subRouter).addNextRouter(router);
		}
		stack.push(router);    	
    }
    
    @Override
    public void visit(OpDistinct opDistinct) {
    	if (opDistinct.getSubOp() != null) {
    		opDistinct.getSubOp().visit(this);
    	}
    	DistinctRouter router = new DistinctRouter(opDistinct);
		
		if (opDistinct.getSubOp() != null) {
			OpRouter subRouter = stack.pop();
			router.addSubRouter(subRouter);
			((OpRouterBase)subRouter).addNextRouter(router);
		}
		stack.push(router);    	
	}

    public void visit(OpMJoin opMJoin) {
    	stack.push(mjoinRouters.get(op2IdMap.get(opMJoin)));
    }
//TODO
    @Override
    public void visit(OpUnion opUnion)
    { visit2(opUnion) ; }
    @Override
    public void visit(OpLeftJoin opLeftJoin)
    { visit2(opLeftJoin) ; }
    @Override
    public void visit(OpService opService)
    { visit1(opService) ; }


    @Override
    protected void visit0(Op0 op) {
    	if (op instanceof OpStream && ((OpStream)op).isSpecial()) {
    		//System.out.println("Special query !!!");
    		for (MJoinRouter mjRouter : mjoinRouters) {
    			OpMJoin curOpMJoin = (OpMJoin)mjRouter.getOp();
    			if (curOpMJoin.containsOpStream((OpStream)op)) {
    				stack.push(mjRouter);
    			}
    		}
    	}
    }
    
    @Override
    protected void visit1(Op1 op) {
    }

    @Override
    protected void visit2(Op2 op) { 
    }
    
    @Override
    protected void visitN(OpN op) {
    }
    
    @Override
    protected void visitExt(OpExt op) {
    }
}