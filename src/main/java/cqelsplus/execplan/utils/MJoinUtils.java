package cqelsplus.execplan.utils;

import java.util.ArrayList;
import java.util.List;

import com.hp.hpl.jena.sparql.algebra.Op;
import com.hp.hpl.jena.sparql.algebra.op.Op1;
import com.hp.hpl.jena.sparql.algebra.op.Op2;
import com.hp.hpl.jena.sparql.algebra.op.OpN;
import com.hp.hpl.jena.sparql.algebra.op.OpQuadPattern;

import cqelsplus.logicplan.algerba.OpMJoin;
import cqelsplus.logicplan.algerba.OpStream;

public class MJoinUtils {

	public static List<OpMJoin> searchOpMJoin(List<Op> queryOps) {
		List<OpMJoin> opMJoinList = new ArrayList<OpMJoin>();
		for (Op queryOp : queryOps) {
			List<OpMJoin> curQueryMjoinList = new ArrayList<OpMJoin>();
			//searchOpMJoin(opMJoinList, queryOp);
			searchOpMJoin(curQueryMjoinList, queryOp);
			if (!curQueryMjoinList.isEmpty()) {
				opMJoinList.addAll(curQueryMjoinList);
			} else {
				//this special case...
				buildSpecialOpMJoin(curQueryMjoinList, queryOp, null);
				//there will be at least 1 mjoin after above step for sure...
				opMJoinList.addAll(curQueryMjoinList);
			}
		}
		return opMJoinList;
	}
	
	public static void buildSpecialOpMJoin(List<OpMJoin> curQueryMjoinList, Op op, Op parent) {
		if (op instanceof OpMJoin) {			
			curQueryMjoinList.add((OpMJoin)op);
		} else {
			if (op instanceof OpStream) {
				//How about op is a static op ?
				List<Op> ops = new ArrayList<Op>();
				ops.add(op);
				((OpStream)op).setSpecial();
				OpMJoin opMJoin = new OpMJoin(ops);
				curQueryMjoinList.add(opMJoin);
			}
			if (op instanceof Op1) {
				buildSpecialOpMJoin(curQueryMjoinList, ((Op1)op).getSubOp(), op);
				
			} else if (op instanceof Op2) {
				buildSpecialOpMJoin(curQueryMjoinList, ((Op2)op).getLeft(), op);
				buildSpecialOpMJoin(curQueryMjoinList, ((Op2)op).getRight(), op);
			
			} else if (op instanceof OpN) {
				for (int i = 0; i < ((OpN)op).size(); i++) {
					buildSpecialOpMJoin(curQueryMjoinList, ((OpN)op).get(i), op);
				}
			}
		}
	}

	public static void searchOpMJoin(List<OpMJoin> curQueryMjoinList, Op op) {
		if (op instanceof OpMJoin) {
			curQueryMjoinList.add((OpMJoin)op);
			//op2IdMap.put((OpMJoin)op, opMJoinList.size() - 1);
			
			for (int i = 0; i < ((OpMJoin)op).size(); i++) {
				searchOpMJoin(curQueryMjoinList, ((OpN)op).get(i));
			}
		} else {
			if (op instanceof Op1) {
				searchOpMJoin(curQueryMjoinList, ((Op1)op).getSubOp());
				
			} else if (op instanceof Op2) {
				searchOpMJoin(curQueryMjoinList, ((Op2)op).getLeft());
				searchOpMJoin(curQueryMjoinList, ((Op2)op).getRight());
			
			} else if (op instanceof OpN) {
				for (int i = 0; i < ((OpN)op).size(); i++) {
					searchOpMJoin(curQueryMjoinList, ((OpN)op).get(i));
				}
			}
		}
	}
	
	public static void separateMJoinElements(OpMJoin opMJoin, List<OpStream> streamOps, 
			List<OpQuadPattern> staticOps, List<Op> others) {
		for (Op op : opMJoin.getElements()) {
			if (op.getClass().equals(OpStream.class)) {
				streamOps.add((OpStream)op);
				continue;
			}
			if (op.getClass().equals(OpQuadPattern.class)) {
				staticOps.add((OpQuadPattern)op);
				continue;
			}
			others.add(op);
		}
	}
	
}
