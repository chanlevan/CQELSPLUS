package cqelsplus.launch;


import com.hp.hpl.jena.sparql.core.Var;

import cqelsplus.engine.ExecContext;
import cqelsplus.engine.SelectListener;
import cqelsplus.execplan.data.IMapping;
import cqelsplus.execplan.oprouters.QueryRouter;

public class SelectOutputListener extends SelectListener{
	public static int count = 0;
	ExecContext context;
	QueryRouter qr;
	public SelectOutputListener(ExecContext context, QueryRouter qr) {
		this.context = context;
		this.qr = qr;
	}
		
	public SelectOutputListener(ExecContext context) {
		this.context = context;
	}
	
	public void update(IMapping mapping) {
		String result = "";
		//out.print("+ q id: " + qId + ": " );
		for (Var var : mapping.getVars()) {
			long value = mapping.getValue(var);
			if (value > 0) {
				result += context.engine().decode(value) + " ";
				//out.print(context.engine().decode(value) + " ");
			} else {
				result += Long.toString(-value) + " ";
				//out.print(-value + " ");
			}
		}
		System.out.println("query id:" + qr.getId()+ "results: " + result);
		//out.println(result);
		//out.flush();
		count++;
	}
}


