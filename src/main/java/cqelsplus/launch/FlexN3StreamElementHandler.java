package cqelsplus.launch;

import com.hp.hpl.jena.graph.Triple;

import cqelsplus.engine.RDFStream;

public class FlexN3StreamElementHandler extends N3StreamElementHandler {
	long throughput = 0;
	
	public FlexN3StreamElementHandler(long throughput, long streamSize, long startCount, RDFStream stream){
		super(streamSize, startCount, stream);
		this.throughput = throughput;
	}
		
	public void triple(int line, int col, Triple triple) {
		stream.stream(triple);
		count++; 
		streamedTriple++;
		if(count==(throughput/10)){
			
			long sleep=100-(System.currentTimeMillis()-start);
			if(sleep>0){
				try {
					//System.out.println("sleep "+sleep);
					Thread.sleep(sleep);
				} catch (InterruptedException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
			count=0;
			start=System.currentTimeMillis();
		}
	}
}
