package cqelsplus.queryparser;

/** 
 * This class implements the triple-based window 
 * @author		Danh Le Phuoc
 * @author 		Chan Le Van
 * @organization DERI Galway, NUIG, Ireland  www.deri.ie
 * @email 	danh.lephuoc@deri.org
 * @email   chan.levan@deri.org
 */
public class TripleWindow implements Window {
    long t;
    
    public TripleWindow( long t) {
    	this.t = t;
    }
	
    public long windowLength() {
    	return t;
    }
	public void purge() {}
	public void reportLatestTime(long t) {}
	public Window clone() {
		TripleWindow w = new TripleWindow(this.t);
		return w;
	}
	
	public long getTripleNumber() {
		return t;
	}
}
