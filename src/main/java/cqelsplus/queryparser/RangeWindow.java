package cqelsplus.queryparser;

import java.util.Timer;
/** 
 * This class implements the time-based window 
 * @author		Danh Le Phuoc
 * @author 		Chan Le Van
 * @organization DERI Galway, NUIG, Ireland  www.deri.ie
 * @email 	danh.lephuoc@deri.org
 * @email   chan.levan@deri.org
 */
public class RangeWindow implements Window {
    long w;
    long slide;
    long wInMili;
    long sInMili;
    long lastTimestamp = -1;
    Timer timer;
    public RangeWindow( long w) {
    	this.w = w;
		timer = new Timer();    	
    }
    
    public RangeWindow(long w, long slide) {
    	this.w = w; 
    	this.slide = slide;
    	this.wInMili = (long)(this.w / 1E6);
		this.sInMili = (long)(this.slide / 1E6);
		timer = new Timer();    	
    }
	
	public RangeWindow(DurationSet durations, Duration slideDuration) {
    	this.w = durations.inNanoSec();
    	this.wInMili = (long)(this.w / 1E6);
    	if(slideDuration != null) {
    		slide = slideDuration.inNanosec();
    		this.sInMili = (long)(this.slide / 1E6);
    	}
	}

	public long getSlide() {
		return (long)(this.slide);
	}
	
	public long getDuration() {
		return (long)(this.w);
	}
	
	public void purge(long timeRange, String message) {}
	
	public synchronized void purge() {
		String message = "purge by DURATION";
		purge(this.w, message);
	}
	public void reportLatestTime(long t) {
		if(lastTimestamp < 0) {
			lastTimestamp = t;
		}
	}

	public RangeWindow clone() {
		RangeWindow w = new RangeWindow(this.getDuration(), this.getSlide());
		w.lastTimestamp = this.lastTimestamp;
		return w;
	}
}
