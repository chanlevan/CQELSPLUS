package cqelsplus.execplan.windows;

import java.util.ArrayList;
import java.util.List;
import java.util.Timer;
import java.util.TimerTask;

import org.apache.log4j.Logger;

import cqelsplus.execplan.data.Cont_Dep_ExpM;
import cqelsplus.execplan.data.LeafTuple;
import cqelsplus.execplan.data.POOL;
import cqelsplus.logicplan.algerba.OpStream;
import cqelsplus.queryparser.RangeWindow;


public class TimeVirtualWindow extends VirtualWindow {
	static Logger logger = Logger.getLogger(TimeVirtualWindow.class);
	long duration; 
	long slide;
	Timer slider;
	PeriodicSlider ps;
	public TimeVirtualWindow(PhysicalWindow pw, OpStream op) {
		super(pw, op);
	}
	
	public TimeVirtualWindow() {
		super();
	}
	
	/**for unit test*/
	public void setDuration(long duration) {
		this.duration = duration;
	}
	
	/**for unit test*/
	public void setSlide(long slide) {
		this.slide = slide;
	}
	
	/**Need to separate this for unit test as well*/
	private void setupSlider(long duration, long slide) {
		if (slide > 0) {
			if (slide > duration) {
				logger.error("Slide parameter can not be higher than Duration");
			}
			ps = new PeriodicSlider(this);
			slider = new Timer();
			long delay = 0;
			long slideInMs = (long)(slide/1E6);
			slider.schedule(ps, delay, slideInMs);
		}
	}
	
	public void init() {
		duration = ((RangeWindow)(((OpStream)op).getWindow())).getDuration();
		slide = ((RangeWindow)(((OpStream)op).getWindow())).getSlide();
		setupSlider(duration, slide);
	}
	

	public long getDuration() {
		return duration;
	}
	
	public long getSlide() {
		return slide;
	}
	
	public List<Cont_Dep_ExpM> insert(LeafTuple diLeaf) {
		List<Cont_Dep_ExpM> expiredBuff = (ArrayList<Cont_Dep_ExpM>)POOL.CONT_DEP_EXPMU_LIST.borrowObject();
		newest = diLeaf;
		if (oldest == null)  {
			oldest = newest;
		} else {
			if (slide > 0) {
				return new ArrayList<Cont_Dep_ExpM>();
			}
			long curTime = newest.timestamp;
			expiredBuff = purge(curTime);
		}
		//pW.updateOldestInvalidItem(oldest);
		pW.updateExpTupleFromVW(oldest);
		return expiredBuff;
	}
	
	
	private List<Cont_Dep_ExpM> purge(long curTime) {
		List<Cont_Dep_ExpM> expiredBuff = new ArrayList<Cont_Dep_ExpM>();
		while ((oldest != null) && oldest.timestamp < (curTime - duration)) {
			Cont_Dep_ExpM eMU = (Cont_Dep_ExpM)POOL.CONT_DEP_EXPMU.borrowObject();
			eMU.setMUN(oldest);
			eMU.setVW(this);
			expiredBuff.add(eMU);
			oldest = oldest.nextNewer;
		}
		return expiredBuff;
	}

	public boolean isExpired(LeafTuple candidate) {
		/**would it be dangerous with this ?*/
		if (oldest == null) 
			return true;
	    /**End question*/
		try {
			boolean result = (oldest.timestamp > candidate.timestamp);
			return result;
		} catch (Exception e) {
			e.printStackTrace();
			return false;
		}

	}
	
	/**Start re-engineering*/
	public boolean isIdentical(VirtualWindow v) {
		if (!(v instanceof TimeVirtualWindow)) return false;
		if (!op.equals(v.getOp())) return false;
		TimeVirtualWindow t = (TimeVirtualWindow)v;
		if (this.duration != t.getDuration() || this.slide != t.getSlide()) return false;
		return true;
	}
	
	public void slide() {
		long curTime = System.nanoTime();
		List<Cont_Dep_ExpM> expiredTuples = purge(curTime);
		
		//System.out.println("periodic slide");
		if (expiredTuples.isEmpty() && oldest == null) return;
		
		LeafTuple guardTuple = oldest;
		if (guardTuple == null) {
			guardTuple = new LeafTuple();
			LeafTuple expiredTuple = expiredTuples.get(expiredTuples.size() - 1).getLeafTuple();
			guardTuple.timestamp = Long.MAX_VALUE - 1;
			guardTuple.prevOlder = expiredTuple;
		}
		
		pW.updateExpTupleFromVW(guardTuple);
		if (!expiredTuples.isEmpty())
			currentMJoinRouter.routeExpiration(expiredTuples);

		
		//pW.updateOldestInvalidItem(oldest);
	}
	
	public class PeriodicSlider extends TimerTask {
		TimeVirtualWindow tvw;
		public PeriodicSlider(TimeVirtualWindow tvw) {
			this.tvw = tvw;
		}
		@Override
		public void run() {
			tvw.slide();
		}
	}
	
	public void killme() {
		slider.cancel();
		System.out.println("Kiled periodic thread from sliding window");
	}
	/**End re-engineering*/
	
}
