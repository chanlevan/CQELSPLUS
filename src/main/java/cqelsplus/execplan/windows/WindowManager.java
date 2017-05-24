package cqelsplus.execplan.windows;

import java.util.ArrayList;
import java.util.Hashtable;
import java.util.List;
import java.util.Map.Entry;

public class WindowManager {
	private Hashtable<String, PhysicalWindow> pWs;
	private int pWCount;
	private List<VirtualWindow> allVWs;
	private static WindowManager wm;
	
	private WindowManager() {
		pWs = new Hashtable<String, PhysicalWindow>();
		pWCount = 0;
		allVWs = new ArrayList<VirtualWindow>();
	}

	private WindowManager(Hashtable<String, PhysicalWindow> pWs, int pWCount, List<VirtualWindow> allVWs) {
		this.pWs = pWs;
		this.pWCount = pWCount;
		this.allVWs = allVWs;
	}
	
	public static WindowManager getInstance() {
	if (wm == null) {
		wm = new WindowManager();
	}
	return wm;
	}
	
	public PhysicalWindow getNotUpatedPhysicalWindow(String pwCode) {
		PhysicalWindow pW = pWs.get(pwCode); 
		return pW;
	}
	
	
	public PhysicalWindow getPhysicalWindow(String pwCode) {
		PhysicalWindow pW = pWs.get(pwCode); 
		if (pW == null) {
			
			pW = new PhysicalWindow(pwCode, pWCount ++);
			pWs.put(pwCode, pW);
		}
		return pW;
	}
	
	public PhysicalWindow getStaticWindow(String pwCode) {
		PhysicalWindow pW = pWs.get(pwCode); 
		if (pW == null) {
			pW = new StaticWindow(pwCode, pWCount ++);
			pWs.put(pwCode, pW);
		}
		return pW;
	}
	
	public PhysicalWindow getBufferingWindow(String pwCode) {
		PhysicalWindow pW = pWs.get(pwCode);
		if (pW == null) {
			pW = new BufferingWindow(pwCode, pWCount++);
			pWs.put(pwCode, pW);
		}
		return pW;
	}
	
	public List<PhysicalWindow> getPWs() {
		List<PhysicalWindow> tmp = new ArrayList<PhysicalWindow>();
		for (Entry<String, PhysicalWindow> entry : pWs.entrySet()) {
			tmp.add(entry.getValue());
		}
		return tmp;
	}
	
	public int getWindowNum() {
		return pWCount;
	}
	
	public void addVirtualWindows(List<VirtualWindow> vWs) {
		for (VirtualWindow v : vWs) {
			boolean belongingV = false;
			for (VirtualWindow tV : allVWs) {
				if (tV.getId() == v.getId()) {
					belongingV = true;
					break;
				}
			}
			if (!belongingV)
				allVWs.add(allVWs.size(), v);
		}
	}
	
	public List<VirtualWindow> getVirtualWindows() {
		return allVWs;
	}
	
	public void printLog() {
		List<PhysicalWindow> pWList = this.getPWs();
		for (PhysicalWindow p : pWList) {
			p.printLog();
		}
	}
	
		
	public void removeVirtualWindow(VirtualWindow vW) {
		for (int i = 0; i < allVWs.size(); i++) {
			if (allVWs.get(i).equals(vW)) {
				allVWs.remove(i);
				break;
			}
		}
	}
	
	public void removePhysicalWindow(PhysicalWindow pW) {
		for (Entry<String, PhysicalWindow> entry : pWs.entrySet()) {
			if (entry.getValue().getId() == pW.getId()) {
				String pWCode = entry.getKey();
				pWs.remove(pWCode);
				break;
			}
		}
	}
	
	/**Start engineering*/
	public VirtualWindow getIdenticalVW(VirtualWindow vW) {
		VirtualWindow resultV = null;
		for (VirtualWindow v : allVWs) {
			if (!v.isIdenticalized() && v.isIdentical(vW)) { 
				resultV = v;
				v.setIdenticalized(true);
				break;
			}
		}
		return resultV;
	}
	
	public void restartIdent4VirtualWindow() {
		for (VirtualWindow vW : allVWs) {
			vW.setIdenticalized(false);
		}
	}
	
	public WindowManager clone() {
		Hashtable<String, PhysicalWindow> pWs = (Hashtable<String, PhysicalWindow>)this.pWs.clone();
		int pWCount = this.pWCount;
		List<VirtualWindow> allVWs = new ArrayList<VirtualWindow>();
		allVWs.addAll(this.allVWs);
		return new WindowManager(pWs, pWCount, allVWs);
		
	}
	
	public void reset() {
		for (VirtualWindow v : allVWs) {
			v.reset();
		}
	}
	/**Stop engineering*/
} 
