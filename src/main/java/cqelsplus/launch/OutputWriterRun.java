package cqelsplus.launch;

import java.io.BufferedWriter;

public class OutputWriterRun implements Runnable {
	private BufferedWriter bw;
	private String result;
	public OutputWriterRun(BufferedWriter bw, String result) {
		this.bw = bw;
		this.result = result;
	}
	@Override
	public void run() {
		synchronized(bw) {
			try {
				bw.write(result);
				bw.flush();
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
	}

}
