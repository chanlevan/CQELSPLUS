package cqelsplus.launch;


import java.io.BufferedReader;
import java.io.FileReader;

import cqelsplus.engine.ExecContext;
import cqelsplus.engine.RDFStream;

public class TextStream extends RDFStream implements Runnable{
	String txtFile;
	boolean stop=false;
	long sleep=0;
	public TextStream(ExecContext context, String uri,String txtFile) {
		super(context, uri);
		this.txtFile=txtFile;
	}

	@Override
	public void stop() {
		stop=true;
	}
	public void setRate(int rate){
		sleep=1000/rate;
	}
	
	public void run() {
		// TODO Auto-generated method stub
		//while (true) {
			try {
				//while (true){
					BufferedReader reader = new BufferedReader(new FileReader(txtFile));
					String strLine;
					while ((strLine = reader.readLine()) != null)   {
						//System.out.println(strLine);
					    String[] data=strLine.split(" ");
						stream(n(data[0]),n(data[1]),n(data[2]));
						//Thread.sleep(3000);
					}
					reader.close();
				//}
			
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} 
		//}
	}
}
