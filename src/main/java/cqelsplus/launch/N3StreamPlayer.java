package cqelsplus.launch;


import java.io.FileReader;
import java.io.InputStream;
import java.io.Reader;

import org.apache.log4j.Logger;

import com.hp.hpl.jena.n3.turtle.TurtleParseException;
import com.hp.hpl.jena.n3.turtle.parser.ParseException;
import com.hp.hpl.jena.n3.turtle.parser.TokenMgrError;
import com.hp.hpl.jena.n3.turtle.parser.TurtleParser;
import com.hp.hpl.jena.shared.JenaException;
import com.hp.hpl.jena.util.FileUtils;

import cqelsplus.engine.ExecContext;
import cqelsplus.engine.RDFStream;


public class N3StreamPlayer extends RDFStream implements Runnable {
	final static Logger logger = Logger.getLogger(N3StreamPlayer.class);
	
	String stFile;
	String stFolder;
	String streamSourcePath;
	long streamSize;
	/**sCP = starting count point*/
	long startCountPoint;
	
	String expOutput;
	boolean stopped;
	
	N3StreamElementHandler streamHandler;
	public N3StreamPlayer (ExecContext context,String iri, String streamSourcePath, long streamSize, 
			long noOfQueries, long windowSize, long startCountPoint, String expOutput){
		super(context,iri);
		this.streamSourcePath = streamSourcePath;
		this.streamSize = streamSize;
		this.startCountPoint = startCountPoint;
		streamHandler = new N3StreamElementHandler(streamSize, startCountPoint, this);
		N3StreamElementHandler.setGlobalParams(expOutput, noOfQueries, windowSize);
		stopped = false;
	}

	public void setFileName(String fileName) {
		this.stFile = fileName;
	}
	
	public void run() {
		FileReader reader;
		try {
			reader = new FileReader(streamSourcePath);
			parse(getURI(), reader);
			reader.close();
		} catch (Exception e) {
			e.printStackTrace();
		}
		
	}
	
	public void parse(String baseURI, InputStream in) {
	        Reader reader = FileUtils.asUTF8(in) ;
	        parse( baseURI, reader) ;
	}
	
	public  void parse(String baseURI, Reader reader)
	 {
        // Nasty things happen if the reader is not UTF-8.
      try {
            TurtleParser parser = new TurtleParser(reader) ;
            parser.setEventHandler(streamHandler) ;
            parser.setBaseURI(baseURI) ;
            parser.parse() ;
        }

        catch (ParseException ex)
        { throw new TurtleParseException(ex.getMessage()) ; }

        catch (TokenMgrError tErr)
        { throw new TurtleParseException(tErr.getMessage()) ; }

        catch (TurtleParseException ex) { throw ex ; }
        
        catch (JenaException ex)  { throw new TurtleParseException(ex.getMessage(), ex) ; }
        catch (Error err)
        {
            //System.out.println("error"+ err.getMessage());
        	throw new TurtleParseException(err.getMessage() , err) ;
        }
        catch (Throwable th)
        {
            throw new TurtleParseException(th.getMessage(), th) ;
        }
    }
	@Override
	public void stop() {
		stopped = true;
	}
	
	public boolean isStopped() {
		return stopped;
	}
	
	public long getStreamedTriples() {
		return streamHandler.getStreamedTriples();
	}
}
