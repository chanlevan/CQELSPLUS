package cqelsplus.engine;

public class ExecContextFactory {
	static ExecContext current = null;
	public static ExecContext current(){
		return current;
	}
	
	public static void setExecContext(ExecContext context) {
		current = context;
	}
}
