package cqelsplus.launch;

public class InputParams {
	/**Home folder of the CQELS engine*/
	public static String CQELS_HOME = "";
	/**Data folder: All data related to engine such as static, etc.*/
	public static String CQELS_DATA = "";
	/**Boolean flag indicating the engine has to work with static data*/
	public static boolean STATIC_INVOLVED = false;
	/**Boolean flag checking if static source has been loaded*/
	public static boolean STATIC_LOADED = false;

	/**The path to the file containing the list of query name*/
	public static String QUERY_LIST_FILE_NAME = "";
	/**The path to a specific query name*/
	public static String QUERY_NAME = "";
	/**Name of static file*/
	public static String STATIC_SOURCE = "";
	/**Name of stream file*/
	public static String STREAM_SOURCE = "";
	/**The number of triples streamed into engine*/
	public static long STREAM_SIZE;
	/**The size of stream window*/
	public static long WINDOW_SIZE;
	/**The starting value when the evaluation started*/ 
	public static int STARTING_COUNT;
	/**The number of queries for experiment*/
	public static int NUMBER_OF_QUERIES;
	/**Boolean flag indicating if the normalization of probing graph need to perform*/
	public static boolean MJOIN_NORMALIZE = false;
	/**Boolean flag indicating if memory management technique is involved*/
	public static boolean MEMORY_REUSE = false;
	/**Boolean flag indicating if Thread Pool or New Thread is used, the false flag infers the Thread Pool is used*/
	public static boolean NEW_THREAD = false;
	/**Boolean flag indicating if developer want to print log in some important parts of code*/
	public static boolean PRINT_LOG = false;
	/**Index setup option
	 * 0: init index for all variable columns
	 * 1: Init index based on variable columns needed
	 * */
	public static int INDEX_SETUP_OPTION = 0;
	
	/**Output destination*/ 
	public static String OUTPUT_DES = "";
	/**Experiment values output*/
	public static String EXPOUT = "";
	/**Engine result log*/
	public static String RESULTLOG = "";


}
