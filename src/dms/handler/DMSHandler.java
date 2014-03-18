package dms.handler;

import java.util.Map;

public interface DMSHandler
{
	public static final String DMSJOBID = "DMSJobId";
	public static final String PREDICITONJOBID = "PreditionJobId";
	public static final String SCHEMAPATH = "/schema";
	public static final String USERDIR = "userdir";
	
	public static final String ETLSRC = "ETLSRC";
	public static final String ETLDST = "ETLDST";
	public static final String ETLSEP = "ETLSEP";
	public static final String INPUT = "Input";
	public static final String OUTPUT = "Output";
	public static final String ETLINPUT = "ETLInput";
	public static final String ETLOUTPUT = "ETLOutput";
	public static final String TRAININPUT = "TrainInput";
	public static final String TRAINOUTPUT = "TrainOutput";
	public static final String TESTINPUT = "TestInput";
	public static final String TESTOUTPUT = "TestnOutput";
	public static final String MODELFILE = "ModelFile";
	public static final String OUTPUTFILE = "OutputFile";
	public static final String SEPCHAR = ",";
	
	public void init(Map<String, String> map);
	
	public void run();
}
