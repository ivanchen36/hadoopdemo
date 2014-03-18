package dms.handler;

import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.ToolRunner;

import dms.etl.PredictionETL;
import dms.test.PredictionTest;
import dms.train.PredictionTrain;

public class PredcitionHandler implements DMSHandler
{
	public String name;
	public Configuration conf;
	public boolean initFlag;
	
	public PredcitionHandler(String name, Configuration conf)
	{
		this.name = name;
		this.conf = conf;
		this.initFlag = false;
	}
	
	public void init(Map<String, String> map)
	{
		String userdir = conf.get(DMSHandler.USERDIR);
		String srcStr = map.get(DMSHandler.ETLSRC);
		String dstStr = map.get(DMSHandler.ETLDST);
		String sepStr = map.get(DMSHandler.ETLSEP);
		
		if(null == srcStr || null == dstStr || null == sepStr)
			return;

		this.conf.set(DMSHandler.ETLSRC, srcStr);
		this.conf.set(DMSHandler.ETLDST, dstStr);
		this.conf.set(DMSHandler.ETLSEP, sepStr);
		this.conf.set(DMSHandler.ETLSEP, sepStr);
		this.conf.set(DMSHandler.ETLINPUT, userdir + "/data");
		this.conf.set(DMSHandler.ETLOUTPUT, userdir + "/train");
		this.conf.set(DMSHandler.TRAININPUT, userdir + "/train");
		this.conf.set(DMSHandler.TRAINOUTPUT, userdir + "/null");
		this.conf.set(DMSHandler.TESTINPUT, userdir + "/train");
		this.conf.set(DMSHandler.TESTOUTPUT, userdir + "/store");
		this.conf.set(DMSHandler.MODELFILE, userdir + "/model/prefiction.model");
		this.initFlag = true;
	}
	
	@Override
	public void run()
	{
		if (!this.initFlag) return;
		try
		{
			ToolRunner.run(this.conf, new PredictionETL(), null);
			ToolRunner.run(this.conf, new PredictionTrain(), null);
			ToolRunner.run(this.conf, new PredictionTest(), null);
		} catch (Exception e)
		{
			e.printStackTrace();
		}
	}

}
