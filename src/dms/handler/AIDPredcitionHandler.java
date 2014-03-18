package dms.handler;

import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.ToolRunner;

import dms.extension.AIDPrediction;

public class AIDPredcitionHandler implements DMSHandler
{
	public String name;
	public Configuration conf;
	public boolean initFlag;
	
	public AIDPredcitionHandler(String name, Configuration conf)
	{
		this.name = name;
		this.conf = conf;
		this.initFlag = false;
	}
	
	public void init(Map<String, String> map)
	{
		String userdir = conf.get(DMSHandler.USERDIR);
		String preditcionJobid = map.get(DMSHandler.PREDICITONJOBID);
		if (null == preditcionJobid) return;
		
		this.conf.set(DMSHandler.PREDICITONJOBID, preditcionJobid);
		this.conf.set(DMSHandler.INPUT, userdir + "/store");
		this.conf.set(DMSHandler.OUTPUT, userdir + "/aidresult");
		this.initFlag = true;
	}
	
	@Override
	public void run()
	{
		if (!this.initFlag) return;
		try
		{
			ToolRunner.run(this.conf, new AIDPrediction(), null);
		} catch (Exception e)
		{
			e.printStackTrace();
		}
	}

}
