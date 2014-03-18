package dms.main;

import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;

import dms.conf.DMSJobconf;
import dms.handler.DMSHandler;
import dms.handler.DMSHandlerFactory;
import dms.job.DMSJobManager;

public class DMSController
{
	public static final String ENDCHAR = "\n";
	public static final String SEPCHAR = ":";
	public static final String HANDLERNAME = "name";
	DMSJobManager jobManager;

	public String handle(String message)
	{
		String handleName = null;
		DMSHandler handler = null;
		String jobId = null;
		Configuration conf = null;
		String[] result = message.split(DMSController.ENDCHAR);
		Map<String, String> map = new HashMap<String, String>(10);
		int len = result.length;
		String[] tmp;

		for (int i = 0; i < len; ++i)
		{
			tmp = result[i].split(DMSController.SEPCHAR);
			if (tmp.length != 2) continue;
			
			if(tmp[0].equals(DMSController.HANDLERNAME))
			{
				handleName = tmp[1];
				continue;
			}
			map.put(tmp[0], tmp[1]);
		}
		if (handleName == null) return null;

		jobId = String.valueOf(new Date().getTime());
		conf = new DMSJobconf();
		conf.set(DMSHandler.DMSJOBID, jobId);
		handler = DMSHandlerFactory.createDMSHandler(handleName, conf);
		if (handler == null) return null;

		handler.init(map);
		handler.run();
		
		return jobId;
	}
}
