package dms.handler;

import org.apache.hadoop.conf.Configuration;

public class DMSHandlerFactory
{
	public static final String PREDICTION = "Prediction";
	public static final String AIDPREDICTION = "AIDPrediction";
	
	public static DMSHandler createDMSHandler(String HandlerName, Configuration conf)
	{
		if (HandlerName.equals(DMSHandlerFactory.PREDICTION))
		{
			return new PredcitionHandler(DMSHandlerFactory.PREDICTION, conf);
		}else if (HandlerName.equals(DMSHandlerFactory.AIDPREDICTION))
		{
			return new AIDPredcitionHandler(DMSHandlerFactory.PREDICTION, conf);
		}
		
		return null;
	}
}