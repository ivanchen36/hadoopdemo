package dms.main;

import dms.handler.DMSHandler;
import dms.handler.DMSHandlerFactory;
import dms.handler.PredcitionHandler;

public class DMSListener
{
	public static final String TEST_ETL_SRC = "SessionID,AdvertisersID,ADOrderID,ADCreativeID,ADPlatformProviderID,"
			+ "SDKVersionNumber,AdPlatformKey,PutInModelType,RequestMode,ADPrice,ADPPPrice,RequestDate,Ip,AppID,"
			+ "AppName,Uuid,Device,Client,OsVersion,Density,Pw,Ph,Long,Lat,ProvinceName,CityName,ISPID,ISPName,"
			+ "NetworkMannerID,NetworkMannerName,IsEffective,IsBilling,AdSpaceType,AdSpaceTypeName,DeviceType,"
			+ "ProcessNode,AppType";

	public static final String TEST_ETL_DST = "ADOrderID,Client,ISPID,NetworkMannerID,RequestDate,AdSpaceType,DeviceType,CityName,RequestMode";
	public static final String TEST_ETL_SEP = ",";

	
	public DMSController controller;
	
	public DMSListener()
	{
		this.controller = new DMSController();
	}
	public void listen()
	{
		String str;
		String testMessage = DMSController.HANDLERNAME + DMSController.SEPCHAR + DMSHandlerFactory.PREDICTION + DMSController.ENDCHAR;
		testMessage += PredcitionHandler.ETLSRC + DMSController.SEPCHAR + TEST_ETL_SRC + DMSController.ENDCHAR;
		testMessage += PredcitionHandler.ETLDST + DMSController.SEPCHAR + TEST_ETL_DST + DMSController.ENDCHAR;
		testMessage += PredcitionHandler.ETLSEP + DMSController.SEPCHAR + TEST_ETL_SEP + DMSController.ENDCHAR;
		
		str = this.controller.handle(testMessage);
		testMessage = DMSController.HANDLERNAME + DMSController.SEPCHAR + DMSHandlerFactory.AIDPREDICTION + DMSController.ENDCHAR;
		testMessage += DMSHandler.PREDICITONJOBID + DMSController.SEPCHAR + str + DMSController.ENDCHAR;
		this.controller.handle(testMessage);
		
	}
	
	public void receive()
	{
		
	}
}
