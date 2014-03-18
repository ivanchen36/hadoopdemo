package dms.format;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;

import dms.handler.DMSHandler;
import dms.hdfs.HDFSFile;

public class ADFormat
{
	public static final String SESSIONID = "SessionID";
	public static final String ADVERTISERSID = "AdvertisersID";
	public static final String ADORDERID = "ADOrderID";
	public static final String ADCREATIVEID = "ADCreativeID";
	public static final String SDKVERSIONNUMBER = "SDKVersionNumber";
	public static final String ADPLATFORMKEY = "AdPlatformKey";
	public static final String ADPLATFORMPROVIDERID = "ADPlatformProviderID";
	public static final String PUTINMODELTYPE = "PutInModelType";
	public static final String REQUESTMODE = "RequestMode";
	public static final String REQUESTDATE = "RequestDate";
	public static final String ADPRICE = "ADPrice";
	public static final String ADPPPRICE = "ADPPPrice";
	public static final String IP = "Ip";
	public static final String APPID = "AppID";
	public static final String APPNAME = "AppName";
	public static final String UUID = "Uuid";
	public static final String DEVICE = "Device";
	public static final String CLIENT = "Client";
	public static final String OSVERSION = "OsVersion";
	public static final String DENSITY = "Density";
	public static final String PW = "Pw";
	public static final String PH = "Ph";
	public static final String LONG = "Long";
	public static final String LAT = "Lat";
	public static final String PROVINCENAME = "ProvinceName";
	public static final String CITYNAME = "CityName";
	public static final String ISPID = "ISPID";
	public static final String NETWORKMANNERID = "NetworkMannerID";
	public static final String NETWORKMANNERNAME = "NetworkMannerName";
	public static final String ISEFFECTIVE = "IsEffective";
	public static final String ISBILLING = "IsBilling";
	public static final String ADSPACETYPE = "AdSpaceType";
	public static final String ADSPACETYPENAME = "AdSpaceTypeName";
	public static final String DEVICETYPE = "DeviceType";
	public static final String PROCESSNODE = "ProcessNode";
	public static final String APPTYPE = "AppType";

	public static final String CITY_APTH = "/profile/city.data";

	private static final String[] DEVICETYPEARR = { "", "电话", "平板电脑" };
	private static final String[] CLIENTTYPEARR = { "", "android", "ios", "wp" };
	private static final String[] ISPTYPEARR = { "", "移动", "联通", "电信", "其他" };
	private static final String[] NETTYPEARR = { "未知", "2G", "3G", "Wifi", "其他" };
	private static final String[] ADSPACETYPEARR = { "", "banner", "插屏", "全屏" };

	private static List<String> CITY_List;
	private ArrayList<String> dimensions;
	private Configuration conf;
	private FileSystem fs;

	public ADFormat(String dst, Configuration conf, FileSystem fs) throws IOException
	{
		String[] arr = dst.split(DMSHandler.SEPCHAR);
		int len = arr.length;
		this.dimensions = new ArrayList<String>(len);
		for (int i = 0; i < len; ++i)
		{
			this.dimensions.add(arr[i]);
		}
		this.conf = conf;
		this.fs = fs;
		synchronized (this)
		{
			if (ADFormat.CITY_List == null)
			{
				this.initCityMap();
			}
		}
	}

	public void initCityMap() throws IOException
	{
		String filename = conf.get(DMSHandler.USERDIR) + PredictionTrainDataFormat.CITY_APTH;
		HDFSFile file = new HDFSFile(this.fs, filename, HDFSFile.READ);
		ADFormat.CITY_List = new ArrayList<String>(100);
		String tmp;

		file.open();
		while ((tmp = file.readLine()) != null)
		{
			ADFormat.CITY_List.add(tmp);
		}
		file.close();
	}

	public String getDeviceType(String idx)
	{
		return ADFormat.DEVICETYPEARR[Integer.valueOf(idx)];
	}

	public String getClientType(String idx)
	{
		return ADFormat.CLIENTTYPEARR[Integer.valueOf(idx)];
	}

	public String getIspType(String idx)
	{
		return ADFormat.ISPTYPEARR[Integer.valueOf(idx)];
	}

	public String getNetType(String idx)
	{
		return ADFormat.NETTYPEARR[Integer.valueOf(idx)];
	}

	public String getAdSpaceType(String idx)
	{
		return ADFormat.ADSPACETYPEARR[Integer.valueOf(idx)];
	}

	public String getCity(String idx)
	{
		return ADFormat.CITY_List.get(Integer.valueOf(idx));
	}

	public String getTime(String idx)
	{
		int time1 = Integer.valueOf(idx);
		int time2 = time1 + 1;

		return String.format("%02d-%02d", time1, time2);
	}

	public String toShowData(String rs)
	{
		String[] arr = rs.split(DMSHandler.SEPCHAR);
		int len = arr.length;
		String show = "";

		--len;
		for (int i = 0; i <= len; i++)
		{
			if (this.dimensions.get(i).equals(ADFormat.CITYNAME))
			{
				show += this.getCity(arr[i]);
			}
			else if (this.dimensions.get(i).equals(ADFormat.ADSPACETYPE))
			{
				show += this.getAdSpaceType(arr[i]);
			}
			else if (this.dimensions.get(i).equals(ADFormat.NETWORKMANNERID))
			{
				show += this.getNetType(arr[i]);
			}
			else if (this.dimensions.get(i).equals(ADFormat.ISPID))
			{
				show += this.getIspType(arr[i]);
			}
			else if (this.dimensions.get(i).equals(ADFormat.CLIENT))
			{
				show += this.getClientType(arr[i]);
			}
			else if (this.dimensions.get(i).equals(ADFormat.DEVICETYPE))
			{
				show += this.getDeviceType(arr[i]);
			}

			if (i != len)
			{
				show += DMSHandler.SEPCHAR;
			}
		}

		return show;
	}
}