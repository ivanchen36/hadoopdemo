package dms.format;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;

import dms.handler.DMSHandler;
import dms.hdfs.HDFSFile;

public class PredictionTrainDataFormat implements ETLTrainDataFormat<Text, IntWritable>
{

	public class PredictionEntry implements Entry<Text, IntWritable>
	{
		private Text key;
		private IntWritable val;

		private PredictionEntry(Text key, IntWritable val)
		{
			this.key = key;
			this.val = val;
		}

		public Text getKey()
		{
			return this.key;
		}

		public IntWritable getVal()
		{
			return this.val;
		}
	}
	
	public static final char NOAID = '0';
	public static final char REQUEST_TYPE = '1';
	public static final char SHOW_TYPE = '2';
	public static final char CLICK_TYPE = '3';
	public static final int CLICK = 1;
	public static final int NOCLICK = 0;
	public static final int GOODAID = '1';
	public static final int BADAID = '0';
	public static final IntWritable NOCLICKVAL = new IntWritable(NOCLICK);
	public static final IntWritable CLICKVAL = new IntWritable(CLICK);
	public static final String CITY_APTH = "/profile/city.data";
	public static final String AID_APTH = "/profile/aid.data";

	private String sepChar;
	private ArrayList<String> dimensions;
	private Configuration conf;
	private static Map<String, String> CITY_MAP;
	private static Map<String, String> AID_MAP;
	private FileSystem fs;

	public void initCityMap() throws IOException
	{
		String filename = conf.get(DMSHandler.USERDIR) + PredictionTrainDataFormat.CITY_APTH;
		HDFSFile file = new HDFSFile(this.fs, filename, HDFSFile.READ);
		PredictionTrainDataFormat.CITY_MAP = new HashMap<String, String>(100);

		String tmp;
		int idx = 0;

		file.open();
		while ((tmp = file.readLine()) != null)
		{
			++idx;
			PredictionTrainDataFormat.CITY_MAP.put(tmp, String.valueOf(idx));
		}
		file.close();
	}

	public void initAIDMap() throws IOException
	{
		String filename = conf.get(DMSHandler.USERDIR) + PredictionTrainDataFormat.AID_APTH;
		HDFSFile file = new HDFSFile(this.fs, filename, HDFSFile.READ);
		PredictionTrainDataFormat.AID_MAP = new HashMap<String, String>(100);

		String tmp;
		String[] arr;

		file.open();
		while ((tmp = file.readLine()) != null)
		{
			arr = tmp.split(DMSHandler.SEPCHAR);
			if (arr.length != 2)
				return;

			PredictionTrainDataFormat.AID_MAP.put(arr[0], arr[1]);
		}
		file.close();
	}

	public PredictionTrainDataFormat(String dst, String sep, Configuration conf, FileSystem fs) throws IOException
	{
		String[] arr = dst.split(sep);
		int len = arr.length;
		this.dimensions = new ArrayList<String>(len);
		for (int i = 0; i < len; ++i)
		{
			this.dimensions.add(arr[i]);
		}
		this.sepChar = sep;
		this.conf = conf;
		this.fs = fs;
		synchronized (this)
		{
			if (PredictionTrainDataFormat.CITY_MAP == null)
			{
				this.initCityMap();
				this.initAIDMap();
			}
		}
	}

	@Override
	public PredictionEntry format(List<String> list)
	{
		int len = dimensions.size();
		if (len != list.size())
			return null;

		String tmp;
		String key = "";
		char c;
		IntWritable val = null;
		--len;
		for (int i = 0; i <= len; ++i)
		{
			tmp = list.get(i);
			if (this.dimensions.get(i).equals(ADFormat.CITYNAME))
			{
				tmp = PredictionTrainDataFormat.CITY_MAP.get(tmp);
				if (null == tmp)
					return null;
			} else if (this.dimensions.get(i).equals(ADFormat.REQUESTDATE))
			{
				Pattern pattern = Pattern.compile(" ([0-9]{1,}):");
				Matcher matcher = pattern.matcher(tmp);
				tmp = matcher.find() ? matcher.group(1) : "00";
			} else if (this.dimensions.get(i).equals(ADFormat.REQUESTMODE))
			{
				c = tmp.charAt(0);
				if (PredictionTrainDataFormat.SHOW_TYPE == c)
					return null;

				val = (c == PredictionTrainDataFormat.REQUEST_TYPE) ? PredictionTrainDataFormat.NOCLICKVAL
						: PredictionTrainDataFormat.CLICKVAL;
				continue;
			} else if (this.dimensions.get(i).equals(ADFormat.ADORDERID))
			{
				c = tmp.charAt(0);
				if (PredictionTrainDataFormat.NOAID == c)
					return null;

				tmp = PredictionTrainDataFormat.AID_MAP.get(tmp);
				if (null == tmp)
					return null;
			}
			key += tmp;
			if (i != len)
			{
				key += this.sepChar;
			}
		}
		return new PredictionEntry(new Text(key), val);
	}
	
	public String getDst()
	{
		String rs = "";
		int len = dimensions.size();
		
		for (int i = 0; i < len; ++i)
		{
			if (this.dimensions.get(i).equals(ADFormat.REQUESTMODE)) continue;
			
			rs += dimensions.get(i);
			if (i != len - 1)
			{
				rs += DMSHandler.SEPCHAR;
			}
		}
		
		return rs;
	}
}
