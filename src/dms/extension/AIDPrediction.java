package dms.extension;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.mahout.classifier.sgd.OnlineLogisticRegression;

import dms.conf.DMSJobconf;
import dms.etl.PredictionETL.ETLMapper;
import dms.format.ADFormat;
import dms.format.PredictionTrainDataFormat;
import dms.handler.DMSHandler;
import dms.hdfs.DMSHDFS;
import dms.hdfs.DataOutputFormat;
import dms.hdfs.HDFSFile;
import dms.schema.SeparatorSchema;
import dms.test.PredictionTest.TestMapper;

public class AIDPrediction extends Configured implements Tool
{
	public static final String JOBNAME = "AIDPrediction";
	
	public static class AIDMapper extends Mapper<LongWritable, Text, Text,Text>
	{
		private Text key = new Text();
		private Text val = new Text();
		
		@Override
		public void map(LongWritable key,Text value, Context context) throws IOException
		{
			String line;
			String aid;
			String rs;
			int pos;

			line = value.toString();
			pos = line.lastIndexOf(DMSHandler.SEPCHAR);
			rs = line.substring(pos + 1);
			line = line.substring(0, pos);
			pos = line.indexOf(DMSHandler.SEPCHAR);
			aid = line.substring(0, pos);
			line = line.substring(pos + 1);
			this.key.set(line);
			this.val.set(aid + DMSHandler.SEPCHAR + rs);
			try
			{
				context.write(this.key, this.val);
			} catch (InterruptedException e)
			{
				e.printStackTrace();
			}
		}
	}
	
	public static class AIDReducer extends Reducer<Text, Text, Text, Text>
	{
		private static FileSystem fs;
		private static ADFormat format;
		private Text val = new Text();
		
		public static void initAIDPrediction(Configuration conf) throws IOException
		{
			String dst;
			int pos;
			String filename = conf.get(DMSHandler.USERDIR) + DMSHandler.SCHEMAPATH + "/"
					+ conf.get(DMSHandler.PREDICITONJOBID);
			HDFSFile file = new HDFSFile(AIDReducer.fs, filename, HDFSFile.READ);
			
			file.open();
			dst = file.readLine();
			pos = dst.indexOf(DMSHandler.SEPCHAR);
			dst = dst.substring(pos + 1);
			format = new ADFormat(dst, conf, AIDReducer.fs);
			file.close();
		}
		
		@Override
		public void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException
		{
			String[] arr;
			float num1 = -1;
			float num2 = -1;
			float f;
			
			synchronized (this)
			{
				if (null == AIDReducer.fs)
				{
					AIDReducer.fs = FileSystem.get(context.getConfiguration());
				}
				if (null == AIDReducer.format)
				{
					AIDReducer.initAIDPrediction(context.getConfiguration());
				}
			}
			
			try
			{
				for (Text rs : values)
				{
					arr = rs.toString().split(DMSHandler.SEPCHAR);
					if (arr.length != 2) return;
					
					if (arr[0].charAt(0) == PredictionTrainDataFormat.GOODAID)
					{
						num1 = Float.valueOf(arr[1]);
					}else
					{
						num2 = Float.valueOf(arr[1]);
					}
				}
				if (-1 == num1 || -1 == num2) return;
				
				f =  (num1 - num2) / 100;
				this.val.set(String.format("%.2f%%", f));
				key.set(AIDReducer.format.toShowData(key.toString()));
				context.write(key, this.val);
			} catch (InterruptedException e)
			{
				e.printStackTrace();
			}
		}
		
		@Override
		protected void cleanup(Context context)
		{
//			if (TestMapper.fs != null)
//			{
//				try
//				{
//					TestMapper.fs.close();
//				} catch (IOException e)
//				{
//					e.printStackTrace();
//				}
//			}
		}
	}

	public int run(String[] args) throws IOException, InterruptedException, ClassNotFoundException
	{
		int rs = 0;
		Configuration conf = this.getConf();
		Configuration aidConf = new DMSJobconf();
		FileSystem fs = FileSystem.get(conf);
		DMSHDFS hdfs = new DMSHDFS(fs);
		String input = conf.get(DMSHandler.INPUT);
		String output = conf.get(DMSHandler.OUTPUT);
		
		hdfs.rmr(output);
		aidConf.set(DMSHandler.PREDICITONJOBID, conf.get(DMSHandler.PREDICITONJOBID));
		aidConf.set(DMSHandler.OUTPUTFILE, "AIDPrediction.data");
		Job job = new Job(aidConf, AIDPrediction.JOBNAME);
		job.setJarByClass(AIDPrediction.class);
		job.setMapperClass(AIDMapper.class);
		job.setReducerClass(AIDReducer.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(DataOutputFormat.class);
		TextInputFormat.addInputPath(job, new Path(input));
		DataOutputFormat.setOutputPath(job, new Path(output));
		fs.close();
		
		rs = job.waitForCompletion(true) ? 0 : 1;
		
		return rs;
	}
}