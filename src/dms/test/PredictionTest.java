package dms.test;

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
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.mahout.classifier.sgd.OnlineLogisticRegression;
import org.apache.mahout.math.Vector;

import dms.conf.DMSJobconf;
import dms.handler.DMSHandler;
import dms.hdfs.DMSHDFS;
import dms.hdfs.DataOutputFormat;
import dms.hdfs.HDFSFile;
import dms.utils.TrainTestUtil;

public class PredictionTest extends Configured implements Tool
{
	public static final String JOBNAME = "PredictionTest";
	
	public static class TestMapper extends Mapper<LongWritable, Text, Text,IntWritable>
	{
		private static FileSystem fs;
		private static OnlineLogisticRegression learningAlgo;
		private Text key = new Text();
		private IntWritable val = new IntWritable();
		
		@Override
		public void map(LongWritable key,Text value, Context context) throws IOException
		{
			String line;
			String [] arr;
			Vector v;
			int rs;

			synchronized (this)
			{
				if (null == TestMapper.fs)
				{
					TestMapper.fs = FileSystem.get(context.getConfiguration());
				}
				if (null == TestMapper.learningAlgo)
				{
					String fileName = context.getConfiguration().get(DMSHandler.MODELFILE);
					HDFSFile file = new HDFSFile(TestMapper.fs, fileName, HDFSFile.READ);;
					file.open();
					TestMapper.learningAlgo = new OnlineLogisticRegression();
					TestMapper.learningAlgo.readFields(file.getInput());
					file.close();
				}
			}
			line = value.toString();
			rs = line.lastIndexOf(DMSHandler.SEPCHAR);
			line = line.substring(0, rs);
			arr = line.split(DMSHandler.SEPCHAR);
			v = TrainTestUtil.getVector(arr, arr.length);
			rs = (int)(TestMapper.learningAlgo.classifyFull(v).get(1) * 10000);
			this.key.set(line);
			this.val.set(rs);
			try
			{
				context.write(this.key, this.val);
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

	public static class TestCombiner extends Reducer<Text, IntWritable, Text, IntWritable>
	{
		@Override
		public void reduce(Text key, Iterable<IntWritable> values, Context context)
				throws IOException
		{
			try
			{
				context.write(key, values.iterator().next());
			} catch (InterruptedException e)
			{
				e.printStackTrace();
			} 
		}
	}

	public static class TestReducer extends Reducer<Text, IntWritable, Text, IntWritable>
	{
		@Override
		public void reduce(Text key, Iterable<IntWritable> values, Context context)
				throws IOException
		{
			try
			{
				context.write(key, values.iterator().next());
			} catch (InterruptedException e)
			{
				e.printStackTrace();
			}
		}
	}

	public int run(String[] args) throws IOException, InterruptedException, ClassNotFoundException
	{
		int rs = 0;
		Configuration conf = this.getConf();
		Configuration testConf = new DMSJobconf();
		FileSystem fs = FileSystem.get(conf);
		DMSHDFS hdfs = new DMSHDFS(fs);
		String input = conf.get(DMSHandler.TESTINPUT);
		String output = conf.get(DMSHandler.TESTOUTPUT);
		
		hdfs.rmr(output);
		testConf.set(DMSHandler.MODELFILE, conf.get(DMSHandler.MODELFILE));
		testConf.set(DMSHandler.OUTPUTFILE, "PredictionTest.data");
		Job job = new Job(testConf, PredictionTest.JOBNAME);
		job.setJarByClass(PredictionTest.class);
		job.setMapperClass(TestMapper.class);
		job.setCombinerClass(TestCombiner.class);
		job.setReducerClass(TestReducer.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(DataOutputFormat.class);
		TextInputFormat.addInputPath(job, new Path(input));
		DataOutputFormat.setOutputPath(job, new Path(output));
		fs.close();
		
		rs = job.waitForCompletion(true) ? 0 : 1;
		
		return rs;
	}
}