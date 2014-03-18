package dms.train;
import java.io.DataOutputStream;
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
import org.apache.mahout.classifier.sgd.L1;
import org.apache.mahout.classifier.sgd.OnlineLogisticRegression;
import org.apache.mahout.math.Vector;

import dms.conf.DMSJobconf;
import dms.format.PredictionTrainDataFormat;
import dms.handler.DMSHandler;
import dms.hdfs.DMSHDFS;
import dms.hdfs.HDFSFile;
import dms.hdfs.NULLOutputFormat;
import dms.utils.TrainTestUtil;

public class PredictionTrain extends Configured implements Tool
{
	public static final String JOBNAME = "PredictionTrain";
	
	public static class TrainMapper extends Mapper<LongWritable, Text, Text,IntWritable>
	{
		private Text key = new Text();
		private IntWritable val;
		
		@Override
		public void map(LongWritable key,Text value, Context context) throws IOException
		{
			String line;
			int pos;
			
			line = value.toString();
			pos = line.lastIndexOf(DMSHandler.SEPCHAR);
			this.key.set(line.substring(0, pos));
			this.val = line.substring(pos + 1).charAt(0) == '0' ? PredictionTrainDataFormat.NOCLICKVAL : PredictionTrainDataFormat.CLICKVAL;
			
			try
			{
				context.write(this.key, this.val);
			} catch (InterruptedException e)
			{
				e.printStackTrace();
			}
		}
	}

	public static class TrainCombiner extends Reducer<Text, IntWritable, Text, IntWritable>
	{
		private IntWritable request = new IntWritable();
		private IntWritable click = new IntWritable();

		@Override
		public void reduce(Text key, Iterable<IntWritable> values, Context context)
				throws IOException
		{
			int requestNum;
			int clickNum;

			requestNum = 0;
			clickNum = 0;
			for (IntWritable val : values)
			{
				if (val.equals(PredictionTrainDataFormat.NOCLICKVAL))
				{
					--requestNum;
					continue;
				}

				++clickNum;
			}
			request.set(requestNum);
			click.set(clickNum);
			try
			{
				context.write(key, request);
				context.write(key, click);
			} catch (InterruptedException e)
			{
				e.printStackTrace();
			}
		}
	}

	public static class TrainReducer extends Reducer<Text, IntWritable, Text, IntWritable>
	{
		private static FileSystem fs;
		private static OnlineLogisticRegression learningAlgo;
		
		@Override
		public void reduce(Text key, Iterable<IntWritable> values, Context context)
				throws IOException
		{
			String[] arr = key.toString().split(DMSHandler.SEPCHAR);
			int len = arr.length;
			Vector v;
			int tmp;
			int rs;
			int i;
			
			synchronized (this)
			{
				if (null == TrainReducer.fs)
				{
					TrainReducer.fs = FileSystem.get(context.getConfiguration());
				}
				if (null == TrainReducer.learningAlgo)
				{
					TrainReducer.learningAlgo = new OnlineLogisticRegression(2, len, new L1());
					TrainReducer.learningAlgo.lambda(0.1);
					TrainReducer.learningAlgo.learningRate(4);
				}
			}
			
			v = TrainTestUtil.getVector(arr, len);
			for (IntWritable val : values)
			{
				tmp = val.get();
				rs = tmp < 0 ? PredictionTrainDataFormat.NOCLICK : PredictionTrainDataFormat.CLICK;
				tmp = Math.abs(tmp);
				for (i = 0; i < tmp; ++i)
				{
					learningAlgo.train(rs, v);
				}
			}
		}
		
		@Override
		protected void cleanup(Context context)
		{
			HDFSFile file = null;
			String fileName = context.getConfiguration().get(DMSHandler.MODELFILE);
			
			try
			{
				file = new HDFSFile(TrainReducer.fs, fileName, HDFSFile.WRITE);
				file.open();
				DataOutputStream out = file.getOutput();
				TrainReducer.learningAlgo.close();
				TrainReducer.learningAlgo.write(out);
			} catch (IOException e)
			{
				e.printStackTrace();
			} 
			finally
			{
				try
				{
					if (file != null)
					{
						file.close();
					}
//					if (TrainReducer.fs != null)
//					{
//						TrainReducer.fs.close();
//					}
				} catch (IOException e)
				{
					e.printStackTrace();
				}
			}
		}
	}

	public int run(String[] args) throws IOException, InterruptedException, ClassNotFoundException
	{
		int rs = 0;
		Configuration conf = this.getConf();
		Configuration trainConf = new DMSJobconf();
		FileSystem fs = FileSystem.get(conf);
		DMSHDFS hdfs = new DMSHDFS(fs);
		String input = conf.get(DMSHandler.TRAININPUT);
		String output = conf.get(DMSHandler.TRAINOUTPUT);
		
		hdfs.rmr(output);
		hdfs.create(conf.get(DMSHandler.MODELFILE), true);
		trainConf.set(DMSHandler.MODELFILE, conf.get(DMSHandler.MODELFILE));
		Job job = new Job(trainConf, PredictionTrain.JOBNAME);
		job.setJarByClass(PredictionTrain.class);
		job.setMapperClass(TrainMapper.class);
		job.setCombinerClass(TrainCombiner.class);
		job.setReducerClass(TrainReducer.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(NULLOutputFormat.class);
		TextInputFormat.addInputPath(job, new Path(input));
		NULLOutputFormat.setOutputPath(job, new Path(output));
		rs = job.waitForCompletion(true) ? 0 : 1;
		
		return rs;
	}
}