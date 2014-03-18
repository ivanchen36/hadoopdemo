package dms.etl;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.List;

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

import dms.conf.DMSJobconf;
import dms.format.ETLTrainDataFormat;
import dms.format.PredictionTrainDataFormat;
import dms.handler.DMSHandler;
import dms.hdfs.DMSHDFS;
import dms.hdfs.DataOutputFormat;
import dms.hdfs.HDFSFile;
import dms.schema.DMSSchema;
import dms.schema.SeparatorSchema;
import dms.utils.TrainTestUtil;

public class PredictionETL extends Configured implements Tool
{
	public static final String JOBNAME = "PredictionETL";

	public static class ETLMapper extends Mapper<LongWritable, Text, Text, IntWritable>
	{
		private static ETLTrainDataFormat<Text, IntWritable> format;
		private static DMSSchema schema;
		private static FileSystem fs;

		public static void initPretiction(Configuration conf) throws IOException
		{
			String src = conf.get(DMSHandler.ETLSRC);
			String dst = conf.get(DMSHandler.ETLDST);
			String sep = conf.get(DMSHandler.ETLSEP);
			schema = new SeparatorSchema(src, dst, sep);
			format = new PredictionTrainDataFormat(dst, sep, conf, ETLMapper.fs);
			String filename = conf.get(DMSHandler.USERDIR) + DMSHandler.SCHEMAPATH + "/"
					+ conf.get(DMSHandler.DMSJOBID);
			DMSHDFS hdfs = new DMSHDFS(ETLMapper.fs);
			if (hdfs.create(filename, false) == false) return;
			
			HDFSFile file = new HDFSFile(ETLMapper.fs, filename, HDFSFile.WRITE);
			file.open();
			file.writeLine(format.getDst());
			file.close();
		}

		@Override
		public void map(LongWritable key, Text value, Context context) throws IOException
		{
			String line;
			List<String> list;

			synchronized (this)
			{
				if (null == ETLMapper.fs)
				{
					ETLMapper.fs = FileSystem.get(context.getConfiguration());
				}
				if (null == ETLMapper.format)
				{
					ETLMapper.initPretiction(context.getConfiguration());
				}
			}
			value = TrainTestUtil.transformTextToUTF8(value, "gbk");
			line = value.toString();
			if (line.charAt(0) == 'S') return;

			list = ETLMapper.schema.parseLine(line);
			if (null == list) return;

			ETLTrainDataFormat.Entry<Text, IntWritable> entry = ETLMapper.format.format(list);
			if (null == entry) return;

			try
			{
				context.write(entry.getKey(), entry.getVal());
			}
			catch (InterruptedException e)
			{
				e.printStackTrace();
			}
		}

		@Override
		protected void cleanup(Context context)
		{
			// try
			// {
			// if (ETLMapper.fs != null)
			// {
			// ETLMapper.fs.close();
			// }
			// } catch (IOException e)
			// {
			// e.printStackTrace();
			// }
		}
	}

	public static class ETLCombiner extends Reducer<Text, IntWritable, Text, IntWritable>
	{
		private IntWritable request = new IntWritable();
		private IntWritable click = new IntWritable();

		@Override
		public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException
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
			}
			catch (InterruptedException e)
			{
				e.printStackTrace();
			}
		}
	}

	public static class ETLReducer extends Reducer<Text, IntWritable, Text, IntWritable>
	{
		@Override
		public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException
		{
			int val;
			int clickNum;
			int noClickNum;

			clickNum = 0;
			noClickNum = 0;
			for (IntWritable rs : values)
			{
				val = rs.get();
				noClickNum -= val;
				if (val > 0)
				{
					clickNum += val;
				}
			}

			while (clickNum-- > 0)
			{
				try
				{
					context.write(key, PredictionTrainDataFormat.CLICKVAL);
				}
				catch (InterruptedException e)
				{
					e.printStackTrace();
				}
			}

			while (noClickNum-- > 0)
			{
				try
				{
					context.write(key, PredictionTrainDataFormat.NOCLICKVAL);
				}
				catch (InterruptedException e)
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
		Configuration etlConf = new DMSJobconf();
		FileSystem fs = FileSystem.get(conf);
		DMSHDFS hdfs = new DMSHDFS(fs);
		String input = conf.get(DMSHandler.ETLINPUT);
		String output = conf.get(DMSHandler.ETLOUTPUT);

		hdfs.rmr(output);
		etlConf.set(DMSHandler.ETLSRC, conf.get(DMSHandler.ETLSRC));
		etlConf.set(DMSHandler.ETLDST, conf.get(DMSHandler.ETLDST));
		etlConf.set(DMSHandler.ETLSEP, conf.get(DMSHandler.ETLSEP));
		etlConf.set(DMSHandler.OUTPUTFILE, "PredictionTrain.data");
		etlConf.set(DMSHandler.DMSJOBID, conf.get(DMSHandler.DMSJOBID));
		Job job = new Job(etlConf, PredictionETL.JOBNAME);
		job.setJarByClass(PredictionETL.class);
		job.setMapperClass(ETLMapper.class);
		job.setCombinerClass(ETLCombiner.class);
		job.setReducerClass(ETLReducer.class);
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