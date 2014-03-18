package dms.hdfs;
import java.io.IOException;

import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class NULLOutputFormat<K, V> extends FileOutputFormat<K, V>
{
	protected static class LineRecordWriter<K, V> extends RecordWriter<K, V>
	{

		@Override
		public void close(TaskAttemptContext arg0) throws IOException, InterruptedException
		{
		}

		@Override
		public void write(K arg0, V arg1) throws IOException, InterruptedException
		{
		}
	}
	@Override
	public RecordWriter<K, V> getRecordWriter(TaskAttemptContext arg0) throws IOException, InterruptedException
	{
		return new LineRecordWriter<K, V>();
	}
}