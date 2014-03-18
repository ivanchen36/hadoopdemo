package dms.hdfs;
import java.io.IOException;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class DMSHDFS
{
	private FileSystem fs;

	public DMSHDFS(FileSystem fs)
	{
		this.fs = fs;
	}

	public void mkdir(String folder) throws IOException
	{
		Path path;

		path = new Path(folder);
		if (!this.fs.exists(path))
		{
			this.fs.mkdirs(path);
		}
	}

	public void rmr(String file) throws IOException
	{
		Path path;

		path = new Path(file);
		this.fs.delete(path, true);
	}
	
	public void rm(String file) throws IOException
	{
		Path path;

		path = new Path(file);
		this.fs.delete(path, false);
	}

	public FileStatus[] ls(String file) throws IOException
	{
		Path path;

		path = new Path(file);
		FileStatus[] list = this.fs.listStatus(path);

		return list;
	}

	public boolean create(String file, boolean overwrite) throws IOException
	{
		boolean rs = true;
		FSDataOutputStream os = null;
		Path path;
		
		try
		{
			path = new Path(file);
			os = this.fs.create(path, overwrite);
		} catch (IOException e)
		{
			rs = false;
		}

		finally
		{
			if (os != null)
			{
				os.close();
			}
		}
		return rs;
	}

	public void copyToRremote(String src, String dst) throws IOException
	{
		this.fs.copyFromLocalFile(new Path(src), new Path(dst));
	}

	public void copyToLocal(String src, String dst) throws IOException
	{
		this.fs.copyToLocalFile(new Path(src), new Path(dst));
	}
}