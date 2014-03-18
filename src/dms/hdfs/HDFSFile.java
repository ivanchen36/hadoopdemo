package dms.hdfs;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class HDFSFile
{
	public static final char NEWLINE = '\n';
	public static final char READ = 1;
	public static final char WRITE = 2;
	public static final char APPEND = 3;
	private String filename;
	private Path path;
	private DataInputStream input;
	private BufferedReader reader;
	private BufferedWriter writer;
	private DataOutputStream output;
	private FileSystem fs;
	private char mode;

	public HDFSFile(FileSystem fs, String filename, char mode) throws IOException
	{
		this.filename = filename;
		this.fs = fs;
		this.path = new Path(filename);
		this.mode = mode;
	}

	public DataInputStream getInput()
	{
		return this.input;
	}

	public DataOutputStream getOutput()
	{
		return this.output;
	}

	public String getFilename()
	{
		return this.filename;
	}

	public void open() throws IOException
	{
		switch (this.mode)
		{
			case HDFSFile.READ:
			{
				this.input = this.fs.open(this.path);
				reader = new BufferedReader(new InputStreamReader(this.input));
				break;
			}
			case HDFSFile.WRITE:
			{
				this.output = this.fs.create(this.path, true);
				writer = new BufferedWriter(new OutputStreamWriter(this.output));
				break;
			}
			case HDFSFile.APPEND:
			{
				this.output = this.fs.append(this.path);
				writer = new BufferedWriter(new OutputStreamWriter(this.output));
				break;
			}
			default: break;
		}
	}

	public void close() throws IOException
	{
		switch (this.mode)
		{
			case HDFSFile.READ:
			{
				if (null == this.input) break;
				
				this.input.close();
				break;
			}
			case HDFSFile.WRITE:
			{
				if (null == this.output) break;
				
				this.output.close();
				break;
			}
			case HDFSFile.APPEND:
			{
				if (null == this.output) break;
				
				this.output.close();
				break;
			}
			default: break;
		}
	}
	
	public String readLine() throws IOException
	{
		if (this.reader == null) return null;
		
		return this.reader.readLine();
	}
	
	public void writeLine(String line) throws IOException
	{
		if (this.writer == null) return;
		
		this.writer.write(line + HDFSFile.NEWLINE);
		this.writer.flush();
	}
}
