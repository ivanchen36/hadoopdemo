package dms.conf;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;


public class DMSJobconf extends Configuration
{
	public DMSJobconf()
	{
		this.addResource(new Path("/etc/hadoop/core-site.xml"));
		this.addResource(new Path("/etc/hadoop/hdfs-site.xml"));
//		this.addResource(new Path("/etc/hadoop/mapred-site.xml"));
	}
}