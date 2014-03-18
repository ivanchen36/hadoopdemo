package dms.format;
import java.util.List;

public interface ETLTrainDataFormat<KEY, VAL> 
{
	public interface Entry<KEY, VAL> 
	{
		public KEY getKey();
		public VAL getVal();
	}
	
	public Entry<KEY, VAL> format(List<String> list);
	
	public String getDst();
}
