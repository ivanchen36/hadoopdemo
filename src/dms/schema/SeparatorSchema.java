package dms.schema;
import java.util.ArrayList;
import java.util.HashMap;

public class SeparatorSchema implements DMSSchema
{
	private ArrayList<Integer> dimensionIdx;
	private int dimLen;
	private String sepChar;
	
	public SeparatorSchema(String src, String dst, String sep)
	{
		int i;
		Integer tmp;
		String[] arr = src.split(sep);
		int len = arr.length;
		
		HashMap<String, Integer> map = new HashMap<String, Integer>(len);
		for (i = 0; i < len; ++i)
		{
			map.put(arr[i], new Integer(i));
		}
		
		arr = dst.split(sep);
		len = arr.length;
		this.dimensionIdx = new ArrayList<Integer>(len);
		for (i = 0; i < len; ++i)
		{
			tmp = map.get(arr[i]);
			if (tmp != null)
			{
				this.dimensionIdx.add(tmp);
			}
		}
		this.sepChar = sep;
		this.dimLen = map.size();
	}
	
	@Override
	public ArrayList<String> parseLine(String line)
	{
		String[] arr = line.split(this.sepChar);
		ArrayList<String> list = new ArrayList<String> (this.dimLen);
		int len = this.dimensionIdx.size();
		
		if (arr.length != this.dimLen) return null;
		
		for (int i = 0; i < len; ++i)
		{
			list.add(arr[this.dimensionIdx.get(i)]);
		}
		
		return list;
	}
}
