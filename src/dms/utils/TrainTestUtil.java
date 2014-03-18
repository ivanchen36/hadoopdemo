package dms.utils;

import java.io.UnsupportedEncodingException;

import org.apache.hadoop.io.Text;
import org.apache.mahout.math.DenseVector;
import org.apache.mahout.math.Vector;

public class TrainTestUtil
{
	public static Text transformTextToUTF8(Text text, String encoding)
	{
		String value = null;
		try
		{
			value = new String(text.getBytes(), 0, text.getLength(), encoding);
		}
		catch (UnsupportedEncodingException e)
		{
			e.printStackTrace();
		}
		return new Text(value);
	}
	
	public static Vector getVector(String[] arr, int len)
	{
		Vector v = new DenseVector(len);
		
		for(int i = 0; i < len; ++i)
		{
			v.set(i, arr[i].charAt(0) - 48);
		}
		
		return v;
	}
}
