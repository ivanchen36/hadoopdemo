package dms.schema;
import java.util.ArrayList;

public interface DMSSchema
{
	public ArrayList<String> parseLine(String line);
}