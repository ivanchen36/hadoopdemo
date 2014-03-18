package dms.main;

public class DMS
{
	public static void main(String[] args) throws Exception
	{
		DMSListener listen = new DMSListener();
		listen.listen();
		
		System.exit(0);
	}
}