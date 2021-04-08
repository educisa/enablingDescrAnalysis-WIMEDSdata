package FormattedZoneToTransformedZone;

import java.io.IOException;

import HBaseLZtoPostgresTZ.landingZoneToTransformedZone;

public class mainFZtoTZ {
	
	
	public static void main(String[] args) throws IOException {
		
		
		FZtoTZ toTransformedZone = new FZtoTZ();

		String rootPath = Thread.currentThread().getContextClassLoader().getResource("").getPath();
		String ctrlPath = rootPath +"control.properties";

		toTransformedZone.setProperties(ctrlPath);
		
		//defining PostgresDB connector params
		String tz_DBurl = toTransformedZone.getTZDBurl();
		String tz_DBusr = toTransformedZone.getTZDBusr();
		String tz_DBpsw = toTransformedZone.getTZDBpwd();
		System.out.println(tz_DBpsw+" "+tz_DBurl+" "+tz_DBusr);
		
		String SQLst = toTransformedZone.getSQLst();
		
		System.out.println(SQLst);
		
		toTransformedZone.LoadInDB(SQLst, tz_DBurl, tz_DBusr, tz_DBpsw);
		
	}

}
