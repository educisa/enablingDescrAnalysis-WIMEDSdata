package FormattedZoneToTransformedZone;

import java.io.IOException;

import LandingZoneToFormattedZone.landingZoneToFormattedZone;

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
		
		//data wrapper for postgres
		String SQLstDataWrapper = toTransformedZone.getSQLst();
		toTransformedZone.LoadInDB(SQLstDataWrapper, tz_DBurl, tz_DBusr, tz_DBpsw);
		
		//create mat views in transformed zone if needed and refresh them
		toTransformedZone.getSQLmv();
		
	}

}
