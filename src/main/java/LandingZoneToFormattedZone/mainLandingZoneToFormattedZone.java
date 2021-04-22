package LandingZoneToFormattedZone;

import java.io.IOException;
import java.text.ParseException;

import org.json.JSONArray;

public class mainLandingZoneToFormattedZone {
	
	
	public static void main(String[] args) throws IOException, InterruptedException, ParseException {
		
		
		landingZoneToFormattedZone lzTotz = new landingZoneToFormattedZone();

		String rootPath = Thread.currentThread().getContextClassLoader().getResource("").getPath();
		String ctrlPath = rootPath +"control.properties";

		lzTotz.setProperties(ctrlPath);
		
		//defining PostgresDB connector params
		
		String tz_DBurl = lzTotz.getDBurl();
		String tz_DBusr = lzTotz.getDBusr();
		String tz_DBpsw = lzTotz.getDBpwd();

		//create Transformed Zone Tables and MV
		PostgresSqlFormattedZone postgresSQLtz = new PostgresSqlFormattedZone(tz_DBurl, tz_DBusr, tz_DBpsw);
		postgresSQLtz.createTables();
		//postgresSQLtz.createMaterializedViews();
		
		//see TZtables from control.properties
		JSONArray content = lzTotz.getLandingZoneWIMEDSdata();
		lzTotz.exportDataToTZ(content);
		
		//export OrgUnits from HBase OrgUnits table to AdministrationUnits table in Formatted zone
		updateAdminUnitInFormattedZone orgUnitsExtr = new updateAdminUnitInFormattedZone();
		orgUnitsExtr.extractOrgUnitsFromLZ();
		
		//refresh materialized views once all data have been exported
		//postgresSQLtz.refreshMaterializedViews();
		
		
	}

}
