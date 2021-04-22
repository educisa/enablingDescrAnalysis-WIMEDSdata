package LandingZoneToFormattedZone;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import org.json.JSONArray;

public class updateAdminUnitInFormattedZone {


	public updateAdminUnitInFormattedZone() {}

	public void extractOrgUnitsFromLZ() throws InterruptedException, IOException {

		updateOrgUnits_multiThreading orgUnitsUpdate = new updateOrgUnits_multiThreading();

		String rootPath = Thread.currentThread().getContextClassLoader().getResource("").getPath();
		String ctrlPath = rootPath +"control.properties";

		orgUnitsUpdate.setProperties(ctrlPath);

		//see if we want to extract OrgUnits
		String tablesNeededInTZ = orgUnitsUpdate.getTZtablesNames();
		String[] tablesTZ = tablesNeededInTZ.split(",");
		List<String> tablesList = Arrays.asList(tablesTZ);
		if(tablesList.contains("AdministrationUnit")) {
			//creates a scanner with TimeRange config
			String scanner_id = orgUnitsUpdate.getTRScannerID();
			JSONArray updateRowsContent = orgUnitsUpdate.updateData(scanner_id);

			int THREADS = 1;
			if(orgUnitsUpdate.getUseMultiThreading().equals("true"))THREADS = Runtime.getRuntime().availableProcessors();
			System.out.println("using "+ THREADS+" threads");

			//call ParallelPartialQueryGenerator with isUpdate param = true (if its false it will generate a bulk operation with INSERT queries)
			ParallelPartialQueryGenerator partialQueryGen = new ParallelPartialQueryGenerator(THREADS);

			String SQLQuery="";
			//put workers to work and get the result query 
			SQLQuery += partialQueryGen.partialQueriesSum(updateRowsContent, "AdministrationUnit");

			if(SQLQuery.equals(""))System.out.println("Everything up-to-date");
			else {
				String DBurl = orgUnitsUpdate.getDBurl();
				String DBusr = orgUnitsUpdate.getDBusr();
				String DBpsw = orgUnitsUpdate.getDBpwd();
				//load to postgresDB
				orgUnitsUpdate.LoadInDB(SQLQuery, DBurl, DBusr, DBpsw);
			}

			//update extraction times in any case
			orgUnitsUpdate.setExtractionTimes(ctrlPath);
		}
		//ELSE => we do not need AdministrationUnits now in FormattedZone Zone (it is not in TZtables in properties file)
		//do nothing
	}

}
