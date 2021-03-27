package HBaseToWIMEDSDB;

import java.io.IOException;

import org.json.JSONArray;

public class mainUpdateDB_OrgUnits_multiThreading {
	
	public static void main(String[] args) throws IOException, Exception{
		System.out.println("··application started··");
		updateOrgUnits_multiThreading orgUnitsUpdate = new updateOrgUnits_multiThreading();
	
		String rootPath = Thread.currentThread().getContextClassLoader().getResource("").getPath();
		String ctrlPath = rootPath +"control.properties";

		orgUnitsUpdate.setProperties(ctrlPath);

		//creates a scanner with TimeRange config
		String scanner_id = orgUnitsUpdate.getTRScannerID();
		JSONArray updateRowsContent = orgUnitsUpdate.updateData(scanner_id);
		
		int THREADS = 1;
		if(orgUnitsUpdate.getUseMultiThreading().equals("true"))THREADS = Runtime.getRuntime().availableProcessors();
		System.out.println("using "+ THREADS+" threads");

		//call ParallelPartialQueryGenerator with isUpdate param = true (if its false it will generate a bulk operation with INSERT queries)
		ParallelPartialQueryGenerator partialQueryGen = new ParallelPartialQueryGenerator(THREADS, true);
		
		String SQLQuery="";
		//put workers to work and get the result query 
		SQLQuery += partialQueryGen.partialQueriesSum(updateRowsContent);
		
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
		System.out.println("··application finished··");
	}

}
