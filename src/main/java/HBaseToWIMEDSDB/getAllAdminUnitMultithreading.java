package HBaseToWIMEDSDB;

import java.io.IOException;

import org.json.JSONArray;

public class getAllAdminUnitMultithreading {
	
	
	public static void main(String[] args) throws IOException, Exception{
		System.out.println("··application started··");
		fullextractAdminUnitmultiThreading  orgUnitsExtr = new fullextractAdminUnitmultiThreading();

		String rootPath = Thread.currentThread().getContextClassLoader().getResource("").getPath();
		String ctrlPath = rootPath +"control.properties";


		orgUnitsExtr.setProperties(ctrlPath);

		String scanner_id = orgUnitsExtr.getScannerId();

		JSONArray content = orgUnitsExtr.getDataFromHBase(scanner_id, ctrlPath);
		System.out.println("lenght ORGUNITS: "+ content.length());
		int THREADS =  Runtime.getRuntime().availableProcessors();
		System.out.println("using "+ THREADS+" threads");

		//call ParallelPartialQueryGenerator
		ParallelPartialQueryGenerator partialQueryGen = new ParallelPartialQueryGenerator(THREADS, false);
		
		String SQLQuery = "TRUNCATE TABLE AdministrationUnit;\n";
		//put workers to work and get the result query 
		SQLQuery += partialQueryGen.partialQueriesSum(content);
		
		String DBurl = orgUnitsExtr.getDBurl();
		String DBusr = orgUnitsExtr.getDBusr();
		String DBpsw = orgUnitsExtr.getDBpwd();
		//load to postgresDB
		orgUnitsExtr.LoadInDB(SQLQuery, DBurl, DBusr, DBpsw);
		
		//set extraction times in control.properties
		orgUnitsExtr.setExtractionTimes(ctrlPath);
		System.out.println("··application finished··");

	}

}
