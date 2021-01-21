package HBaseToWIMEDSDB;

import java.io.IOException;

import org.json.JSONArray;

public class getAllAdminUnitMultithreading {
	
	
	public static void main(String[] args) throws IOException, Exception{
		System.out.println("ˇˇapplication startedˇˇ");
		fullextractAdminUnitmultiThreading  orgUnitsExtr = new fullextractAdminUnitmultiThreading();

		String rootPath = Thread.currentThread().getContextClassLoader().getResource("").getPath();
		String ctrlPath = rootPath +"control.properties";


		orgUnitsExtr.setProperties(ctrlPath);

		String scanner_id = orgUnitsExtr.getScannerId();

		JSONArray content = orgUnitsExtr.getDataFromHBase(scanner_id, ctrlPath);
		System.out.println("lenght of the final content array: "+ content.length());
		int THREADS =  Runtime.getRuntime().availableProcessors();
		System.out.println("using "+ THREADS+" threads");

		//call ParallelPartialQueryGenerator
		ParallelPartialQueryGenerator partialQueryGen = new ParallelPartialQueryGenerator(THREADS);
		
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
		System.out.println("ˇˇapplication finishedˇˇ");

	}

}
