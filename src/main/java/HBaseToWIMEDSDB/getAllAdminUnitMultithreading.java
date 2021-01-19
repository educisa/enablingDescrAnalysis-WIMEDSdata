package HBaseToWIMEDSDB;

import java.io.IOException;

import org.json.JSONArray;

public class getAllAdminUnitMultithreading {
	
	
	public static void main(String[] args) throws IOException, Exception{

		fullextractAdminUnitmultiThreading  orgUnitsExtr = new fullextractAdminUnitmultiThreading();

		String rootPath = Thread.currentThread().getContextClassLoader().getResource("").getPath();
		String ctrlPath = rootPath +"control.properties";


		orgUnitsExtr.setProperties(ctrlPath);

		String scanner_id = orgUnitsExtr.getScannerId();
		
		//marribara el JSONArrayContent amb tots els AdminUnit encoded
		JSONArray content = orgUnitsExtr.getDataFromHBase(scanner_id, ctrlPath);
		System.out.println("lenght of the final content array: "+ content.length());
		//ara hem hauré de repartir aquest JSONArray amb tants threads com sigui possible
		//cada un d'ells haurà de fer la feina de decode i crear les queries corresponents per cada JSONObject dins el JSONArray parcial que li toqui
		//després el partim a trossos i cada thread crea la seva partialQuery
		int THREADS =  Runtime.getRuntime().availableProcessors();
		System.out.println("using "+ THREADS+" threads");

		//call ParallelPartialQueryGenerator
		ParallelPartialQueryGenerator partialQueryGen = new ParallelPartialQueryGenerator(THREADS);
		
		String SQLQuery = "TRUNCATE TABLE AdministrationUnit;\n";
		//put workers to work and get the result total query 
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
