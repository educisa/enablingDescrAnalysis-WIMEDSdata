package HBaseToWIMEDSDB;

import java.io.IOException;


public class HBase_orgUnitExtraction {
	public static void main(String[] args) throws IOException, Exception{
		
		organisationUnitsExtraction orgUnitsExtr = new organisationUnitsExtraction();
		
		String rootPath = Thread.currentThread().getContextClassLoader().getResource("").getPath();
		String ctrlPath = rootPath +"control.properties";
		
		orgUnitsExtr.setProperties(ctrlPath);
		
		String scanner_id = orgUnitsExtr.getScannerId();

		String SQLQuery = orgUnitsExtr.getDataFromHBase(scanner_id, ctrlPath);
	
		String DBurl = orgUnitsExtr.getDBurl();
		String DBusr = orgUnitsExtr.getDBusr();
		String DBpsw = orgUnitsExtr.getDBpwd();
		//load to postgresDB
		orgUnitsExtr.LoadInDB(SQLQuery, DBurl, DBusr, DBpsw);
		
	}

}

