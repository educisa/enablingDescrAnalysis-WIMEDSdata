package HBaseToWIMEDSDB;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.Properties;

public class updateDB_orgUnits {
	public static void main(String[] args) throws IOException, Exception{
		organisationUnitsUpdate orgUnitsUpdate = new organisationUnitsUpdate();
	
		String rootPath = Thread.currentThread().getContextClassLoader().getResource("").getPath();
		String ctrlPath = rootPath +"control.properties";

		orgUnitsUpdate.setProperties(ctrlPath);


		//creates a scanner with TimeRange config
		String scanner_id = orgUnitsUpdate.getTRScannerID();
		
		String SQLQuery = orgUnitsUpdate.updateData(scanner_id);
		
		if(SQLQuery=="")System.out.println("Everything up-to-date");
		else {
			String DBurl = orgUnitsUpdate.getDBurl();
			String DBusr = orgUnitsUpdate.getDBusr();
			String DBpsw = orgUnitsUpdate.getDBpwd();

			orgUnitsUpdate.LoadInDB(SQLQuery, DBurl, DBusr, DBpsw);
		}

	}

}

