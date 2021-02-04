package HBaseLZtoPostgresTZ;

import java.io.IOException;
import java.text.ParseException;

import org.json.JSONArray;

public class mainToTransformedZoneBlocks {
	

	//de moment només agafo de prova les dades de AdministrationUnits
	//les carrego sobre una bd postgres de testing named dbBlocks

	public static void main(String[] args) throws IOException, InterruptedException, ParseException {
		
		toTransformedZoneBlocks ttz = new toTransformedZoneBlocks();

		String rootPath = Thread.currentThread().getContextClassLoader().getResource("").getPath();
		String ctrlPath = rootPath +"control.properties";

		ttz.setProperties(ctrlPath);
		
		//since for Data we already know the row key, there is no need for a scanner
		//defining PostgresDB connector params
		
		String tz_DBurl = ttz.getDBurl();
		String tz_DBusr = ttz.getDBusr();
		String tz_DBpsw = ttz.getDBpwd();
		
		//get AdministrationUnits (en CAP MOMENT HAIG d'agafar les dades d'adminUnit de WIMEDS-Table-Data)
		//it is just implemented to test block technique implementation
		
		//content String will contain the complete JSONArray from a specific table
		//content es el sumatori de tots els partialArrays recollits dels blocs
		
		
		//see TZtables from control.properties
		ttz.exportDataToTransformedZone();
		
	
		//ttz.LoadInDB(reqSQLQuery, tz_DBurl, tz_DBusr, tz_DBpsw);
	}

	
}
