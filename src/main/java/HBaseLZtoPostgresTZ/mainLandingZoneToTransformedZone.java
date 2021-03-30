package HBaseLZtoPostgresTZ;

import java.io.IOException;
import java.text.ParseException;

import org.json.JSONArray;

public class mainLandingZoneToTransformedZone {
	
	
	public static void main(String[] args) throws IOException, InterruptedException, ParseException {
		
		
		landingZoneToTransformedZone lzTotz = new landingZoneToTransformedZone();

		String rootPath = Thread.currentThread().getContextClassLoader().getResource("").getPath();
		String ctrlPath = rootPath +"control.properties";

		lzTotz.setProperties(ctrlPath);
		
		//since for Data we already know the row key, there is no need for a scanner
		//defining PostgresDB connector params
		
		String tz_DBurl = lzTotz.getDBurl();
		String tz_DBusr = lzTotz.getDBusr();
		String tz_DBpsw = lzTotz.getDBpwd();
		
		
		/*
		//create Transoformed Zone Tables and MV
		PostgresSqlTransformedZone postgresSQLtz = new PostgresSqlTransformedZone(tz_DBurl, tz_DBusr, tz_DBpsw);
		postgresSQLtz.createTables();
		postgresSQLtz.createMaterializedViews();
		*/
		//get AdministrationUnits (en CAP MOMENT HAIG d'agafar les dades d'adminUnit de WIMEDS-Table-Data)
		//it is just implemented to test block technique implementation
		//content String will contain the complete JSONArray from a specific table
		//content es el sumatori de tots els partialArrays recollits dels blocs
		
		JSONArray content = lzTotz.getLandingZoneWIMEDSdata();
		lzTotz.exportDataToTZ(content);
		
		/*
		//see TZtables from control.properties
		lzTotz.exportDataToTransformedZone();
		//once both medicalSupply and manufacturer Tables are populated with HBase data, populate their join table
		postgresSQLtz.populateMedicalSupply_manufacturerTable();
		//refresh materialized views once all data have been exported
		postgresSQLtz.refreshMaterializedViews();
		*/
		
	}

}
