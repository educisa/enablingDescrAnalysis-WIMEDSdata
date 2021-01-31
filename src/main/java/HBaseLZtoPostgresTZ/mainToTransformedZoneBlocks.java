package HBaseLZtoPostgresTZ;

import java.io.IOException;

import org.json.JSONArray;

public class mainToTransformedZoneBlocks {
	

	//de moment només agafo de prova les dades de AdministrationUnits
	//les carrego sobre una bd postgres de testing named dbBlocks

	public static void main(String[] args) throws IOException {
		
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
		
		
		
		String colQuali = "AdministrationUnit";//get them from CSV in control.properties
		JSONArray jsonA = ttz.getDataFromHBase(ctrlPath, colQuali);
		System.out.println("el tamany total de les dades de AdministrationUnit: "+ jsonA.length());
		
		
		
		/*
		//priova fent més crides que blockNum
		//un dels statusCode i msg de Response hauria de sortir que no ha trobat res, en principi no es un 200 OK
		for(int i = 0; i < 5; ++i) {
			
			String content = ttz.getDataBlocks(ctrlPath, "AdministrationUnit", i);
			//calculem la size de cada un dels strings aixi comparo bé si s'ha fet tot correctament
			//amb les sizes agafades quan faig el store a HBase
			JSONArray jsonA = new JSONArray(content);
			System.out.println("el tamany del data block "+i+" de AdministrationUnit es: "+ jsonA.length());
			
		}*/
		
	
		//ttz.LoadInDB(reqSQLQuery, tz_DBurl, tz_DBusr, tz_DBpsw);
	}

	
}
