package HBaseLZtoPostgresTZ;

import java.io.IOException;

import java.io.UnsupportedEncodingException;
import java.text.ParseException;

public class mainToTransformedZone {
	
	//FULL RequestMedicines Process data extraction from HBase(landing zone) to TZ, needed for posterior analysis 
	public static void main(String[] args) throws UnsupportedEncodingException, IOException, ParseException {
		
		organisationUnitsExtractionToTZ orgUnitsExtr = new organisationUnitsExtractionToTZ();
		toTransformedZone ttz = new toTransformedZone();
		
		String rootPath = Thread.currentThread().getContextClassLoader().getResource("").getPath();
		String ctrlPath = rootPath +"control.properties";
		
		orgUnitsExtr.setProperties(ctrlPath);
		ttz.setProperties(ctrlPath);
		
		//since for Data we already know the row key, there is no need for a scanner
		//defining PostgresDB connector params
		
		String tz_DBurl = ttz.getDBurl();
		String tz_DBusr = ttz.getDBusr();
		String tz_DBpsw = ttz.getDBpwd();
		
		
		PostgresSqlTransformedZone postgresSQLtz = new PostgresSqlTransformedZone(tz_DBurl, tz_DBusr, tz_DBpsw);
		postgresSQLtz.createTables();
		postgresSQLtz.createMaterializedViews();
		
		
		//Administration Units from DEV-Org-Units Table landing zone HBase
		String scanner_id = orgUnitsExtr.getScannerId();
		String OrgUnitSQLQuery = orgUnitsExtr.getDataFromHBase(scanner_id, ctrlPath);
		ttz.LoadInDB(OrgUnitSQLQuery, tz_DBurl, tz_DBusr, tz_DBpsw);
		orgUnitsExtr.setExtractionTimes(ctrlPath);
		
		
		//Requests
		String reqContent = ttz.getRequestsFromHBase(ctrlPath);
		String reqSQLQuery = ttz.getRequestsQuery(reqContent);
		ttz.LoadInDB(reqSQLQuery, tz_DBurl, tz_DBusr, tz_DBpsw);
		
		//shipmentR
		String shipContent = ttz.getshipmentRFromHBase(ctrlPath);
		String shipSQLQuery = ttz.getshipmentRquery(shipContent);
		ttz.LoadInDB(shipSQLQuery, tz_DBurl, tz_DBusr, tz_DBpsw);
		
		//Disease
		String diseaseContent = ttz.getDiseaseFromHBase(ctrlPath);
		String diseaseSQLQuery = ttz.getDiseaseQuery(diseaseContent);
		ttz.LoadInDB(diseaseSQLQuery, tz_DBurl, tz_DBusr, tz_DBpsw);
		
		//RequestStatus
		String requestStatusContent = ttz.getRequestStatusFromHBase(ctrlPath);
		String requesStatusSQLQuery = ttz.getRequestStatusQuery(requestStatusContent);
		ttz.LoadInDB(requesStatusSQLQuery, tz_DBurl, tz_DBusr, tz_DBpsw);
	
		//Manufacturer
		String manufacturerContent = ttz.getManufacturerFromHBase(ctrlPath);
		String manufacturerSQLQuery = ttz.getManufacturerQuery(manufacturerContent);
		ttz.LoadInDB(manufacturerSQLQuery, tz_DBurl, tz_DBusr, tz_DBpsw);
		
		//MedicalSupply
		String medicalSupplyContent = ttz.getMedicalSupplyFromHBase(ctrlPath);
		String medicalSupplySQLQuery = ttz.getMedicalSupplyQuery(medicalSupplyContent);
		ttz.LoadInDB(medicalSupplySQLQuery, tz_DBurl, tz_DBusr, tz_DBpsw);
		
		//update extraction times from WIMEDS-Table-Data (HBase landing zone) 
		ttz.setExtractionTimes(ctrlPath);
		//once both medicalSupply and manufacturer Tables are populated with HBase data, populate their join table
		postgresSQLtz.populateMedicalSupply_manufacturerTable();
		//refresh materialized views
		postgresSQLtz.refreshMaterializedViews();
		
	}

}
