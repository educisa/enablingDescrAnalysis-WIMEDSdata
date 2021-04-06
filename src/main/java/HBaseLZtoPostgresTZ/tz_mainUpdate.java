package HBaseLZtoPostgresTZ;

import java.io.IOException;

public class tz_mainUpdate {

	public static void main(String[] args) throws IOException, Exception{
		
		String rootPath = Thread.currentThread().getContextClassLoader().getResource("").getPath();
		String ctrlPath = rootPath +"control.properties";

		///UPDATE AdministrationUnit Table from transformedZone Postgres
		tz_updateAdminUnit tzUpdateAU = new tz_updateAdminUnit();
		tzUpdateAU.setProperties(ctrlPath);
		//creates a scanner with TimeRange config
		String scanner_id = tzUpdateAU.getScannerId();
		
		String SQLQuery = tzUpdateAU.updateData(scanner_id);
		
		String tz_DBurl = tzUpdateAU.getDBurl();
		String tz_DBusr = tzUpdateAU.getDBusr();
		String tz_DBpsw = tzUpdateAU.getDBpwd();

		if(SQLQuery=="")System.out.println("everything up-to-date");
		else {
			tzUpdateAU.LoadInDB(SQLQuery, tz_DBurl, tz_DBusr, tz_DBpsw);
		}
		
		//UPDATE all other tables from RequestMedicinesProcess in transformedZone Postgres
		//by the moment it is just a extraction, as done on the first one
		//tz_thisExtraction is updated when extracting AdministrationUnit, not here. 
		//(it could lead to data inconsistencies between landingzone and tz if a more efficient update were implemented)
		
		toTransformedZone ttz = new toTransformedZone();
		ttz.setProperties(ctrlPath);
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

		//once both medicalSupply and manufacturer Tables are updated with HBase data, update their join table
		//ON CONFLICT DO NOTHING, it has been already populated before on the first extraction
		PostgresSqlTransformedZone postgresSQLtz = new PostgresSqlTransformedZone(tz_DBurl, tz_DBusr, tz_DBpsw);
		//postgresSQLtz.populateMedicalSupply_manufacturerTable();
		
		//update extraction times from WIMEDS-Table-Data (HBase landing zone) 
		ttz.setExtractionTimes(ctrlPath);
		//refresh materialized views
		postgresSQLtz.refreshMaterializedViews();

	}

}
