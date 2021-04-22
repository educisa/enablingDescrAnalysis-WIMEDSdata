package FormattedZoneToTransformedZone;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

public class FZtoTZ {

	
	public FZtoTZ(){}
	String fz_DBurl, fz_DBusr, fz_DBpwd;//formatted zone, where the schema is
	String FZdbname, FZhost, FZport, FZupdatable; //useful for creating the foreign connection
	String tz_DBurl, tz_DBusr, tz_DBpwd;//transformed zone, where MV are
	//es una nova transformed zone de prova, li diré provaTZdb
	
	

	public String getSQLst() {
		
		//generar sql statements for having foreign tables inside transformed zone and can generate MV from them
		String SQLst = "create extension if not exists postgres_fdw;"; //create 
		
		//set up the connection to the foreign database, that is, formatted zone
		String stConnForeignDB = "DO $$BEGIN "+"create server formattedZone_foreign_server FOREIGN DATA WRAPPER postgres_fdw OPTIONS ("
				+"dbname '"+FZdbname+"',"
				+"host '"+FZhost+"',"
				+"port '"+FZport+"',"
				+"updatable '"+FZupdatable+"');"
						+ "EXCEPTION WHEN duplicate_object THEN NULL;"
						+ "END;$$;"; //ignore
		
		
		//set up user mapping
		String setupUSRmapping = "CREATE USER MAPPING if not exists FOR public SERVER formattedZone_foreign_server OPTIONS("
		+"user '"+fz_DBusr+"',"
		+"password '"+tz_DBpwd+"');";
		
		//create schema statement
		String dropschema ="DROP schema if exists FZ_foreign_tables cascade;";
		String createschema="CREATE SCHEMA if not exists FZ_foreign_tables;";

		//it is strongly reccomended to put the foreign tables in their own schema for tidiness- since Pg does cross-schema queries , no problem there
		//we will have the foreign tables in their own little area so people do not get confused when looking at them
		
		//import foreign schema, for having foreign tables int transformed zone
		//should be parametrized
		String importsFormattedZoneSchema= "IMPORT FOREIGN SCHEMA public "
		//--LIMIT to the tables strictly needed, i need all of them since I already have the needed ones in formatted zone db
		+"FROM server formattedZone_foreign_server "
		+"INTO FZ_foreign_tables;";
		
		//with these statements we already have formatted zone tables as foreign tables inside our transformed zone database
		//send sql string to the db driver
		
		
		//I join the 5 steps in one string and send it through the JDBC Driver
		String statement = SQLst+stConnForeignDB+setupUSRmapping+dropschema+createschema+importsFormattedZoneSchema;
		return statement;
	}
	
	
	
	
	
	//materialized views to be used for OLAP tool Tableau
	public void getSQLmv() {
		
		String SQLQueryMV0 ="CREATE MATERIALIZED VIEW if not exists request_broadOverview AS "
				+ "SELECT distinct r.id as requestID, r.requestdate as requestDate, au.shortname as Country, au1.shortname as WHORegion, d.name as disease, r.diseasephase as phase, r.healthFacility"
				+ " FROM fz_foreign_tables.request r, fz_foreign_tables.administrationunit au, fz_foreign_tables.disease d, fz_foreign_tables.administrationunit au1"
				+ " WHERE r.countryid=au.id and r.diseaseid=d.id and au.parentid=au1.id"
				+ " order by r.id, r.requestdate, au.shortname, d.name;";
		
		String SQLQueryMV1 = "CREATE materialized view if not exists described_requestsoverview as"
				+ " SELECT distinct r.id, abs(s.receptiondate :: date - r.requestdate :: date) as requestProcessTime_days,"
				+ "abs(s.shipmentcreationdate :: date - r.requestdate :: date) as requestToShipmentTime_days,"
				+ "abs(s.shippeddate :: date - s.shipmentcreationdate :: date) as timeToPrepareShipment_days,"
				+ "abs(s.receptiondate :: date - s.shippeddate :: date) as shipmentDuration_days,"
				+ "s.shipmentcreationdate, s.shipmentstatus, s.EDD, s.shippeddate, s.receptiondate, r.quantity, s.quantityreceived,"
				+ "r.requestdate as requestDate, r.healthfacility, au.shortname as Country, au1.shortname as WHORegion, d.name as disease,"
				+ "r.patientweightinkg as patient_weigthInKg, r.patientage as patient_age, r.diseasephase as phase, r.transmissionway,"
				+ "rs.name as requeststatus, ms.name as medicalsupplyname, ms.completename as medicalsupplyFormat"
				+ " from fz_foreign_tables.request r LEFT JOIN fz_foreign_tables.shipmentr s on r.id=s.requestid "
				+ "left join fz_foreign_tables.disease d on d.id=r.diseaseid "
				+ "left join fz_foreign_tables.administrationunit au on au.id=r.countryid "
				+ "left join fz_foreign_tables.administrationunit au1 on au.parentid=au1.id "
				+ "left join fz_foreign_tables.requeststatus rs on r.requeststatusid=rs.id "
				+ "left join fz_foreign_tables.medicalsupply ms on r.medicalsupplyid=ms.id;";
		
		String SQLQueryMV2 = "CREATE MATERIALIZED VIEW if not exists MailingOffice_RequestsSummary AS "
				+ "SELECT distinct r.id as requestID, r.requestdate as requestDate, rs.name as requeststatus, au.shortname as Country, au1.shortname as WHORegion, ms.name as medicalSupplyName, ms.completename as medSupCompleteName,"
				+ "r.quantity as askedQuantity, s.quantityreceived, s.couriername, s.trackingnumber, s.edd, s.shippeddate, s.receptiondate, s.healthfacilityname as healthfacility_Requester,"
				+ "abs(s.receptiondate :: date - s.shippeddate :: date) as shipmentDuration_days"
				+ " FROM fz_foreign_tables.request r, fz_foreign_tables.requeststatus rs, fz_foreign_tables.administrationunit au, fz_foreign_tables.administrationunit au1, "
				+ "fz_foreign_tables.medicalsupply ms, fz_foreign_tables.shipmentr s"
				+ " WHERE r.requeststatusid = rs.id and r.id=s.requestid and r.countryid=au.id and au.parentid=au1.id and r.medicalsupplyid=ms.id"
				+ " order by r.id, r.requestdate, au.shortname, s.trackingnumber;";
		
		String SQLQueryMV3="create materialized view if not exists medicalsupply_manufacturerto as "
				+ "SELECT DISTINCT"
				+ "    medicalsupply.id as medicalSupplyID,"
				+ "    manufacturer.id as manufacturerID,"
				+ "	medicalsupply.name as medicalsupply,"
				+ "	manufacturer.name as manufacturer"
				+ " FROM"
				+ "    fz_foreign_tables.medicalsupply as medicalsupply, fz_foreign_tables.manufacturer as manufacturer"
				+ " where manufacturer.id = ANY (medicalsupply.manufacturersids)"
				+ " order by medicalSupplyID, manufacturerID;";
		
		
		String SQLQueryMV = SQLQueryMV0+SQLQueryMV1+SQLQueryMV2+SQLQueryMV3;
		LoadInDB(SQLQueryMV, tz_DBurl, tz_DBusr, tz_DBpwd);
		
		//refresh mat views
		String refreshMV0 = "REFRESH MATERIALIZED VIEW request_broadOverview;";
		String refreshMV1 = "REFRESH MATERIALIZED VIEW MailingOffice_RequestsSummary;";
		String refreshMV2 = "REFRESH MATERIALIZED VIEW described_requestsoverview;";
		String refreshMV3 = "REFRESH MATERIALIZED VIEW medicalsupply_manufacturerto;";

		String SQLQueryRefresh = refreshMV0+refreshMV1+refreshMV2+refreshMV3;
		LoadInDB(SQLQueryRefresh , tz_DBurl, tz_DBusr, tz_DBpwd);
		
	}
	
	
	public void LoadInDB(String SQLstatement, String tz_DBurl, String tz_DBLogin, String tz_DBPassword) {
		Connection cn = null;
		Statement st = null;	
		System.out.println("...working on statements...");
		try {
			//Step 1 : loading driver
			Class.forName("org.postgresql.Driver");
			//Step 2 : Connection
			cn = DriverManager.getConnection(tz_DBurl, tz_DBLogin, tz_DBPassword);
			//Step 3 : creation statement
			st = cn.createStatement();
			
			// Step 5 execution SQLQuery
			st.executeUpdate(SQLstatement);
			System.out.println("statament executed succesfully!");
			
			}
		catch (SQLException e)
		{
			e.printStackTrace();
		}
		catch (ClassNotFoundException e)
		{
			e.printStackTrace();
		}
		finally 
		{
			try 
			{
				cn.close();
				st.close();
			}
			catch (SQLException e)
			{
				e.printStackTrace();
			}
		}
	}
	
	
	
	
	public void setProperties(String ctrlPath) throws IOException {
		Properties props = new Properties();
        FileInputStream in = new FileInputStream(ctrlPath);
        try {
			props.load(in);
		} catch (IOException e1) {
			e1.printStackTrace();
		}
		this.setDBurl(props.getProperty("fz_DBurl"));
		this.setDBusr(props.getProperty("fz_DBusr"));
		this.setDBpwd(props.getProperty("fz_DBpwd"));
		
		this.setFZdbname(props.getProperty("FZdbname"));
		this.setFZhost(props.getProperty("FZhost"));
		this.setFZport(props.getProperty("FZport"));
		this.setFZupdatable(props.getProperty("FZupdatable"));
		
		this.setTZDBurl(props.getProperty("provaTZdbURL"));
		this.setTZDBusr(props.getProperty("provaTZdbUSR"));
		this.setTZDBpwd(props.getProperty("provaTZdbPWD"));
		
        try {
			in.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
		/////////
		
	}
	
	//getters and setters
	public void setDBurl(String str)     {this.fz_DBurl = str;}
	public String getDBurl()             {return fz_DBurl;}
	public void setDBusr(String str)     {this.fz_DBusr = str;}
	public String getDBusr()             {return fz_DBusr;}
	public void setDBpwd(String str)     {this.fz_DBpwd = str;}
	public String getDBpwd()             {return fz_DBpwd;}
	
	public void setFZdbname(String str)     {this.FZdbname = str;}
	public String getFZdbname()             {return FZdbname;}
	public void setFZhost(String str)     {this.FZhost = str;}
	public String getFZhost()             {return FZhost;}
	public void setFZport(String str)     {this.FZport= str;}
	public String getFZport()             {return FZport;}
	public void setFZupdatable(String str) {this.FZupdatable=str;}
	public String getFZupdatable()		{return this.FZupdatable;}

	
	//transformed zone
	public void setTZDBurl(String str)     {this.tz_DBurl = str;}
	public String getTZDBurl()             {return tz_DBurl;}
	public void setTZDBusr(String str)     {this.tz_DBusr = str;}
	public String getTZDBusr()             {return tz_DBusr;}
	public void setTZDBpwd(String str)     {this.tz_DBpwd = str;}
	public String getTZDBpwd()             {return tz_DBpwd;}

}
