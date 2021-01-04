package HBaseLZtoPostgresTZ;

import java.io.File;
import java.io.FileReader;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.Properties;

public class PostgresSqlTransformedZone {
	
	String DBurl, DBuser, DBpwd;

	public PostgresSqlTransformedZone(String url, String user, String pwd) {
		this.DBurl = url;
		this.DBuser = user;
		this.DBpwd = pwd;
	}
	
	
	public void createTables() {
		
		String SQLQueryAdminUnit = createAdminUnitTable();
		String SQLQueryReq = createRequestsTable();
		String SQLQueryShipmentR = createShipmentRTable();
		String SQLQueryDisease = createDiseaseTable();
		String SQLQUeryRequestStatus = createRequestStatusTable();
		String SQLQUeryManufacturer = createManufacturerTable();
		String SQLQueryMedicalSupply = createMedicalSupplyTable();
		String SQLQueryMedicalSupply_manufacturer = createMedicalSupply_manufacturerTable();
		String SQLQueryTimeDimTable = createTimeDimTable_populate();//not used since Tableau do its work for us
		
		
		String SQLQuery = SQLQueryAdminUnit+SQLQueryReq+SQLQueryShipmentR+SQLQueryDisease+SQLQUeryRequestStatus
				+SQLQUeryManufacturer+SQLQueryMedicalSupply+SQLQueryMedicalSupply_manufacturer+SQLQueryTimeDimTable;
		
		DoitInDB(SQLQuery, DBurl, DBuser, DBpwd);
	}
	
	//materialized views to be used for OLAP tool Tableau
	public void createMaterializedViews() {
		
		String SQLQueryMV0 ="CREATE MATERIALIZED VIEW IF NOT EXISTS requests_summary1 AS"
				+ " SELECT distinct r.id as requestID, r.requestdate as requestDate, r.healthfacility as healthFacility, au.shortname as Country, au1.shortname as continent, rs.name as requestStatus, ms.name as medicalSupply, d.name as disease"
				+ " FROM requests r, administrationunit au, requeststatus rs, medicalsupply ms, disease d, administrationunit au1"
				+ " WHERE r.countryid=au.id and r.requeststatusid=rs.id and r.medicalsupplyid=ms.id and r.diseaseid=d.id and au.parentid=au1.id"
				+ " order by r.id, r.requestdate, au.shortname, rs.name, ms.name, d.name;";
		
		String SQLQueryMV1 = "CREATE MATERIALIZED VIEW IF NOT EXISTS requests_summary2 AS"
				+ " SELECT distinct r.id as requestID, r.requestdate as requestDate, au.shortname as Country, au1.shortname as continent, d.name as disease, r.patientage as patient_age,"
				+ "r.patientweightinkg as patient_weigthInKg, r.diseasephase as phase"
				+ " FROM requests r, administrationunit au, requeststatus rs, medicalsupply ms, disease d, administrationunit au1"
				+ " WHERE r.countryid=au.id and r.requeststatusid=rs.id and r.medicalsupplyid=ms.id and r.diseaseid=d.id and au.parentid=au1.id"
				+ " order by r.id, r.requestdate, au.shortname, d.name, r.patientage, r.patientweightinkg, r.diseasephase;";
		
		String SQLQueryMV2 = "CREATE MATERIALIZED VIEW IF NOT EXISTS manufacturer_requests_summary AS"
				+ " SELECT distinct r.id as requestID, r.requestdate as requestDate, au.shortname as shortname, rs.name as requestStatus, ms.name as medicalSupply, mf.name as manufacturer"
				+ " FROM requests r, administrationunit au, requeststatus rs, medicalsupply ms, manufacturer mf, medicalsupply_manufacturer msm"
				+ " WHERE r.countryid=au.id and r.requeststatusid=rs.id and r.medicalsupplyid=ms.id and ms.id=msm.medicalsupplyid and msm.manufacturerid=mf.id"
				+ " order by r.id, r.requestdate, au.shortname, rs.name, ms.name, mf.name;";
		
		String SQLQueryMV = SQLQueryMV0+SQLQueryMV1+SQLQueryMV2;
		DoitInDB(SQLQueryMV, DBurl, DBuser, DBpwd);
	}
	
	//to be called each time an update from the tables is done, in order to get updated data in Tableau
	public void refreshMaterializedViews() {
		String refreshMV0 = "REFRESH MATERIALIZED VIEW requests_summary1;";
		String refreshMV1 = "REFRESH MATERIALIZED VIEW requests_summary2;";
		String refreshMV2 = "REFRESH MATERIALIZED VIEW manufacturer_requests_summary;";
		
		String SQLQUeryRefreshMV = refreshMV0+refreshMV1+refreshMV2;
		DoitInDB(SQLQUeryRefreshMV, DBurl, DBuser, DBpwd);
	}
	
	
	public void populateMedicalSupply_manufacturerTable() {
		
		String SQLQueryPopulate = "insert into medicalsupply_manufacturer"
				+ " SELECT DISTINCT"
				+ " medicalsupply.id as medicalSupplyID,"
				+ " manufacturer.id as manufacturerID"
				+ " FROM"
				+ " medicalsupply, manufacturer"
				+ " where manufacturer.id = ANY (medicalsupply.manufacturersids)"
				+ " order by medicalSupplyID, manufacturerID"
				+ " ON CONFLICT DO NOTHING;";
		
		DoitInDB(SQLQueryPopulate, DBurl, DBuser, DBpwd);
	}
	
	
	public String createAdminUnitTable() {
		String SQLQuery = "CREATE TABLE IF NOT EXISTS administrationunit"
				+ "("
				+ "id character(11)  NOT NULL PRIMARY KEY,"
				+ "parentId character(11),"
				+ "name character varying(150),"
				+ "shortName character varying(150),"
				+ "dateLastUpdated date,"
				+ "leaf boolean,"
				+ "levelNumber smallint,"
				+ "address character varying(300),"
				+ "url character varying(300)"
				+ ");";
		
		return SQLQuery;
	}
	
	
	public String createRequestsTable() {
		String SQLQuery = "CREATE TABLE IF NOT EXISTS Requests("
				+ "id int NOT NULL PRIMARY KEY,"
				+ "countryid character varying(11),"
				+ "healthfacility character varying(150),"
				+ "diseaseid integer,"
				+ "medicalsupplyid integer,"
				+ "requeststatusid integer,"
				+ "requestdate date,"
				+ "patientAge integer,"
				+ "patientWeightInKg integer,"
				+ "diseasePhase character varying(22)"
				+ ");";
		
		return SQLQuery;
	}
	
	public String createShipmentRTable() {
		
		String SQLQuery = "CREATE TABLE IF NOT EXISTS shipmentR("
				+ "id int NOT NULL PRIMARY KEY,"
				+ "quantity integer,"
				+ "quantityreceived integer,"
				+ "medicalsupplyname character varying(150),"
				+ "requestid integer,"
				+ "shipmentstatus character varying(150),"
				+ "shipmentcreationdate date"
				+ ");";
		
		return SQLQuery;
	}
	public String createDiseaseTable() {
		
		String SQLQuery = "CREATE TABLE IF NOT EXISTS Disease"
				+ "("
				+ "id int NOT NULL PRIMARY KEY,"
				+ "name character varying(150),"
				+ "completeName character varying(150),"
				+ "medicalSuppliesIDs Integer[]"
				+ ");";
		
		return SQLQuery;
	}
	public String createRequestStatusTable() {
		
		String SQLQuery = "CREATE TABLE IF NOT EXISTS RequestStatus"
				+ "("
				+ "id int NOT NULL PRIMARY KEY,"
				+ "name character varying(150)"
				+ ");";
		
		return SQLQuery;
	}
	public String createManufacturerTable() {
		
		String SQLQuery = "CREATE TABLE IF NOT EXISTS Manufacturer"
				+ "("
				+ "id int NOT NULL PRIMARY KEY,"
				+ "name character varying(150)"
				+ ");";
		
		return SQLQuery;
	}
	
	public String createMedicalSupplyTable() {
		
		String SQLQuery = "CREATE TABLE IF NOT EXISTS MedicalSupply"
				+ "("
				+ "id int NOT NULL PRIMARY KEY,"
				+ "name character varying(150),"
				+ "completeName character varying(150),"
				+ "manufacturersIDs Integer[]"
				+ ");";
		
		return SQLQuery;
	}
	
	public String createMedicalSupply_manufacturerTable() {
		
		String SQLQuery = "CREATE TABLE IF NOT EXISTS medicalsupply_manufacturer"
				+ "("
				+ "medicalSupplyID int,"
				+ "manufacturerID int,"
				+ "PRIMARY KEY(medicalSupplyID, manufacturerID)"
				+ ");";
		
		return SQLQuery;
	}
	
	
	public String createTimeDimTable_populate() {
		
		String SQLQueryCreate = "	CREATE TABLE IF NOT EXISTS time_dim"
				+ "("
				+ "date date,"
				+ "year numeric,"
				+ "month numeric,"
				+ "monthname character(11),"
				+ "day numeric,"
				+ "dayofyear numeric,"
				+ "weekdayname character varying(11),"
				+ "calendarweek numeric,\r\n"
				+ "formatteddate character(15),"
				+ "quartal character(2),"
				+ "yearquartal character(7),"
				+ "yearmonth character(7),"
				+ "yearcalendarweek character(7),"
				+ "CONSTRAINT date PRIMARY KEY (date)"
				+ ");";
		String SQLQUeryPopulate = "insert into time_dim"
				+ " SELECT"
				+ "	datum as Date,"
				+ "	extract(year from datum) AS Year,"
				+ "	extract(month from datum) AS Month,"
				+ "	to_char(datum, 'Month') AS MonthName,"
				+ "	extract(day from datum) AS Day,"
				+ "	extract(doy from datum) AS DayOfYear,"
				+ "	to_char(datum, 'Day') AS WeekdayName,"
				+ "	extract(week from datum) AS CalendarWeek,"
				+ "	to_char(datum, 'dd. mm. yyyy') AS FormattedDate,"
				+ "	'Q' || to_char(datum, 'Q') AS Quartal,"
				+ "	to_char(datum, 'yyyy/\"Q\"Q') AS YearQuartal,"
				+ "	to_char(datum, 'yyyy/mm') AS YearMonth,"
				+ "	to_char(datum, 'iyyy/IW') AS YearCalendarWeek"
				+ " FROM ("
				+ "	SELECT '2010-01-01'::DATE + sequence.day AS datum"
				+ "	FROM generate_series(0,7307) AS sequence(day)" //I do from 2010 to 2030, there are 8 leap years, means 365 * 20 + 8 records
				+ "	GROUP BY sequence.day"
				+ "     ) DQ"
				+ " order by 1"
				+ " ON CONFLICT DO NOTHING;";
		
		
		String SQLQuery = SQLQueryCreate+SQLQUeryPopulate;
		return SQLQuery;
	}
	
	
	public void DoitInDB(String SQLQuery, String tz_DBurl, String tz_DBLogin, String tz_DBPassword) {
		Connection cn = null;
		Statement st = null;	
		System.out.println("...tables and MV in DB...");
		try {
			//Step 1 : loading driver
			Class.forName("org.postgresql.Driver");
			//Step 2 : Connection
			cn = DriverManager.getConnection(tz_DBurl, tz_DBLogin, tz_DBPassword);
			//Step 3 : creation statement
			st = cn.createStatement();
			
			// Step 5 execution SQLQuery
			st.executeUpdate(SQLQuery);
			System.out.println("Task done successfully!");
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
	
}
