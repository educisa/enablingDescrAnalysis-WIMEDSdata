package LandingZoneToFormattedZone;

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

public class PostgresSqlFormattedZone {
	
	String DBurl, DBuser, DBpwd;

	public PostgresSqlFormattedZone(String url, String user, String pwd) {
		this.DBurl = url;
		this.DBuser = user;
		this.DBpwd = pwd;
	}
	
	
	public void createTables() {
		
		String SQLQueryAdminUnit = createAdminUnitTable();
		String SQLQueryShipmentR = createShipmentRTable();
		String SQLQueryDisease = createDiseaseTable();
		String SQLQUeryRequestStatus = createRequestStatusTable();
		String SQLQUeryManufacturer = createManufacturerTable();// Requests still do not have FK to manufacturer. Problem coming from data schema from WIMEDS business data
		String SQLQueryMedicalSupply = createMedicalSupplyTable();
		//String SQLQueryMedicalSupply_manufacturer = createMedicalSupply_manufacturerTable(); changed to Materialized View,in the future may be useful when having FK manufacturerid inside Request table
		//String SQLQueryTimeDimTable = createTimeDimTable_populate();//not used since Tableau do this work for us
		String SQLQueryReq = createRequestsTable();
		
		String SQLQuery = SQLQueryAdminUnit+SQLQueryDisease+SQLQUeryRequestStatus
				+SQLQueryMedicalSupply+SQLQUeryManufacturer+SQLQueryReq+SQLQueryShipmentR;//order for FK
		
		DoitInDB(SQLQuery, DBurl, DBuser, DBpwd);
	}
	
	//materialized views to be used for OLAP tool Tableau
	public void createMaterializedViews() {
		
		String SQLQueryMV0 ="CREATE MATERIALIZED VIEW if not exists request_broadOverview AS "
				+ "SELECT distinct r.id as requestID, r.requestdate as requestDate, au.shortname as Country, au1.shortname as WHORegion, d.name as disease, r.diseasephase as phase, r.healthFacility"
				+ " FROM request r, administrationunit au, disease d, administrationunit au1"
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
				+ " from request r LEFT JOIN shipmentr s on r.id=s.requestid "
				+ "left join disease d on d.id=r.diseaseid "
				+ "left join administrationunit au on au.id=r.countryid "
				+ "left join administrationunit au1 on au.parentid=au1.id "
				+ "left join requeststatus rs on r.requeststatusid=rs.id "
				+ "left join medicalsupply ms on r.medicalsupplyid=ms.id;";
		
		String SQLQueryMV2 = "CREATE MATERIALIZED VIEW if not exists MailingOffice_RequestsSummary AS "
				+ "SELECT distinct r.id as requestID, r.requestdate as requestDate, rs.name as requeststatus, au.shortname as Country, au1.shortname as WHORegion, ms.name as medicalSupplyName, ms.completename as medSupCompleteName,"
				+ "r.quantity as askedQuantity, s.quantityreceived, s.couriername, s.trackingnumber, s.edd, s.shippeddate, s.receptiondate, s.healthfacilityname as healthfacility_Requester,"
				+ "abs(s.receptiondate :: date - s.shippeddate :: date) as shipmentDuration_days"
				+ " FROM request r, requeststatus rs, administrationunit au, administrationunit au1, medicalsupply ms, shipmentr s"
				+ " WHERE r.requeststatusid = rs.id and r.id=s.requestid and r.countryid=au.id and au.parentid=au1.id and r.medicalsupplyid=ms.id"
				+ " order by r.id, r.requestdate, au.shortname, s.trackingnumber;";
		
		String SQLQueryMV3="create materialized view if not exists medicalsupplies_manufacturers as "
				+ "SELECT DISTINCT"
				+ "    medicalsupply.id as medicalSupplyID,"
				+ "    manufacturer.id as manufacturerID,"
				+ "	medicalsupply.name as medicalsupply,"
				+ "	manufacturer.name as manufacturer"
				+ " FROM"
				+ "    medicalsupply, manufacturer"
				+ " where manufacturer.id = ANY (medicalsupply.manufacturersids)"
				+ " order by medicalSupplyID, manufacturerID;";
		
		
		String SQLQueryMV = SQLQueryMV0+SQLQueryMV1+SQLQueryMV2+SQLQueryMV3;
		DoitInDB(SQLQueryMV, DBurl, DBuser, DBpwd);
	}
	
	//to be called each time a transformed zone update is done, in order to get updated data in Tableau
	public void refreshMaterializedViews() {
		String refreshMV0 = "REFRESH MATERIALIZED VIEW request_broadOverview;";
		String refreshMV1 = "REFRESH MATERIALIZED VIEW MailingOffice_RequestsSummary;";
		String refreshMV2 = "REFRESH MATERIALIZED VIEW described_requestsoverview;";
		String refresgMV3 = "REFRESH MATERIALIZED VIEW medicalsupply_manufacturerto;";
		
		
		String SQLQUeryRefreshMV = refreshMV0+refreshMV1+refreshMV2;
		DoitInDB(SQLQUeryRefreshMV, DBurl, DBuser, DBpwd);
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
		String SQLQuery = "CREATE TABLE IF NOT EXISTS Request("
				+ "id int NOT NULL PRIMARY KEY,"
				+ "countryid character varying(11)," 
				+ "healthfacility character varying(150),"
				+ "diseaseid integer," 
				+ "medicalsupplyid integer," 
				+ "requeststatusid integer," 
				+ "requestdate date,"
				+ "patientAge integer,"
				+ "patientWeightInKg integer,"
				+ "diseasePhase character varying(22),"
				+ "transmissionWay character varying(22),"
				+ "quantity integer"
				+ ");";
		
		return SQLQuery;
	}
	
	public String createShipmentRTable() {
		
		String SQLQuery = "CREATE TABLE IF NOT EXISTS shipmentR("
				+ "id int NOT NULL PRIMARY KEY,"
				+ "shipmentcreationdate date,"
				+ "shipmentstatus character varying(150),"
				+ "EDD date,"
				+ "shippedDate date,"
				+ "receptionDate date,"
				+ "requestid integer,"
				+ "medicalsupplyid character varying(150),"
				+ "healthFacilityName character varying(200),"
				+ "quantity integer,"
				+ "quantityreceived integer,"
				+ "courierName character varying(10),"
				+ "trackingnumber integer"
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
	
	/*changed to MV
	public String createMedicalSupply_manufacturerTable() {
		
		String SQLQuery = "CREATE TABLE IF NOT EXISTS medicalsupply_manufacturer"
				+ "("
				+ "medicalSupplyID int,"
				+ "manufacturerID int,"
				+ "PRIMARY KEY(medicalSupplyID, manufacturerID)"
				+ ");";
		
		return SQLQuery;
	}*/
	
	/*changed to MV, it will be refreshed
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
	}*/
	
	//not used. Tableau give us all the needed date functionalities.
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
