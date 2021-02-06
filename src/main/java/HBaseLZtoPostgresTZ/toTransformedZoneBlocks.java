package HBaseLZtoPostgresTZ;

import java.io.FileInputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.ParseException;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.Vector;

import org.json.JSONArray;
import org.json.JSONObject;

import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.Response;

public class toTransformedZoneBlocks {
	
	public toTransformedZoneBlocks() {}
	private String ctrlPath;
	private boolean useMultiThreading;
	private String tz_DBurl, tz_DBusr, tz_DBpwd, HBaseTableURL, completeRowKey, colFam, tablesNamesCSV, tablesMultiThread, threadControl;
	long tz_thisExtractionTS;
	
	
	//initial func
	public void exportDataToTransformedZone() throws IOException, InterruptedException, ParseException {
		//if useMultiThreading = false then tablesMultiThread should be overwritten to "";
		if(!useMultiThreading)tablesMultiThread ="";//any multi Threading is used
		String[] tables = tablesNamesCSV.split(",");
		for(int i = 0; i< tables.length; ++i) {
			String SQLQuery = "";
			if(tables[i].equals("AdministrationUnit")) {
				fullextractAdminUnitmultiThreading orgUnitsExtr = new fullextractAdminUnitmultiThreading();
				orgUnitsExtr.setProperties(ctrlPath);
				if(tablesMultiThread.contains(tables[i])) SQLQuery= orgUnitsExtr.getSQLQueryAdminUnit();//implemented a part since it is not directly coming from WIMEDS-Table-Data
				else SQLQuery = orgUnitsExtr.getSQLQueryAdminUnitnoMT();
			}
			else { //are tables from HBase WIMEDS-Data-Table
				System.out.println("extracting "+tables[i]+" data");
				JSONArray jsonA = getDataFromHBase(tables[i]);
				String data = jsonA.toString();
				System.out.println("el tamany total de les dades de "+tables[i]+": "+ jsonA.length());
				System.out.println(tablesMultiThread);
				//ara aqui ja tinc tot el JSONArray de una taula en concret
				//call function in order to generate SQLQueries
				/*depenen de quina taula es tracti faré multithreading o no
			per generar les queries utilitzant multithreading només ho tinc fet per AdministrationUnit i Request
			per les altres de moment NO s'utilitza multithreading i es generaran les SQL queries en una funcio d'aquest fitxer
			TINC en tablesMultiThread a control.properties les taules que vull utilitzar java multithreading
			HA D'ESTAR IMPLEMENTAT...(ara només hi tinc=AdministrationUnit,Request)
				 */
				//haig de cridar a un generador de queries o a un altre
				//look at useMultiThreading, if true means we have to look at which tables do we want to apply it
				if(tables[i].equals("Request")) {
					if(tablesMultiThread.contains(tables[i]))SQLQuery=getRequestQueryMT(data, tables[i]);
					else SQLQuery = getRequestQuery(data);
				}
				else if(tables[i].equals("Manufacturer"))SQLQuery=getManufacturerQuery(data);
				else if(tables[i].equals("Disease"))SQLQuery=getDiseaseQuery(data);
				else if(tables[i].equals("MedicalSupply"))SQLQuery=getMedicalSupplyQuery(data);
				else if(tables[i].equals("RequestStatus"))SQLQuery=getRequestStatusQuery(data);
				else if(tables[i].equals("ShipmentR"))SQLQuery=getShipmentRQuery(data);
				//else if(...) add the same for the other needed tables
			}
			//call function LoadInDB
			LoadInDB(SQLQuery, tz_DBurl, tz_DBusr, tz_DBpwd);
		}
	}
	
	public JSONArray getDataFromHBase(String colQuali) throws IOException{
		
		//la completeRowKey ja la tinc com a global var, aniré sumant tots els partialJSONArray
		//els partialJSONArray els aniré agafant de una funció que faci la crida respectiva a la HBase REST API amb la completeRowKey+"$numBlock"
		//aqui doncs es on anirá implementat si queden més blocks a mirar o no.
		boolean finished = false;
		int blockNum = 0;
		JSONArray fullContent = new JSONArray();
		while(finished==false) {
			//call getDataBlocks to get the next data block
			String partialContent = getDataBlocks(completeRowKey, colQuali, blockNum);
			if(partialContent=="")finished=true;
			else {

				//JSONObject JSONObjectContent = new JSONObject(partialContent);
				JSONArray JSONArrayRows = new JSONArray(partialContent);
				System.out.println("ssssssize: "+ JSONArrayRows.length());
				//concatenate arrays
				for(int i = 0; i < JSONArrayRows.length(); ++i) {
					fullContent.put(JSONArrayRows.getJSONObject(i));
					if(i==0)System.out.println(JSONArrayRows.getJSONObject(i));
				}
				System.out.println("tamany "+ fullContent.length());
				blockNum += 1;
			}
		}
		return fullContent;//string with all objects
	}
	
	public String getDataBlocks(String completeRowKey ,String colQuali, int blockNum) throws IOException{
		//colqualifier (e.g., ShipmentR)
		//columnFamily = Data (should be parametrized) I have it as a global var
		//rowKey used to get data from block with identifier numBlock
		String crk = this.completeRowKey+"$"+blockNum;
		//do HBase REST API call to get data from rowKey crk
		String url = HBaseTableURL+'/'+crk+'/'+colFam+':'+colQuali; //url to be used for the REST API call
		System.out.println(url);
		String content="";

		OkHttpClient client = new OkHttpClient().newBuilder()
				.build();
		Request request = new Request.Builder()
				.url("http://who-dev.essi.upc.edu:60000/WIMEDS-Table-Data/"+crk+"/"+colFam+":"+colQuali) //use parametrized url
				.method("GET", null)
				.build();
		Response response = client.newCall(request).execute();

		String msg = response.message();
		Integer statusCode = response.code();

		content = response.body().string();
		response.body().close();
		System.out.println("this is the msg: "+ msg);
		System.out.println("this is the statusCode: "+ statusCode);
		
		if(statusCode==404)content="";
		
		return content;
	}
	
	
	//using multithreading to generate SQLQuery
	public String getRequestQueryMT(String data, String whichData) throws InterruptedException, IOException, ParseException {
		String SQLQuery = "TRUNCATE TABLE "+whichData+";\n"; //I should not use TRUNCATE if i want rows with same id for historical data in the analysis
		JSONArray content = new JSONArray(data);
		int THREADS;
		if(threadControl.equals("availableProcessors")) THREADS =  Runtime.getRuntime().availableProcessors();
		else THREADS = Integer.parseInt(threadControl);
		System.out.println("using "+THREADS+" threads, generating queries");
		//call ParallelPartialQueryGenerator
		ParallelPartialQueryGenerator partialQueryGen = new ParallelPartialQueryGenerator(THREADS);
		//put workers to work and get the result query 
		SQLQuery += partialQueryGen.partialQueriesSum(content, whichData);
		//System.out.println(SQLQuery);
		return SQLQuery;
	}

	//not using multiThreading
	public String getRequestQuery(String requestData) {
		System.out.println("...generating SQLQuery from HBase data...");
		System.out.println("WITHOUT making use of multithreading");
		String SQLQuery = "TRUNCATE TABLE requests;\n";
		//see if we have more or one Request in the response, if there is one, create an array an insert it there
		JSONArray jsonArrayReq = new JSONArray();
		jsonArrayReq = new JSONArray(requestData);
		
		for(int i = 0;i<jsonArrayReq.length(); ++i) {
			int id, diseaseID, medicalSupplyID, requestStatus, weightInKg, age;
			String countryID, healthFacilityID, requestDateString, healthFacility, phase;
			
			JSONObject requestJSONObj = jsonArrayReq.getJSONObject(i);
			id = requestJSONObj.getInt("persistenceId");
			countryID = requestJSONObj.getJSONObject("countryAdminUnit").getString("id");
			diseaseID = requestJSONObj.getJSONObject("disease").getInt("persistenceId");
			medicalSupplyID = requestJSONObj.getJSONObject("medicalSupply").getInt("persistenceId");
			requestStatus = requestJSONObj.getJSONObject("requestStatus").getInt("persistenceId");
			//healthFacilityID = requestJSONObj.getString("healthFacilityPid");
			requestDateString = requestJSONObj.getString("requestDate");
			requestDateString = requestDateString.substring(0,10);
			healthFacility = requestJSONObj.getString("currentHealthFacilityName");
			weightInKg = requestJSONObj.getInt("weightInKg");
			age = requestJSONObj.getInt("age");
			phase = requestJSONObj.getString("phase");
	
			SQLQuery += "INSERT INTO requests VALUES ("
					+ id + ",'"
					+ countryID + "','"
					+ healthFacility + "',"
					+ diseaseID + ","
					+ medicalSupplyID + ","
					+ requestStatus + ",'"
					+ requestDateString + "',"
					+ age + ","
					+ weightInKg + ",'"
					+ phase +"');\n";
		}
		return SQLQuery;
	}
	
	//not using multithreading to generate SQLQuery
	public String getManufacturerQuery(String ManufacturerData) {
		System.out.println("...generating SQLQuery from HBase data...");
		String SQLQuery = "TRUNCATE TABLE Manufacturer;\n";
		
		JSONArray jsonArrayDisease = new JSONArray(ManufacturerData);
		for(int i = 0;i<jsonArrayDisease.length(); ++i) {
			int id;
			String name;
			
			JSONObject DiseaseJSONObj = jsonArrayDisease.getJSONObject(i);
			id = DiseaseJSONObj.getInt("persistenceId");
			name = DiseaseJSONObj.getString("name");

			SQLQuery += "INSERT INTO Manufacturer VALUES ("
					+ id + ",'"
					+ name + "');\n";
		}
		return SQLQuery;
	}
	
	
	public String getDiseaseQuery(String DiseaseData) {
		System.out.println("...generating SQLQuery from HBase data...");
		String SQLQuery = "TRUNCATE TABLE Disease;\n";
		
		JSONArray jsonArrayDisease = new JSONArray(DiseaseData);
		for(int i = 0;i<jsonArrayDisease.length(); ++i) {
			int id;
			List<Integer> medicalSuppliesIDs = new ArrayList<Integer>();
			String name, completeName;
			
			
			JSONObject DiseaseJSONObj = jsonArrayDisease.getJSONObject(i);
			id = DiseaseJSONObj.getInt("persistenceId");
			name = DiseaseJSONObj.getString("name");
			completeName = DiseaseJSONObj.getString("completeName");
	
			//el medicalSupply => null,1,*
			Object aObj = DiseaseJSONObj.get("medicalSupplies");
			if (aObj instanceof JSONArray) {
				JSONArray medicalSuppliesJSONArray = new JSONArray();
				medicalSuppliesJSONArray = DiseaseJSONObj.getJSONArray("medicalSupplies");
				for(int j = 0; j < medicalSuppliesJSONArray.length(); ++j) {
					JSONObject medicalSuppliesJSONObj = medicalSuppliesJSONArray.getJSONObject(j);
					medicalSuppliesIDs.add(medicalSuppliesJSONObj.getInt("persistenceId"));
				}
			}

	
			SQLQuery += "INSERT INTO Disease VALUES ("
					+ id + ",'"
					+ name + "','"
					+ completeName + "',ARRAY"
					+ medicalSuppliesIDs + ");\n";
		}
		return SQLQuery;
	}
	
	public String getMedicalSupplyQuery(String medicalSupplyData) {
		System.out.println("...generating SQLQuery from HBase data...");
		String SQLQuery = "TRUNCATE TABLE MedicalSupply;\n";
		
		JSONArray jsonArrayMedicalSupply = new JSONArray(medicalSupplyData);
		
		for(int i = 0;i<jsonArrayMedicalSupply.length(); ++i) {
			int id;
			List<Integer> manufacturersIDs = new ArrayList<Integer>();
			String name, completeName;
			
			
			JSONObject MedicalSupplyJSONObj = jsonArrayMedicalSupply.getJSONObject(i);
			id = MedicalSupplyJSONObj.getInt("persistenceId");
			name = MedicalSupplyJSONObj.getString("name");
			completeName = MedicalSupplyJSONObj.getString("completeName");
	
			//el manufacturer => null,1,*
			Object aObj = MedicalSupplyJSONObj.get("manufacturers");
			if (aObj instanceof JSONArray) {
				JSONArray manufacturersJSONArray = new JSONArray();
				manufacturersJSONArray = MedicalSupplyJSONObj.getJSONArray("manufacturers");
				for(int j = 0; j < manufacturersJSONArray.length(); ++j) {
					JSONObject medicalSuppliesJSONObj = manufacturersJSONArray.getJSONObject(j);
					manufacturersIDs.add(medicalSuppliesJSONObj.getInt("persistenceId"));
				}
			}
			
			if(manufacturersIDs.isEmpty()) {
				Integer manufacturerEmpty = null;
				SQLQuery += "INSERT INTO MedicalSupply VALUES ("
						+ id + ",'"
						+ name + "','"
						+ completeName + "',"
						+ manufacturerEmpty + ");\n";
			}
			else {
				SQLQuery += "INSERT INTO MedicalSupply VALUES ("
						+ id + ",'"
						+ name + "','"
						+ completeName + "',ARRAY"
						+ manufacturersIDs + ");\n";
			}
		}
		return SQLQuery;
	}
	
	public String getRequestStatusQuery(String RequestStatusData) {
		System.out.println("...generating SQLQuery from HBase data...");
		String SQLQuery = "TRUNCATE TABLE RequestStatus;\n";
		
		JSONArray jsonArrayDisease = new JSONArray(RequestStatusData);
		for(int i = 0;i<jsonArrayDisease.length(); ++i) {
			int id;
			String name;
			
			JSONObject DiseaseJSONObj = jsonArrayDisease.getJSONObject(i);
			id = DiseaseJSONObj.getInt("persistenceId");
			name = DiseaseJSONObj.getString("name");

			SQLQuery += "INSERT INTO RequestStatus VALUES ("
					+ id + ",'"
					+ name + "');\n";
		}
		return SQLQuery;
	}
	
	public String getShipmentRQuery(String shipmentRdata) {
		System.out.println("...generating SQLQuery from HBase data...");
		String SQLQuery = "TRUNCATE TABLE shipmentR;\n";
		
		JSONArray jsonArrayship = new JSONArray(shipmentRdata);
		
		for(int i = 0;i<jsonArrayship.length(); ++i) {
			int id, quantity, requestID;
			Integer quantityReceived = null;//int can not be null
			String medicalSupplyName, shipmentStatus, shipmentDateCreationString;
			
			
			JSONObject shipmentJSONObj = jsonArrayship.getJSONObject(i);
			id = shipmentJSONObj.getInt("persistenceId");
			quantity = shipmentJSONObj.getInt("quantity");
	
			Object aObj = shipmentJSONObj.get("quantityReceived");
			if (aObj instanceof Integer) {
				quantityReceived = shipmentJSONObj.getInt("quantityReceived");
			}
			medicalSupplyName = shipmentJSONObj.getJSONObject("medicalSupply").getString("name");
			requestID = shipmentJSONObj.getJSONObject("request").getInt("persistenceId");
			shipmentDateCreationString = shipmentJSONObj.getString("dateOfCreation");
			shipmentDateCreationString = shipmentDateCreationString.substring(0,10);
			shipmentStatus = shipmentJSONObj.getString("shipmentStatus");
	
			SQLQuery += "INSERT INTO shipmentR VALUES ("
					+ id + ","
					+ quantity + ","
					+ quantityReceived + ",'"
					+ medicalSupplyName + "',"
					+ requestID + ",'"
					+ shipmentStatus + "','"
					+ shipmentDateCreationString +"');\n";
		}
		
		
		return SQLQuery;
	}
	
	
	
	public void LoadInDB(String SQLQuery, String tz_DBurl, String tz_DBLogin, String tz_DBPassword) {
		Connection cn = null;
		Statement st = null;	
		System.out.println("...inserting data to DB...");
		try {
			//Step 1 : loading driver
			Class.forName("org.postgresql.Driver");
			//Step 2 : Connection
			cn = DriverManager.getConnection(tz_DBurl, tz_DBLogin, tz_DBPassword);
			//Step 3 : creation statement
			st = cn.createStatement();
			
			// Step 5 execution SQLQuery
			st.executeUpdate(SQLQuery);
			System.out.println("Loading done successfully!");
			
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
	
	
	public void setProperties(String ctrlPath) throws IOException{
		
		//////////
		Properties props = new Properties();
        FileInputStream in = new FileInputStream(ctrlPath);

        props.load(in);
		this.setDBurl(props.getProperty("tz_DBurl"));
		this.setDBusr(props.getProperty("tz_DBusr"));
		this.setDBpwd(props.getProperty("tz_DBpwd"));
		this.setHBaseTableURL(props.getProperty("urlWIMEDSdataTable"));
		this.setthiscompleteRowKey(props.getProperty("completeRowKey"));
		this.setColFam(props.getProperty("family1"));
		this.setTZtablesNames(props.getProperty("TZtables"));
		this.setUseMultiThreading(props.getProperty("useMultiThreading"));
		this.setTablesNamesMT(props.getProperty("tablesMultiThread"));
		this.setThreadControl(props.getProperty("nThreads"));
		this.ctrlPath=ctrlPath;
        in.close();
		/////////
	}
	
	
	//getters and setters
	public void setDBurl(String str)     {this.tz_DBurl = str;}
	public String getDBurl()             {return tz_DBurl;}
	public void setDBusr(String str)     {this.tz_DBusr = str;}
	public String getDBusr()             {return tz_DBusr;}
	public void setDBpwd(String str)     {this.tz_DBpwd = str;}
	public String getDBpwd()             {return tz_DBpwd;}

	public void setHBaseTableURL(String str)     {this.HBaseTableURL = str;}
	public String getHBaseTableURL()             {return HBaseTableURL;}
	
	public void setColFam(String str)     {this.colFam = str;}
	public String getColFam()             {return colFam;}
	
	public void setthiscompleteRowKey(String str)     {this.completeRowKey = str;}
	public String getthiscompleteRowKey() {return completeRowKey;}
	
	public String getTZtablesNames() {return this.tablesNamesCSV;}
	public void setTZtablesNames(String tablesNamesCSV) {this.tablesNamesCSV = tablesNamesCSV;}
	
	public String getTablesNamesMT() {return this.tablesMultiThread;}
	public void setTablesNamesMT(String tablesMultiThread) {this.tablesMultiThread = tablesMultiThread;}
	
	public String getThreadControl() {return this.threadControl;}
	public void setThreadControl(String s) {this.threadControl = s;}
	
	public boolean getUseMultiThreading() {return this.useMultiThreading;}
	public void setUseMultiThreading(String s) {
		if(s.equals("true"))this.useMultiThreading = true;
		else this.useMultiThreading=false;
	}
	
	
	
	
	
}
