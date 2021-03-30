package HBaseLZtoPostgresTZ;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.PrintStream;
import java.io.UnsupportedEncodingException;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.Properties;

import org.json.JSONArray;
import org.json.JSONObject;

import okhttp3.Headers;
import okhttp3.MediaType;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.RequestBody;
import okhttp3.Response;
import okhttp3.ResponseBody;

public class landingZoneToTransformedZone {
	
	public landingZoneToTransformedZone() {}
	private String ctrlPath;
	private boolean useMultiThreading;
	private String tz_DBurl, tz_DBusr, tz_DBpwd, HBaseTableURL, completeRowKey, colFam, tablesNamesCSV, tablesMultiThread, threadControl,
	bodyScanner, scannerTRbatch;
	long tz_thisExtractionTS;
	String thisWIMEDSextractionTS;
	
	
	
	
	public String getTRScannerID() throws IOException {

		try {
			OkHttpClient client = new OkHttpClient().newBuilder()
					.build();
			MediaType mediaType = MediaType.parse("text/xml");
			RequestBody body = RequestBody.create(bodyScanner,mediaType);
			Request request = new Request.Builder()
					.url(HBaseTableURL+"/scanner")
					.method("PUT", body)
					.addHeader("Accept", "text/xml")
					.addHeader("Content-Type", "text/xml")
					.build();
			Response response = client.newCall(request).execute();

			Headers headers = response.headers();
			String scanner_id = headers.value(0);
			System.out.println("scanner_id: " + scanner_id.substring(scanner_id.lastIndexOf("/") + 1)); 
			scanner_id = scanner_id.substring(scanner_id.lastIndexOf("/") + 1);
			String msg = response.message();
			Integer statusCode = response.code();

			response.body().close();

			return scanner_id;
		}
		catch(Exception e) {System.out.println("Connection error! \nExecution stopped!");
		System.exit(1);
		return null;}
		
	}
	
	
	public JSONArray getLandingZoneWIMEDSdata() throws IOException {
		
		String scanner_id = getTRScannerID();
		
		Boolean finishScan=false;
		int calls = 0;
		
		JSONArray JSONArrayTotalRows = new JSONArray();
		while(finishScan.equals(false)) {
			
			try {
				OkHttpClient client = new OkHttpClient().newBuilder()
						.build();
				Request request = new Request.Builder()
						.url(HBaseTableURL+"/scanner/"+scanner_id)
						.method("GET", null)
						.addHeader("Accept", "application/json")
						.build();
				Response response = client.newCall(request).execute();
				ResponseBody body = response.body();
				++calls;
				String content = response.body().string();
				Headers headers = response.headers();

				String msg = response.message();
				Integer statusCode = response.code();

				//means scanner has finished
				if(statusCode == 204) {
					response.body().close();
					finishScan=true;
					System.out.println("...update scan finished...");
				}

				else {
					JSONObject JSONObjectContent = new JSONObject(content);
					
					JSONArray JSONArrayRows = (JSONArray) JSONObjectContent.get("Row");
					JSONArrayTotalRows.put(JSONArrayRows);
					System.out.println("this is the lenght of the JSONArrayRow :"+JSONArrayRows.length());
					//for each rows retrieve the values inside each of the columns
				/*
					for (int i = 0; i < JSONArrayRows.length(); i++) {

						for(int j = 0; j < tables.length; ++j) {
							System.out.println(i+"-"+j+"->"+tables[j]);
							JSONObject RowObj = JSONArrayRows.getJSONObject(i);
							JSONArray encodedJSONArray = RowObj.getJSONArray("Cell");
							//cada JSONObject(i) es una column, per exemple JSONObject(0) son les diseases
							//JSONObject(1) son els manufacturers
							JSONObject encodedJSONObject = encodedJSONArray.getJSONObject(i);
							String encodedValue = encodedJSONObject.getString("$");
							//decode the encodedValue
							byte[] dataBytes = Base64.getDecoder().decode(encodedValue);
							String decodedValue="";
							try {
								decodedValue = new String(dataBytes, StandardCharsets.UTF_8.name());
								if(j==1)System.out.println(decodedValue);

							} catch (UnsupportedEncodingException e) {
								e.printStackTrace();
							}
							
							String SQLQuery = "";
							if(tables[j].equals("Request")) {
								//if(tablesMultiThread.contains(tables[i]))SQLQuery=getRequestQueryMT(decodedValue, tables[i]);
								//else SQLQuery = getRequestQuery(decodedValue);
								//SQLQuery = getRequestQuery(decodedValue);
								//LoadInDB(SQLQuery, tz_DBurl, tz_DBusr, tz_DBpwd);
							}
							
							else if(tables[j].equals("Manufacturer")) {
								System.out.println("Manufacturerrrrrrrrrr");
								//SQLQuery=getManufacturerQuery(decodedValue);
							}
							else if(tables[j].equals("Disease")) {
								System.out.println("Disease");
								//SQLQuery=getDiseaseQuery(decodedValue);
							}
							else if(tables[j].equals("MedicalSupply")) {
								//SQLQuery=getMedicalSupplyQuery(decodedValue);
							}
							else if(tables[j].equals("RequestStatus")) {
								//SQLQuery=getRequestStatusQuery(decodedValue);
							}
							else if(tables[j].equals("ShipmentR")) {
								//SQLQuery=getShipmentRQuery(decodedValue);
							}

						}
					}*/
					/*
					PrintStream fileStream = new PrintStream("outputLZtoTZ.txt");
					System.setOut(fileStream);
					System.out.println(JSONObjectContent);
					*/
				}
				
			}
			catch(Exception e) {System.out.println("Connection error! \nExecution stopped!");
			System.exit(1);
			}
		}

		//System.out.println(JSONArrayTotalRows);
		//System.out.println(calls);
		return JSONArrayTotalRows;
	}
	
	public void exportDataToTZ(JSONArray JSONArrayTotalRows) throws FileNotFoundException {
		String[] tables = tablesNamesCSV.split(",");
		String encodedValue="";
		//for each rows retrieve the values inside each of the columns
		System.out.println(JSONArrayTotalRows.length());
		for (int i = 0; i < JSONArrayTotalRows.length(); i++) {
			JSONArray RowArray = JSONArrayTotalRows.getJSONArray(i);
			JSONObject RowObj = RowArray.getJSONObject(i);
			JSONArray encodedJSONArray = RowObj.getJSONArray("Cell");
			for(int j = 0; j < tables.length; ++j) {
				System.out.println(i+"-"+j);
				//cada JSONObject(i) es una column, per exemple JSONObject(0) son les diseases
				//JSONObject(1) son els manufacturers
				JSONObject encodedJSONObject = encodedJSONArray.getJSONObject(j);
				encodedValue = encodedJSONObject.getString("$");
				//decode the encodedValue
				byte[] dataBytes = Base64.getDecoder().decode(encodedValue);
				String decodedValue="";
				try {
					decodedValue = new String(dataBytes, StandardCharsets.UTF_8.name());
					if(j==1)System.out.println(decodedValue);

				} catch (UnsupportedEncodingException e) {
					e.printStackTrace();
				}

				String SQLQuery = "";
				if(tables[j].equals("Request")) {
					//if(tablesMultiThread.contains(tables[i]))SQLQuery=getRequestQueryMT(decodedValue, tables[i]);
					//else SQLQuery = getRequestQuery(decodedValue);
					SQLQuery = getRequestQuery(decodedValue);
					LoadInDB(SQLQuery, tz_DBurl, tz_DBusr, tz_DBpwd);
				}
				
				else if(tables[j].equals("Manufacturer")) {
					System.out.println("Manufacturerrrrrrrrrr");
					//SQLQuery=getManufacturerQuery(decodedValue);
				}
				else if(tables[j].equals("Disease")) {
					System.out.println("Disease");
					//SQLQuery=getDiseaseQuery(decodedValue);
				}
				else if(tables[j].equals("MedicalSupply")) {
					//SQLQuery=getMedicalSupplyQuery(decodedValue);
				}
				else if(tables[j].equals("RequestStatus")) {
					//SQLQuery=getRequestStatusQuery(decodedValue);
				}
				else if(tables[j].equals("ShipmentR")) {
					//SQLQuery=getShipmentRQuery(decodedValue);
				}

			}

		}
		/*
		PrintStream fileStream = new PrintStream("outputLZtoTZ.txt");
		System.setOut(fileStream);
		System.out.println(JSONObjectContent);
		*/
	}
	
	//not using multiThreading
		public String getRequestQuery(String requestData) {
			System.out.println("...generating SQLQuery from HBase data...");
			System.out.println("WITHOUT making use of multithreading");
			//String SQLQuery = "TRUNCATE TABLE requests;\n";
			String SQLQuery = "";
			//see if we have more or one Request in the response, if there is one, create an array an insert it there
			JSONArray jsonArrayReq = new JSONArray();
			jsonArrayReq = new JSONArray(requestData);
			
			for(int i = 0;i<jsonArrayReq.length(); ++i) {
				int id, diseaseID, medicalSupplyID, requestStatus, weightInKg, age, quantity;
				String countryID, requestDateString, healthFacility, phase, transmissionWay;
				
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
				transmissionWay = requestJSONObj.getString("transmissionWay");
				quantity = requestJSONObj.getInt("quantity");
				
				SQLQuery += "INSERT INTO request VALUES ("
						+ id + ",'"
						+ countryID + "','"
						+ healthFacility + "',"
						+ diseaseID + ","
						+ medicalSupplyID + ","
						+ requestStatus + ",'"
						+ requestDateString + "',"
						+ age + ","
						+ weightInKg + ",'"
						+ phase +"','"
						+ transmissionWay +"',"
						+ quantity +") ON CONFLICT ON CONSTRAINT request_pkey"
								+ " DO"
								+ " UPDATE"
								+ " SET"
								+" countryID='"+countryID+"',"
								+" healthFacility='"+healthFacility+"',"
								+" diseaseID="+diseaseID+","
								+" medicalSupplyID="+medicalSupplyID+","
								+" requestStatusid="+requestStatus+","
								+" requestDate='"+requestDateString+"',"
								+" patientage="+age+","
								+" patientweightinkg="+weightInKg+","
								+" diseasephase='"+phase+"',"
								+" transmissionway='"+transmissionWay+"',"
								+" quantity="+quantity+";\n";
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
				boolean received = false;
				int id, quantity, requestID;
				Integer quantityReceived = null;//int can not be null
				String receptionDateString = null;
				String medicalSupplyName, shipmentStatus, shipmentDateCreationString, EDDstring, healthFacilityName, shippedDateString;
				
				JSONObject shipmentJSONObj = jsonArrayship.getJSONObject(i);
				id = shipmentJSONObj.getInt("persistenceId");
				quantity = shipmentJSONObj.getInt("quantity");
				healthFacilityName = shipmentJSONObj.getString("healthFacilityName");
				Object aObj = shipmentJSONObj.get("quantityReceived");
				if (aObj instanceof Integer) {
					quantityReceived = shipmentJSONObj.getInt("quantityReceived");
				}
				aObj = shipmentJSONObj.get("receptionDate");
				if (aObj instanceof String) {
					receptionDateString = shipmentJSONObj.getString("receptionDate");
					received = true;
				}
				medicalSupplyName = shipmentJSONObj.getJSONObject("medicalSupply").getString("name");
				requestID = shipmentJSONObj.getJSONObject("request").getInt("persistenceId");
				shipmentDateCreationString = shipmentJSONObj.getString("dateOfCreation");
				shipmentDateCreationString = shipmentDateCreationString.substring(0,10);
				EDDstring = shipmentJSONObj.getString("edd");
				shippedDateString = shipmentJSONObj.getString("shippedDate");
				shipmentStatus = shipmentJSONObj.getString("shipmentStatus");

				SQLQuery += "INSERT INTO shipmentR VALUES ("
						+ id + ",'"
						+ shipmentDateCreationString + "','"
						+ shipmentStatus + "','"
						+ EDDstring + "','"
						+ shippedDateString + "',";
				if(receptionDateString == null)SQLQuery+=null + ",";
				else SQLQuery += "'"+receptionDateString + "',";
				SQLQuery += requestID + ",'"
						+ medicalSupplyName + "','"
						+ healthFacilityName + "',"
						+ quantity + ","
						+ quantityReceived + ");\n";
				System.out.println(SQLQuery);
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
	
	
	
	//delete scanner after using it
	public void deleteScanner(String scanner_id) throws IOException {
		OkHttpClient client = new OkHttpClient().newBuilder()
				  .build();
				MediaType mediaType = MediaType.parse("text/plain");
				RequestBody body = RequestBody.create("", mediaType);
				Request request = new Request.Builder()
				  .url(HBaseTableURL+"/scanner/"+scanner_id)
				  .method("DELETE", body)
				  .addHeader("Accept", "text/xml")
				  .build();
				Response response = client.newCall(request).execute();
				response.body().close();
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
		System.out.println("getting data from rowKey: "+ props.getProperty("completeRowKey"));
		this.setColFam(props.getProperty("family1"));
		this.setTZtablesNames(props.getProperty("TZtables"));
		///////////////////
		this.setthisWIMEDSextractionTS(props.getProperty("thisWIMEDSextractionTimestamp"));
		this.setScannerTRbatch(props.getProperty("LZscannerBatch"));
		///////////////////
		this.setUseMultiThreading(props.getProperty("useMultiThreading"));
		this.setTablesNamesMT(props.getProperty("tablesMultiThread"));
		this.setThreadControl(props.getProperty("nThreads"));
		this.ctrlPath=ctrlPath;
		
		
        Instant nowi = Instant.now();
		LocalDateTime now = LocalDateTime.ofInstant(nowi, ZoneId.systemDefault());
        Timestamp timestamp = Timestamp.valueOf(now);
	    long nowTS = timestamp.getTime(); //returns a long
	    
	    String endTimeCTRL = String.valueOf(nowTS);
	    String startTimeCTRL = getthisWIMEDSextractionTS().toString();
        String scannerBatchString = "<Scanner batch="+"\""+scannerTRbatch+"\"";
        String startTimeString = " startTime="+"\""+startTimeCTRL+"\"";
        String endTimeString = " endTime="+"\""+endTimeCTRL+"\""+" />";
        //body <= scannerBatchString, startTimeString, endTimeSTring
        this.bodyScanner = scannerBatchString+startTimeString+endTimeString;
        System.out.println("this is the bodyScanner: "+bodyScanner);
		
		
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
		
		//////////////////////
		public String getthisWIMEDSextractionTS() {return this.thisWIMEDSextractionTS;}
		public void setthisWIMEDSextractionTS(String ts) {this.thisWIMEDSextractionTS = ts;}
		
	    public void setScannerTRbatch(String str)     {this.scannerTRbatch = str;}
	    public String getScannerTRbatch()             {return scannerTRbatch;}
		//////////////////////
	    
		public String getThreadControl() {return this.threadControl;}
		public void setThreadControl(String s) {this.threadControl = s;}
		
		public boolean getUseMultiThreading() {return this.useMultiThreading;}
		public void setUseMultiThreading(String s) {
			if(s.equals("true"))this.useMultiThreading = true;
			else this.useMultiThreading=false;
		}

}
