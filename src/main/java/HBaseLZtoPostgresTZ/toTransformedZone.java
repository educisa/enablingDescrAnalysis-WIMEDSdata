package HBaseLZtoPostgresTZ;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.UnsupportedEncodingException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Base64;
import java.util.List;
import java.util.Properties;
import java.util.Base64.Decoder;
import java.util.Date;

import org.json.JSONArray;
import org.json.JSONObject;
import org.json.JSONTokener;
import org.json.XML;

import okhttp3.Headers;
import okhttp3.MediaType;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.RequestBody;
import okhttp3.Response;
import okhttp3.ResponseBody;

public class toTransformedZone {
	
	// 1) where i should extract organisation units from HBase and export it to postgres geographical dimension (organisation_units) in transformed_zoneDB
	// 2) same for Requests, i need to think which data from the requests is valuable to go into to the Fact table request
	
	/*1) same as organisationUnitsExtraction.java, i need just to change DB configurations 
	(set new config attr for the transformed_zoneDB on config.properties)*/
	
	public toTransformedZone() {}
	
	private String tz_DBurl, tz_DBusr, tz_DBpwd, HBaseURL, scannerBatch, completeRowKey;
	long tz_thisExtractionTS;
	
	Instant start = Instant.now();
	
	public String getScannerId() throws UnsupportedEncodingException, IOException{
		OkHttpClient client = new OkHttpClient().newBuilder()
				  .build();
				MediaType mediaType = MediaType.parse("String");
				RequestBody body = RequestBody.create(scannerBatch, mediaType);
				Request request = new Request.Builder()
				  .url(HBaseURL+"/scanner")
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
	
	//delete scanner after using it
	public void deleteScanner(String scanner_id) throws IOException {
		OkHttpClient client = new OkHttpClient().newBuilder()
				.build();
		MediaType mediaType = MediaType.parse("text/plain");
		RequestBody body = RequestBody.create("", mediaType);
		Request request = new Request.Builder()
				.url(HBaseURL+"/scanner/"+scanner_id)
				.method("DELETE", body)
				.addHeader("Accept", "text/xml")
				.build();
		Response response = client.newCall(request).execute();
		System.out.println("scanner with id: "+ scanner_id + " deleted");
		response.body().close();
	}
	
	
	//using multithreading
	public String getRequestQuery(String ctrlPath) throws InterruptedException, IOException, ParseException {
		String requestData = getRequestsFromHBase(ctrlPath);
		String SQLQuery = "TRUNCATE TABLE requests;\n";
		JSONArray content = new JSONArray(requestData);
		int THREADS =  Runtime.getRuntime().availableProcessors();
		System.out.println("using "+ THREADS+" threads, generating queries");
		//call ParallelPartialQueryGenerator
		ParallelPartialQueryGenerator partialQueryGen = new ParallelPartialQueryGenerator(THREADS);
		//put workers to work and get the result query 
		SQLQuery += partialQueryGen.partialQueriesSum(content, "Request");
		//System.out.println(SQLQuery);
		return SQLQuery;
	}
	
	public String getRequestsFromHBase(String ctrlPath) throws IOException, ParseException {

		OkHttpClient client = new OkHttpClient().newBuilder()
				  .build();
				Request request = new Request.Builder()
				  .url("http://who-dev.essi.upc.edu:60000/WIMEDS-Table-Data/"+completeRowKey+"/Data:Request")
				  .method("GET", null)
				  .build();
				Response response = client.newCall(request).execute();

		String msg = response.message();
		Integer statusCode = response.code();
		
		String content = "";

		Path path = Paths.get("outputrequests.txt");
		content = response.body().string();
		try {
			Files.writeString(path, content, StandardCharsets.UTF_8);
		} catch (IOException ex) {
			System.out.println("exception");
		}
		System.out.println(completeRowKey);
	
		response.body().close();
		//update extraction times after doing the extraction, already done when extracting AdminUnits
		//setExtractionTimes(ctrlPath);
		return content; 
	}
	
	
	
	public String getshipmentRFromHBase(String ctrlPath) throws IOException, ParseException {
		
		OkHttpClient client = new OkHttpClient().newBuilder()
				.build();
		Request request = new Request.Builder()
				.url("http://who-dev.essi.upc.edu:60000/WIMEDS-Table-Data/"+completeRowKey+"/Data:ShipmentR")
				.method("GET", null)
				.build();
		Response response = client.newCall(request).execute();
		
		String msg = response.message();
		Integer statusCode = response.code();
		
		String content = "";

		Path path = Paths.get("outputshipmentR.txt");
		content = response.body().string();
		System.out.println("...content extracted...");
		try {
			Files.writeString(path, content, StandardCharsets.UTF_8);
		} catch (IOException ex) {
			System.out.println("exception");
		}
		response.body().close();
		//update extraction times after doing the extraction
		//setExtractionTimes(ctrlPath);
		return content; 
	}
	
	
public String getDiseaseFromHBase(String ctrlPath) throws IOException, ParseException {
		
		OkHttpClient client = new OkHttpClient().newBuilder()
				.build();
		Request request = new Request.Builder()
				.url("http://who-dev.essi.upc.edu:60000/WIMEDS-Table-Data/"+completeRowKey+"/Data:Disease")
				.method("GET", null)
				.build();
		Response response = client.newCall(request).execute();
		
		String msg = response.message();
		Integer statusCode = response.code();
		
		String content = "";

		Path path = Paths.get("outputDisease.txt");
		content = response.body().string();
		System.out.println("...content extracted...");
		try {
			Files.writeString(path, content, StandardCharsets.UTF_8);
		} catch (IOException ex) {
			System.out.println("exception");
		}
		response.body().close();
		//update extraction times after doing the extraction
		//setExtractionTimes(ctrlPath);
		return content; 
	}

public String getRequestStatusFromHBase(String ctrlPath) throws IOException, ParseException {

	OkHttpClient client = new OkHttpClient().newBuilder()
			.build();
	Request request = new Request.Builder()
			.url("http://who-dev.essi.upc.edu:60000/WIMEDS-Table-Data/"+completeRowKey+"/Data:RequestStatus")
			.method("GET", null)
			.build();
	Response response = client.newCall(request).execute();

	String msg = response.message();
	Integer statusCode = response.code();

	String content = "";

	Path path = Paths.get("outputRequestStatus.txt");
	content = response.body().string();
	System.out.println("...content extracted...");
	try {
		Files.writeString(path, content, StandardCharsets.UTF_8);
	} catch (IOException ex) {
		System.out.println("exception");
	}
	response.body().close();
	//update extraction times after doing the extraction
	//setExtractionTimes(ctrlPath);
	return content; 
}

public String getManufacturerFromHBase(String ctrlPath) throws IOException, ParseException {

	OkHttpClient client = new OkHttpClient().newBuilder()
			.build();
	Request request = new Request.Builder()
			.url("http://who-dev.essi.upc.edu:60000/WIMEDS-Table-Data/"+completeRowKey+"/Data:Manufacturer")
			.method("GET", null)
			.build();
	Response response = client.newCall(request).execute();

	String msg = response.message();
	Integer statusCode = response.code();

	String content = "";

	Path path = Paths.get("outputManufacturer.txt");
	content = response.body().string();
	System.out.println("...content extracted...");
	try {
		Files.writeString(path, content, StandardCharsets.UTF_8);
	} catch (IOException ex) {
		System.out.println("exception");
	}
	response.body().close();
	//update extraction times after doing the extraction
	//setExtractionTimes(ctrlPath);
	return content; 
}


public String getMedicalSupplyFromHBase(String ctrlPath) throws IOException, ParseException {

	OkHttpClient client = new OkHttpClient().newBuilder()
			.build();
	Request request = new Request.Builder()
			.url("http://who-dev.essi.upc.edu:60000/WIMEDS-Table-Data/"+completeRowKey+"/Data:MedicalSupply")
			.method("GET", null)
			.build();
	Response response = client.newCall(request).execute();

	String msg = response.message();
	Integer statusCode = response.code();

	String content = "";

	Path path = Paths.get("outputMedicalSupply.txt");
	content = response.body().string();
	System.out.println("...content extracted...");
	try {
		Files.writeString(path, content, StandardCharsets.UTF_8);
	} catch (IOException ex) {
		System.out.println("exception");
	}
	response.body().close();
	//update extraction times after doing the extraction
	//setExtractionTimes(ctrlPath);
	return content; 
}
	
	
	
	public String getRequestsQuery(String requestData) {
		System.out.println("...generating SQLQuery from HBase data...");
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
	
	public String getshipmentRquery(String shipmentRdata) {
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
			
			
			Instant end = Instant.now();
			LocalDateTime ldtStart = LocalDateTime.ofInstant(start, ZoneId.systemDefault());
			LocalDateTime ldtEnd = LocalDateTime.ofInstant(end, ZoneId.systemDefault());
			System.out.println("time spent: " + Duration.between(ldtStart, ldtEnd).toSeconds()+" seconds");
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
	
	//call this function when extracting data=>update extraction times at control.properties
	//this updates the Extraction time when last extraction from WIMEDS-Table-Data has been made
	public void setExtractionTimes(String ctrlPath) throws IOException {
		///////////////
		FileInputStream in = new FileInputStream(ctrlPath);

		Properties props = new Properties();
		props.load(in);
		String prevExtractionTS = props.getProperty("tz_thisExtractionWIMEDSTableData");
		in.close();

		FileOutputStream out = new FileOutputStream(ctrlPath);

		LocalDateTime now = LocalDateTime.now();
		Timestamp timestamp = Timestamp.valueOf(now);
		long thisExtractionTS = timestamp.getTime(); //returns a long
		String current = String.valueOf(thisExtractionTS);
		props.setProperty("tz_prevExtractionWIMEDSTableData", prevExtractionTS);
		props.setProperty("tz_thisExtractionWIMEDSTableData", current);

		props.store(out, null);
		out.close();
		//////////////
	}

	
	public void setProperties(String ctrlPath) throws IOException{
		
		//////////
		Properties props = new Properties();
        FileInputStream in = new FileInputStream(ctrlPath);

        props.load(in);
		this.setDBurl(props.getProperty("testDBurl"));
		this.setDBusr(props.getProperty("testDBusr"));
		this.setDBpwd(props.getProperty("testDBpwd"));
		this.setHBaseURL(props.getProperty("urlWIMEDSdataTable"));
		this.setthiscompleteRowKey(props.getProperty("completeRowKey"));
		
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

	public void setHBaseURL(String str)     {this.HBaseURL = str;}
	public String getHBaseURL()             {return HBaseURL;}
	
	public void setthiscompleteRowKey(String str)     {this.completeRowKey = str;}
	public String getthiscompleteRowKey() {return completeRowKey;}

}
