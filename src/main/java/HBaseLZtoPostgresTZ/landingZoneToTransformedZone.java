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
import java.text.ParseException;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Arrays;
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
	List<String> tablesMultiThreadList;
	
	
	
	
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
		
		JSONArray JSONArrayRows = new JSONArray();
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
				System.out.println("call number: "+ calls);
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
				    JSONArrayRows = (JSONArray) JSONObjectContent.get("Row");
					
					System.out.println("this is the lenght of the JSONArrayRow :"+JSONArrayRows.length());
				}
				
			}
			catch(Exception e) {System.out.println("Connection error! \nExecution stopped!");
			System.exit(1);
			}
		}

		//System.out.println(JSONArrayTotalRows);
		System.out.println("number of scanner calls: "+calls);
		return JSONArrayRows;
	}
	
	public void exportDataToTZ(JSONArray JSONArrayRows) throws FileNotFoundException {
		
		//for each rows retrieve the values inside each of the columns
		System.out.println("el total de rows agafades de HBase es: "+JSONArrayRows.length());
		for (int i = 0; i < JSONArrayRows.length(); i++) {
			//JSONArray RowArray = JSONArrayRows.getJSONArray(i);
			JSONObject RowObj = JSONArrayRows.getJSONObject(i);
			System.out.println("la size de la row es: "+ RowObj.length());
			JSONArray encodedJSONArray = RowObj.getJSONArray("Cell");
			System.out.println("la size de lo de dins de la Cell es: "+encodedJSONArray.length());

			for(int j = 0; j < encodedJSONArray.length(); ++j) {
				//cada encodedJSONArray(j) és una columna, el problema es saber quina és
				System.out.println(i+"-"+j);
				//AGAFAR LA COLUMN DE DINS DEL encododedJSONArray.get(j) !!!!! :)))))))
				JSONObject dataCell = (JSONObject) encodedJSONArray.get(j);
				
				String encodedColumn = dataCell.getString("column");
				byte[] dataBytes = Base64.getDecoder().decode(encodedColumn);
				String decodedColumn="";
				try {
					decodedColumn = new String(dataBytes, StandardCharsets.UTF_8.name());
					System.out.println(decodedColumn);
				} catch (UnsupportedEncodingException e) {
					e.printStackTrace();
				}
				
				String encodedValue = dataCell.getString("$");
				String decodedValue="";
				dataBytes = Base64.getDecoder().decode(encodedValue);
				try {
					decodedValue = new String(dataBytes, StandardCharsets.UTF_8.name());
				} catch (UnsupportedEncodingException e) {
					e.printStackTrace();
				}
				
				//un cop ja tinc la decodedColumn agafo només el columnIdentifier
				//també tinc decodedValue
				String columnIdentifier = decodedColumn.substring(5, decodedColumn.length()); //pos 5 is next to ':' (e.g., Data:Request)
				System.out.println(columnIdentifier);
				
				//en el control.properties tenim totes les TZtables que volem dins la transformedZone (potser no les voldriem totes..)
				//per això mirem si, per cada una de les columnes extretes (columnIdentifier) esta en les TZtables array contains it.
				//si la conté enviem el nom de la columna a undistribuidor que envia a fer una query o una altre segons la column
				String[] tables = tablesNamesCSV.split(",");
				List tablesList = Arrays.asList(tables);
				if(tablesList.contains(columnIdentifier)) {
					//cridar a organitzador de generacions de SQLQueries
					queryGenerator(columnIdentifier, decodedValue);
				}
				
			}

		}
		/*
		PrintStream fileStream = new PrintStream("outputLZtoTZ.txt");
		System.setOut(fileStream);
		System.out.println(JSONObjectContent);
		*/
	}
	
	private void queryGenerator(String columnIdentifier, String decodedValue) {
		//only tables in tablesTZ from control.properties are implemented here.
		//if more are needed, add the call here to the getNewNeededTableQuery("NewNeededTable")
		String SQLQuery = "";
		if(columnIdentifier.equals("Request")) {
			if(tablesMultiThread.contains(columnIdentifier))SQLQuery=getSQLQueryMultiThreading(decodedValue, columnIdentifier);
			else SQLQuery = getRequestQuery(decodedValue);
		}
		else if(columnIdentifier.equals("Manufacturer")) {
			SQLQuery=getManufacturerQuery(decodedValue);
		}
		else if(columnIdentifier.equals("Disease")) {
			SQLQuery=getDiseaseQuery(decodedValue);
		}
		else if(columnIdentifier.equals("MedicalSupply")) {
			SQLQuery=getMedicalSupplyQuery(decodedValue);
		}
		else if(columnIdentifier.equals("RequestStatus")) {
			SQLQuery=getRequestStatusQuery(decodedValue);
		}
		else if(columnIdentifier.equals("ShipmentR")) {
			if(tablesMultiThread.contains(columnIdentifier))SQLQuery=getSQLQueryMultiThreading(decodedValue, columnIdentifier);
			else SQLQuery = getShipmentRQuery(decodedValue);
		}
		
		LoadInDB(SQLQuery, tz_DBurl, tz_DBusr, tz_DBpwd);

	}


	//not using multiThreading
		public String getRequestQuery(String requestData) {
			System.out.println("...generating requests SQLQuery from HBase data...");
			System.out.println("WITHOUT making use of multithreading");
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
		
		
		//multithreading to generate SQLQuery
		public String getSQLQueryMultiThreading(String data, String whichData) {
			String SQLQuery = ""; //UPSERT query
			JSONArray content = new JSONArray(data);
			int THREADS;
			if(threadControl.equals("availableProcessors")) THREADS =  Runtime.getRuntime().availableProcessors();
			else THREADS = Integer.parseInt(threadControl);
			//System.out.println("using "+THREADS+" threads, generating queries");
			//call ParallelPartialQueryGenerator
			ParallelPartialQueryGenerator partialQueryGen = new ParallelPartialQueryGenerator(THREADS);
			//put workers to work and get the result query 
			try {
				SQLQuery += partialQueryGen.partialQueriesSum(content, whichData);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}

			return SQLQuery;
		}
		
		
		public String getManufacturerQuery(String ManufacturerData) {
			System.out.println("...generating manufacturers SQLQuery from HBase data...");
			String SQLQuery="";
			JSONArray jsonArrayDisease = new JSONArray(ManufacturerData);
			for(int i = 0;i<jsonArrayDisease.length(); ++i) {
				int id;
				String name;
				
				JSONObject DiseaseJSONObj = jsonArrayDisease.getJSONObject(i);
				id = DiseaseJSONObj.getInt("persistenceId");
				name = DiseaseJSONObj.getString("name");

				SQLQuery += "INSERT INTO Manufacturer VALUES ("
						+ id + ",'"
						+ name + "') ON CONFLICT ON CONSTRAINT manufacturer_pkey"
						+" DO UPDATE SET"
						+" id="+id+","
						+" name='"+name+"';\n";
			}
			return SQLQuery;
		}
		
		
		public String getDiseaseQuery(String DiseaseData) {
			System.out.println("...generating diseases SQLQuery from HBase data...");
			String SQLQuery="";
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
			
				String medsuppliesids=medicalSuppliesIDs.toString();
				String medsuppliesidsMod=medsuppliesids.replace("[", "{");
				medsuppliesidsMod=medsuppliesidsMod.replace("]", "}");

				SQLQuery += "INSERT INTO Disease VALUES ("
						+ id + ",'"
						+ name + "','"
						+ completeName + "',ARRAY"
						+ medicalSuppliesIDs + ") ON CONFLICT ON CONSTRAINT disease_pkey"
						+ " DO UPDATE SET"
						+ " name='"+name+"',"
						+ " completeName='"+completeName+"',"
						+ " medicalsuppliesids='"+medsuppliesidsMod+"';\n";
			}
			return SQLQuery;
		}
		
		
		public String getMedicalSupplyQuery(String medicalSupplyData) {
			System.out.println("...generating medical supplies SQLQuery from HBase data...");
			String SQLQuery="";
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
							+ manufacturerEmpty + ") ON CONFLICT ON CONSTRAINT medicalsupply_pkey"
							+ " DO UPDATE SET"
							+ " name='"+name+"',"
							+ " completename='"+completeName+"';\n";
				}
				else {
					String manufacturersids=manufacturersIDs.toString();
					String manufacturersidsMod=manufacturersids.replace("[", "{");
					manufacturersidsMod=manufacturersidsMod.replace("]", "}");
					SQLQuery += "INSERT INTO MedicalSupply VALUES ("
							+ id + ",'"
							+ name + "','"
							+ completeName + "',ARRAY"
							+ manufacturersIDs + ") ON CONFLICT ON CONSTRAINT medicalsupply_pkey"
							+ " DO UPDATE SET"
							+ " name='"+name+"',"
							+ " completename='"+completeName+"',"
							+ " manufacturersids='"+manufacturersidsMod+"';\n";
				}
			}
			return SQLQuery;
		}
		
		public String getRequestStatusQuery(String RequestStatusData) {
			System.out.println("...generating requeststatus SQLQuery from HBase data...");
			String SQLQuery="";
			
			JSONArray jsonArrayDisease = new JSONArray(RequestStatusData);
			for(int i = 0;i<jsonArrayDisease.length(); ++i) {
				int id;
				String name;
				
				JSONObject DiseaseJSONObj = jsonArrayDisease.getJSONObject(i);
				id = DiseaseJSONObj.getInt("persistenceId");
				name = DiseaseJSONObj.getString("name");

				SQLQuery += "INSERT INTO RequestStatus VALUES ("
						+ id + ",'"
						+ name + "') ON CONFLICT ON CONSTRAINT requeststatus_pkey"
						+ " DO UPDATE SET"
						+ " name='"+name+"';\n"; 
			}
			return SQLQuery;
		}
		
		public String getShipmentRQuery(String shipmentRdata) {
			System.out.println("...generating shipmentr SQLQuery from HBase data...");
			String SQLQuery="";
			
			JSONArray jsonArrayship = new JSONArray(shipmentRdata);
			
			for(int i = 0;i<jsonArrayship.length(); ++i) {
				int id, quantity, requestID, trackingNumber;
				Integer quantityReceived = null;//int can not be null
				String receptionDateString = null;
				String courierName = null;
				String medicalSupplyName, shipmentStatus, shipmentDateCreationString, EDDstring, healthFacilityName, 
				shippedDateString;
				
				JSONObject shipmentJSONObj = jsonArrayship.getJSONObject(i);
				id = shipmentJSONObj.getInt("persistenceId");
				quantity = shipmentJSONObj.getInt("quantity");
				healthFacilityName = shipmentJSONObj.getString("healthFacilityName");
				Object aObj = shipmentJSONObj.get("quantityReceived");
				if (aObj instanceof Integer) {
					quantityReceived = shipmentJSONObj.getInt("quantityReceived");//if not received is null
				}
				aObj = shipmentJSONObj.get("receptionDate"); //if not received is null
				if (aObj instanceof String) {
					receptionDateString = shipmentJSONObj.getString("receptionDate");
				}
				medicalSupplyName = shipmentJSONObj.getJSONObject("medicalSupply").getString("name");
				requestID = shipmentJSONObj.getJSONObject("request").getInt("persistenceId");
				shipmentDateCreationString = shipmentJSONObj.getString("dateOfCreation");
				shipmentDateCreationString = shipmentDateCreationString.substring(0,10);
				EDDstring = shipmentJSONObj.getString("edd");
				aObj = shipmentJSONObj.get("shippedDate");// if not shipped is null
				if (aObj instanceof String) {
					courierName = shipmentJSONObj.getString("shippedDate");
				}
				shippedDateString = shipmentJSONObj.getString("shippedDate");
				shipmentStatus = shipmentJSONObj.getString("shipmentStatus");
				aObj = shipmentJSONObj.get("courierName");// sometimes courierName is null
				if (aObj instanceof String) {
					courierName = shipmentJSONObj.getString("courierName");
				}
				trackingNumber = shipmentJSONObj.getInt("trackingNumber");
				
				SQLQuery += "INSERT INTO shipmentR VALUES ("
						+ id + ",'"
						+ shipmentDateCreationString + "','"
						+ shipmentStatus + "','"
						+ EDDstring + "','";
				if(shippedDateString==null)SQLQuery+=null+",";
				else SQLQuery+=shippedDateString+"',";
				if(receptionDateString == null)SQLQuery+=null + ",";
				else SQLQuery += "'"+receptionDateString + "',";
				SQLQuery += requestID + ",'"
						+ medicalSupplyName + "','"
						+ healthFacilityName + "',"
						+ quantity + ",";
				if(quantityReceived == null)SQLQuery+=null + ",";
				else SQLQuery += quantityReceived+",";
				if(courierName == null)SQLQuery+=null + ",";
				else SQLQuery += "'"+courierName+"',";
				SQLQuery+= trackingNumber+") ON CONFLICT ON CONSTRAINT shipmentr_pkey"
						+ " DO UPDATE SET"
						+ " shipmentcreationdate='"+shipmentDateCreationString+"',"
						+ " shipmentstatus='"+shipmentStatus+"',"
						+ " EDD='"+EDDstring+"',";
				if(shippedDateString==null)SQLQuery+= " shippedDate="+shippedDateString+",";
				else SQLQuery+= " shippedDate='"+shippedDateString+"',";
				if(receptionDateString==null)SQLQuery+= " receptionDate="+receptionDateString+",";
				else SQLQuery+= " receptionDate='"+receptionDateString+"',";	
						SQLQuery+= " medicalsupplyname='"+medicalSupplyName+"',"
						+ " healthFacilityName='"+healthFacilityName+"',"
						+ " quantity="+quantity+","
						+ " quantityreceived="+quantityReceived+",";
				if(courierName==null)SQLQuery+= " courierName="+courierName+",";
				else SQLQuery+= " courierName='"+courierName+"',";
						SQLQuery+=" trackingNumber="+trackingNumber+";\n";

			}
			
			
			return SQLQuery;
		}
		
		
		
		public String getAdministrationUnitQuery(String AdministrationUnitData) {
			System.out.println("...generating administrationunit SQLQuery from HBase data...");
			String SQLQuery="";
			
			JSONArray jsonArrayAdminsitrationUnit = new JSONArray(AdministrationUnitData);
			System.out.println("aquesta es la lenght de les administrationunits a codificar ara:"+jsonArrayAdminsitrationUnit.length());
			for(int i = 0;i<jsonArrayAdminsitrationUnit.length(); ++i) {
				JSONObject AdministrationUnitJSONObj = jsonArrayAdminsitrationUnit.getJSONObject(i);
				String parentid = "";
				if(AdministrationUnitJSONObj.has("parent")) {
					JSONObject JSONObjparent = AdministrationUnitJSONObj.getJSONObject("parent");
					parentid = JSONObjparent.getString("id");
				}
				
				Object aObj = AdministrationUnitJSONObj.get("address"); //if not received is null
				String address=null;
				if (aObj instanceof String) {
					address = AdministrationUnitJSONObj.getString("address");
				}
				aObj = AdministrationUnitJSONObj.get("url");
				String url = null;//just some organisationUnits have (e.g., http://www.hospitalclinic.org)
				if(aObj instanceof String) {
					url = AdministrationUnitJSONObj.getString("url");
				}
				if(i==1) {
					if(address.equals("null"))System.out.print("aquesta es la null address: "+ address);
				System.out.println(address);
				System.out.println(url);
				}
				String id, name, shortname, datelastupdated;
				Boolean leaf;
				Integer levelnumber;
				id = AdministrationUnitJSONObj.getString("id");
				name = AdministrationUnitJSONObj.getString("name");
				/*
				if(id.contentEquals("yNHpYw6uIFQ")) {
					System.out.println("nom mal fet, " +name);
					String name1 = name.replaceAll("'", "''");
					System.out.println("nom ben fet"+name1);
				}*/
				shortname = AdministrationUnitJSONObj.getString("shortName");
				datelastupdated = AdministrationUnitJSONObj.getString("dateLastUpdated");
				leaf = AdministrationUnitJSONObj.getBoolean("leaf");
				levelnumber = AdministrationUnitJSONObj.getInt("levelNumber");
				//!id.contentEquals("yNHpYw6uIFQ")&&
				if((i<20)) {
				SQLQuery += "INSERT INTO AdministrationUnit VALUES ('"
						+ id + "','"
						+ parentid + "','"
						+ name.replaceAll("'","") + "','"
						+ shortname.toString().replaceAll("'","") + "','"
						+ datelastupdated.substring(0,10) + "',"
						+ leaf + ","
						+ levelnumber + ",'";
						if(address.equals("null"))SQLQuery+=address+",";
						else SQLQuery+= "'"+address+"',";
						if(url.equals("null"))SQLQuery+= url+")";
						else SQLQuery+= "'"+url+"')";
						SQLQuery+= " ON CONFLICT ON CONSTRAINT administrationunit_pkey"
						+ " DO UPDATE SET"
						+ " parentid='"+parentid+"',"
						+ " name='"+name+"',"
						+ " shortname='"+shortname+"',"
						+ " datelastupdated='"+datelastupdated.substring(0,10)+"',"
						+ " leaf="+leaf+","
						+ " levelnumber="+levelnumber+",";
						if(address.equals("null"))SQLQuery+= " address="+address+",";
						else SQLQuery+= " address='"+address+"',";
						if(url.equals("null"))SQLQuery+= " url="+url+";\n";
						else SQLQuery+= " url='"+url+"';\n";	
						
						
						
						
			}
			}
			System.out.println(SQLQuery);
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
		
		//define tablesmMultiThreading
		if(!useMultiThreading)this.tablesMultiThread ="";//NO multi Threading is used
		else {
			String[] tablesMT = tablesMultiThread.split(",");
			List tablesListMT = Arrays.asList(tablesMT);
			System.out.println("tables multithreadin: "+ tablesListMT);
		}
		
			
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
