package WIMEDSbonitaToHBase;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Base64;
import java.util.Date;
import java.util.Properties;

import org.json.JSONArray;
import org.json.JSONObject;

import okhttp3.MediaType;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.RequestBody;
import okhttp3.Response;
import okhttp3.ResponseBody;

public class IndividualRequestExtraction {

	String tableName, family, rowKeyBase, metadataVersion, completeRowKey, jsessionid, bonitaToken;
	public IndividualRequestExtraction(){}


	//export all the requests to HBase. All Requests under the same rowKey, stored as JSONArray
	public void exportRequests() throws IOException {
		OkHttpClient client = new OkHttpClient().newBuilder()
				.build();
		Request request = new Request.Builder()
				.url("http://localhost:8080/bonita/API/bdm/businessData/com.company.model.Request?q=find&p=0&c=10000")
				.method("GET", null)
				.addHeader("Cookie", "JSESSIONID="+jsessionid+"; X-Bonita-API-Token="+bonitaToken+";")
				.build();

		Response response = client.newCall(request).execute();
		ResponseBody body = response.body();

		String bodyString = response.body().string();

		JSONArray JSONArray = new JSONArray(bodyString); 
		putRequestData(JSONArray);

	}

	//export all the shipments objects to HBase. All Shipments under the same rowKey (inside the same as Request)
	//in the Process of RequestMedicine there are 2 columns: Requests, Shipments (both stored in JSON)
	public void exportShipments() throws IOException {

		OkHttpClient client = new OkHttpClient().newBuilder()
				.build();
		Request request = new Request.Builder()
				.url("http://localhost:8080/bonita/API/bdm/businessData/com.company.model.ShipmentR?q=find&p=0&c=10000")
				.method("GET", null)
				.addHeader("Cookie", "JSESSIONID="+jsessionid+"; X-Bonita-API-Token="+bonitaToken+";")
				.build();
		Response response = client.newCall(request).execute();

		ResponseBody body = response.body();

		String bodyString = response.body().string();

		JSONArray JSONArray = new JSONArray(bodyString); 
		putShipmentData(JSONArray);
	}



	public void exportRequestDocuments() throws IOException {
		OkHttpClient client = new OkHttpClient().newBuilder()
				.build();
		Request request = new Request.Builder()
				.url("http://localhost:8080/bonita/API/bdm/businessData/com.company.model.RequestDocument?q=find&p=0&c=10000")
				.method("GET", null)
				.addHeader("Cookie", "JSESSIONID="+jsessionid+"; X-Bonita-API-Token="+bonitaToken+";")
				.build();
		Response response = client.newCall(request).execute();

		ResponseBody body = response.body();

		String bodyString = response.body().string();

		JSONArray JSONArray = new JSONArray(bodyString); 
		putRequestDocumentData(JSONArray);

	}

	//export all the shipments objects to HBase. All Shipments under the same rowKey (inside the same as Request)
	//in the Process of RequestMedicine there are 2 columns: Requests, Shipments (both stored in JSON)
	public void exportDisease() throws IOException {

		OkHttpClient client = new OkHttpClient().newBuilder()
				.build();
		Request request = new Request.Builder()
				.url("http://localhost:8080/bonita/API/bdm/businessData/com.company.model.Disease?q=find&p=0&c=10000")
				.method("GET", null)
				.addHeader("Cookie", "JSESSIONID="+jsessionid+"; X-Bonita-API-Token="+bonitaToken+";")
				.build();
		Response response = client.newCall(request).execute();

		String bodyString = response.body().string();


		JSONArray JSONArray = new JSONArray(bodyString); 
		putDiseaseData(JSONArray);

	}

	//export all the shipments objects to HBase. All Shipments under the same rowKey (inside the same as Request)
	//in the Process of RequestMedicine there are 2 columns: Requests, Shipments (both stored in JSON)
	public void exportRequestStatus() throws IOException {

		OkHttpClient client = new OkHttpClient().newBuilder()
				.build();
		Request request = new Request.Builder()
				.url("http://localhost:8080/bonita/API/bdm/businessData/com.company.model.RequestStatus?q=find&p=0&c=10000")
				.method("GET", null)
				.addHeader("Cookie", "JSESSIONID="+jsessionid+"; X-Bonita-API-Token="+bonitaToken+";")
				.build();
		Response response = client.newCall(request).execute();

		String bodyString = response.body().string();

		JSONArray JSONArray = new JSONArray(bodyString); 
		putRequestStatusData(JSONArray);

	}

	//export all the shipments objects to HBase. All Shipments under the same rowKey (inside the same as Request)
	//in the Process of RequestMedicine there are 2 columns: Requests, Shipments (both stored in JSON)
	public void exportMedicalSupply() throws IOException {

		OkHttpClient client = new OkHttpClient().newBuilder()
				.build();
		Request request = new Request.Builder()
				.url("http://localhost:8080/bonita/API/bdm/businessData/com.company.model.MedicalSupply?q=find&p=0&c=10000")
				.method("GET", null)
				.addHeader("Cookie", "JSESSIONID="+jsessionid+"; X-Bonita-API-Token="+bonitaToken+";")
				.build();
		Response response = client.newCall(request).execute();

		String bodyString = response.body().string();


		JSONArray JSONArray = new JSONArray(bodyString); 
		putMedicalSupplyData(JSONArray);

	}

	//export all the shipments objects to HBase. All Shipments under the same rowKey (inside the same as Request)
	//in the Process of RequestMedicine there are 2 columns: Requests, Shipments (both stored in JSON)
	public void exportManufacturer() throws IOException {

		OkHttpClient client = new OkHttpClient().newBuilder()
				.build();
		Request request = new Request.Builder()
				.url("http://localhost:8080/bonita/API/bdm/businessData/com.company.model.AdministrationUnit?q=find&p=0&c=35000")
				.method("GET", null)
				.addHeader("Cookie", "JSESSIONID="+jsessionid+"; X-Bonita-API-Token="+bonitaToken+";")
				.build();
		Response response = client.newCall(request).execute();

		String bodyString = response.body().string();

		JSONArray JSONArray = new JSONArray(bodyString); 
		putManufacturerData(JSONArray);

	}
	
	
	
	public void exportAdministrationUnit() throws IOException {
		OkHttpClient client = new OkHttpClient().newBuilder()
				.build();
		Request request = new Request.Builder()
				.url("http://localhost:8080/bonita/API/bdm/businessData/com.company.model.AdministrationUnit?q=find&p=0&c=35000")
				.method("GET", null)
				.addHeader("Cookie", "JSESSIONID="+jsessionid+"; X-Bonita-API-Token="+bonitaToken+";")
				.build();
		Response response = client.newCall(request).execute();
		
		ResponseBody body = response.body();

		String bodyString = response.body().string();

		JSONArray resultJSONArray = new JSONArray(bodyString);
		System.out.println(resultJSONArray.length());
		putAdministrationUnitData(resultJSONArray);
	}



	public void putRequestData(JSONArray requestsJSONArray) throws IOException {

		//encode data for HBase
		String JSONrowPUT = RequestDataEncoding(requestsJSONArray);

		OkHttpClient client = new OkHttpClient().newBuilder()
				.build();
		MediaType mediaType = MediaType.parse("application/json");
		RequestBody body = RequestBody.create(JSONrowPUT, mediaType);
		Request request = new Request.Builder()
				.url("http://who-dev.essi.upc.edu:60000/WIMEDS-Table-Data/fakerow")
				.method("PUT", body)
				.addHeader("Content-Type", "application/json")
				.build();
		Response response = client.newCall(request).execute();

		String msg = response.message();
		Integer statusCode = response.code();

	}

	public void putShipmentData(JSONArray shipmentJSONArray) throws IOException {

		//encode data for HBase
		String JSONrowPUT = shipmentRdataEncoding(shipmentJSONArray);

		OkHttpClient client = new OkHttpClient().newBuilder()
				.build();
		MediaType mediaType = MediaType.parse("application/json");
		RequestBody body = RequestBody.create(JSONrowPUT, mediaType);
		Request request = new Request.Builder()
				.url("http://who-dev.essi.upc.edu:60000/WIMEDS-Table-Data/fakerow")
				.method("PUT", body)
				.addHeader("Content-Type", "application/json")
				.build();
		Response response = client.newCall(request).execute();
	}


	public void putRequestDocumentData(JSONArray requestsDocuJSONArray) throws IOException {

		//encode data for HBase
		String JSONrowPUT = RequestDocumentEncoding(requestsDocuJSONArray);

		OkHttpClient client = new OkHttpClient().newBuilder()
				.build();
		MediaType mediaType = MediaType.parse("application/json");
		RequestBody body = RequestBody.create(JSONrowPUT, mediaType);
		Request request = new Request.Builder()
				.url("http://who-dev.essi.upc.edu:60000/WIMEDS-Table-Data/fakerow")
				.method("PUT", body)
				.addHeader("Content-Type", "application/json")
				.build();
		Response response = client.newCall(request).execute();

		String msg = response.message();
		Integer statusCode = response.code();

	}


	public void putDiseaseData(JSONArray diseaseJSONArray) throws IOException {

		//encode data for HBase
		String JSONrowPUT = DiseaseDataEncoding(diseaseJSONArray);
		System.out.println("//////////storing disease data/////////////////");


		OkHttpClient client = new OkHttpClient().newBuilder()
				.build();
		MediaType mediaType = MediaType.parse("application/json");
		RequestBody body = RequestBody.create(JSONrowPUT, mediaType);
		Request request = new Request.Builder()
				.url("http://who-dev.essi.upc.edu:60000/WIMEDS-Table-Data/fakerow")
				.method("PUT", body)
				.addHeader("Content-Type", "application/json")
				.build();
		Response response = client.newCall(request).execute();

		String msg = response.message();
		Integer statusCode = response.code();
	}



	public void putRequestStatusData(JSONArray RequestStatusJSONArray) throws IOException {

		//encode data for HBase
		String JSONrowPUT = RequestStatusDataEncoding(RequestStatusJSONArray);
		System.out.println("//////////storing requestStatus data/////////////////");


		OkHttpClient client = new OkHttpClient().newBuilder()
				.build();
		MediaType mediaType = MediaType.parse("application/json");
		RequestBody body = RequestBody.create(JSONrowPUT, mediaType);
		Request request = new Request.Builder()
				.url("http://who-dev.essi.upc.edu:60000/WIMEDS-Table-Data/fakerow")
				.method("PUT", body)
				.addHeader("Content-Type", "application/json")
				.build();
		Response response = client.newCall(request).execute();

		String msg = response.message();
		Integer statusCode = response.code();
	}



	public void putMedicalSupplyData(JSONArray RequestStatusJSONArray) throws IOException {

		//encode data for HBase
		String JSONrowPUT = MedicalSupplyDataEncoding(RequestStatusJSONArray);
		System.out.println("//////////storing medicalSupply data/////////////////");


		OkHttpClient client = new OkHttpClient().newBuilder()
				.build();
		MediaType mediaType = MediaType.parse("application/json");
		RequestBody body = RequestBody.create(JSONrowPUT, mediaType);
		Request request = new Request.Builder()
				.url("http://who-dev.essi.upc.edu:60000/WIMEDS-Table-Data/fakerow")
				.method("PUT", body)
				.addHeader("Content-Type", "application/json")
				.build();
		Response response = client.newCall(request).execute();
		
		String msg = response.message();
		Integer statusCode = response.code();
		System.out.println("msg: "+msg+" statusCode: "+statusCode);
	}


	public void putManufacturerData(JSONArray RequestStatusJSONArray) throws IOException {

		//encode data for HBase
		String JSONrowPUT = ManufacturerDataEncoding(RequestStatusJSONArray);
		System.out.println("//////////storing manufacturer data/////////////////");


		OkHttpClient client = new OkHttpClient().newBuilder()
				.build();
		MediaType mediaType = MediaType.parse("application/json");
		RequestBody body = RequestBody.create(JSONrowPUT, mediaType);
		Request request = new Request.Builder()
				.url("http://who-dev.essi.upc.edu:60000/WIMEDS-Table-Data/fakerow")
				.method("PUT", body)
				.addHeader("Content-Type", "application/json")
				.build();
		Response response = client.newCall(request).execute();
		
		String msg = response.message();
		Integer statusCode = response.code();
		System.out.println("msg: "+msg+" statusCode: "+statusCode);

	}
	
	
	public void putAdministrationUnitData(JSONArray AdminUnitJSONArray) throws IOException {

		//encode data for HBase
		String JSONrowPUT = AdministrationUnitDataEncoding(AdminUnitJSONArray);
		System.out.println("//////////storing administration unit data/////////////////");


		OkHttpClient client = new OkHttpClient().newBuilder()
				.build();
		MediaType mediaType = MediaType.parse("application/json");
		RequestBody body = RequestBody.create(JSONrowPUT, mediaType);
		Request request = new Request.Builder()
				.url("http://who-dev.essi.upc.edu:60000/WIMEDS-Table-Data/fakerow")
				.method("PUT", body)
				.addHeader("Content-Type", "application/json")
				.build();
		Response response = client.newCall(request).execute();
		
		String msg = response.message();
		Integer statusCode = response.code();
		System.out.println("msg: "+msg+" statusCode: "+statusCode);

	}

	public String RequestDataEncoding(JSONArray requestsJSONArray) throws IOException {
		/////////////////////////////////////		
		String rootPath = Thread.currentThread().getContextClassLoader().getResource("").getPath();
		String ctrlPath = rootPath + "control.properties";

		FileInputStream in = new FileInputStream(ctrlPath);

		Properties props = new Properties();
		props.load(in);
		//gets
		String mtdVersion = "";
		String rkb = "";
		rkb = props.getProperty("rowKeyBase");
		setRowKeyBase(rkb);
		mtdVersion = props.getProperty("metadataVersion");
		setMetadataVersion(mtdVersion);
		in.close();

		DateFormat sdf = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS");
		Date date = new Date();
		String currentTime = sdf.format(date).toString();
		//create the complete RowKey, that is, completeRowKey and set it (the other ones will use completeRowKey, global var)
		String crk = rkb+mtdVersion+'$'+currentTime;
		setCompleteRowKey(crk);


		String stringColumn = "Data:Request";//should be get from control.properties
		String value = requestsJSONArray.toString();

		Base64.Encoder enc = Base64.getEncoder();

		String encodedKey = enc.encodeToString(crk.getBytes());

		String encodedColumn = enc.encodeToString(stringColumn.getBytes());

		String encodedValue = enc.encodeToString(value.getBytes());

		encodedKey = '"'+encodedKey+'"';
		encodedColumn = '"'+encodedColumn+'"';
		encodedValue = '"'+encodedValue+'"';

		String JSONrowPUT = "{\"Row\":[{\"key\":"+encodedKey+", \"Cell\": [{\"column\":"+encodedColumn+", \"$\":"+encodedValue+"}]}]}";

		return JSONrowPUT;

	}

	private String shipmentRdataEncoding(JSONArray requestsJSONArray) {

		String stringKey = getCompleteRowKey(); 
		String stringColumn = "Data:ShipmentR";
		String value = requestsJSONArray.toString();
		Base64.Encoder enc = Base64.getEncoder();

		String encodedKey = enc.encodeToString(stringKey.getBytes());

		String encodedColumn = enc.encodeToString(stringColumn.getBytes());

		String encodedValue = enc.encodeToString(value.getBytes());

		encodedKey = '"'+encodedKey+'"';
		encodedColumn = '"'+encodedColumn+'"';
		encodedValue = '"'+encodedValue+'"';

		String JSONrowPUT = "{\"Row\":[{\"key\":"+encodedKey+", \"Cell\": [{\"column\":"+encodedColumn+", \"$\":"+encodedValue+"}]}]}";

		return JSONrowPUT;
	}


	private String RequestDocumentEncoding(JSONArray requestDocumentJSONArray) {

		String stringKey = getCompleteRowKey(); 
		String stringColumn = "Data:RequestDocument";

		int size = requestDocumentJSONArray.length();

		//encodedFile is already encoded, think a way to put it inside the encoded JSONObject
		for (int i = 0; i < requestDocumentJSONArray.length(); i++) {
			requestDocumentJSONArray.getJSONObject(i).remove("encodedFile");
		}

		Base64.Encoder enc = Base64.getEncoder();

		String encodedKey = enc.encodeToString(stringKey.getBytes());


		String encodedColumn = enc.encodeToString(stringColumn.getBytes());


		String value = requestDocumentJSONArray.toString();

		String encodedValue = enc.encodeToString(value.getBytes());

		encodedKey = '"'+encodedKey+'"';
		encodedColumn = '"'+encodedColumn+'"';
		encodedValue = '"'+encodedValue+'"';


		String JSONrowPUT = "{\"Row\":[{\"key\":"+encodedKey+", \"Cell\": [{\"column\":"+encodedColumn+", \"$\":"+encodedValue+"}]}]}";
		return JSONrowPUT;
	}


	private String DiseaseDataEncoding(JSONArray requestsJSONArray) {

		String stringKey = getCompleteRowKey();
		String stringColumn = "Data:Disease";
		String value = requestsJSONArray.toString();

		Base64.Encoder enc = Base64.getEncoder();

		String encodedKey = enc.encodeToString(stringKey.getBytes());

		String encodedColumn = enc.encodeToString(stringColumn.getBytes());

		String encodedValue = enc.encodeToString(value.getBytes());

		encodedKey = '"'+encodedKey+'"';
		encodedColumn = '"'+encodedColumn+'"';
		encodedValue = '"'+encodedValue+'"';

		String JSONrowPUT = "{\"Row\":[{\"key\":"+encodedKey+", \"Cell\": [{\"column\":"+encodedColumn+", \"$\":"+encodedValue+"}]}]}";

		return JSONrowPUT;
	}

	private String RequestStatusDataEncoding(JSONArray requestsJSONArray) {

		String stringKey = getCompleteRowKey();
		String stringColumn = "Data:RequestStatus";
		String value = requestsJSONArray.toString();

		Base64.Encoder enc = Base64.getEncoder();

		String encodedKey = enc.encodeToString(stringKey.getBytes());

		String encodedColumn = enc.encodeToString(stringColumn.getBytes());

		String encodedValue = enc.encodeToString(value.getBytes());

		encodedKey = '"'+encodedKey+'"';
		encodedColumn = '"'+encodedColumn+'"';
		encodedValue = '"'+encodedValue+'"';

		String JSONrowPUT = "{\"Row\":[{\"key\":"+encodedKey+", \"Cell\": [{\"column\":"+encodedColumn+", \"$\":"+encodedValue+"}]}]}";

		return JSONrowPUT;
	}

	private String MedicalSupplyDataEncoding(JSONArray requestsJSONArray) {

		String stringKey = getCompleteRowKey(); 
		String stringColumn = "Data:MedicalSupply";
		String value = requestsJSONArray.toString();
		Base64.Encoder enc = Base64.getEncoder();

		String encodedKey = enc.encodeToString(stringKey.getBytes());

		String encodedColumn = enc.encodeToString(stringColumn.getBytes());

		String encodedValue = enc.encodeToString(value.getBytes());

		encodedKey = '"'+encodedKey+'"';
		encodedColumn = '"'+encodedColumn+'"';
		encodedValue = '"'+encodedValue+'"';

		String JSONrowPUT = "{\"Row\":[{\"key\":"+encodedKey+", \"Cell\": [{\"column\":"+encodedColumn+", \"$\":"+encodedValue+"}]}]}";

		return JSONrowPUT;
	}


	private String ManufacturerDataEncoding(JSONArray requestsJSONArray) {

		String stringKey = getCompleteRowKey();
		String stringColumn = "Data:Manufacturer";
		String value = requestsJSONArray.toString();

		Base64.Encoder enc = Base64.getEncoder();

		String encodedKey = enc.encodeToString(stringKey.getBytes());

		String encodedColumn = enc.encodeToString(stringColumn.getBytes());

		String encodedValue = enc.encodeToString(value.getBytes());

		encodedKey = '"'+encodedKey+'"';
		encodedColumn = '"'+encodedColumn+'"';
		encodedValue = '"'+encodedValue+'"';

		String JSONrowPUT = "{\"Row\":[{\"key\":"+encodedKey+", \"Cell\": [{\"column\":"+encodedColumn+", \"$\":"+encodedValue+"}]}]}";

		return JSONrowPUT;
	}
	
	
	private String AdministrationUnitDataEncoding(JSONArray requestsJSONArray) {

		String stringKey = getCompleteRowKey();
		String stringColumn = "Data:AdminUnit";
		String value = requestsJSONArray.toString();
		int sizeInBytes = value.getBytes().length;
		System.out.println("size in bytes of the string: "+ sizeInBytes);
		Base64.Encoder enc = Base64.getEncoder();

		String encodedKey = enc.encodeToString(stringKey.getBytes());

		String encodedColumn = enc.encodeToString(stringColumn.getBytes());

		String encodedValue = enc.encodeToString(value.getBytes());

		encodedKey = '"'+encodedKey+'"';
		encodedColumn = '"'+encodedColumn+'"';
		encodedValue = '"'+encodedValue+'"';

		String JSONrowPUT = "{\"Row\":[{\"key\":"+encodedKey+", \"Cell\": [{\"column\":"+encodedColumn+", \"$\":"+encodedValue+"}]}]}";

		return JSONrowPUT;
	}


	public void completeRowKeyUpdate(String ctrlPath, String crk) throws IOException {
		///////////
		FileInputStream in = new FileInputStream(ctrlPath);

		Properties props = new Properties();
		props.load(in);
		String prevcompleteRowKey = props.getProperty("completeRowKey");
		in.close();
		FileOutputStream out = new FileOutputStream(ctrlPath);

		//props.setProperty("prevExtraction", prev);
		props.setProperty("completeRowKey", crk);
		System.out.println("this is the row key: "+ completeRowKey);
		props.store(out, null);
		out.close();
		///////////
	}

	//to be called when after an extraction/update is done, update thisWIMEDSextractionDateTime
	public void updateWIMEDSextractionDateTime(String ctrlPath) throws IOException {
		/////////
		FileInputStream in = new FileInputStream(ctrlPath);

		Properties props = new Properties();
		props.load(in);
		String prev = props.getProperty("thisWIMEDSextractionDateTime");
		System.out.println("last WIMEDS DB extraction was made on: "+ prev);
		in.close();
		FileOutputStream out = new FileOutputStream(ctrlPath);
		//calcula new thisWIMEDSextractionDateTime
		String pattern = "yyyy-MM-dd'T'HH:mm:ss.SSSSSS'Z'";
		SimpleDateFormat ZDateFormat = new SimpleDateFormat(pattern);
		String dateZ = ZDateFormat.format(new Date());
		//update thisWIMEDSExtractionDateTime in control.properties
		props.setProperty("thisWIMEDSextractionDateTime", dateZ);
		System.out.println("this WIMEDS DB extraction has been done at: "+ dateZ);
		props.store(out, null);
		out.close();
	}


	public void setProps(String ctrlPath) throws IOException {
		//////////////

		FileInputStream in = new FileInputStream(ctrlPath);

		Properties props = new Properties();
		props.load(in);
		setTableName(props.getProperty("datatableName"));
		setFamily(props.getProperty("family1"));
		setRowKeyBase(props.getProperty("rowKeyBase"));
		setMetadataVersion(props.getProperty("metadataVersion"));
		setJSESSIONID(props.getProperty("jsessionid"));
		setBonitaToken(props.getProperty("bonitaToken"));
		//JSESSIONID and X-Bonita-API-Token still not parametrized...I have errors getting the cookie
		in.close();
		/////////////
	}

	public void setTableName(String str){this.tableName = str;}
	public void setFamily(String str){this.family = str;}

	public String getTableName() {return this.tableName;}
	public String getFamily() {return this.family;}

	public String getRowKeyBase() {return this.rowKeyBase;}
	public void setRowKeyBase(String str){this.rowKeyBase = str;}

	public String getMetadataVersion() {return this.metadataVersion;}
	public void setMetadataVersion(String str){this.metadataVersion = str;}

	public String getCompleteRowKey() {return this.completeRowKey;}
	public void setCompleteRowKey(String str){this.completeRowKey = str;}

	public String getJSESSIONID() {return this.jsessionid;}
	public void setJSESSIONID(String str){this.jsessionid = str;}

	public String getBonitaToken() {return this.bonitaToken;}
	public void setBonitaToken(String str){this.bonitaToken = str;}



}

