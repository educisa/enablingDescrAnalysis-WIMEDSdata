package WIMEDSbonitaToHBase;

import java.awt.Container;
import java.awt.Window;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.Socket;
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

public class IndividualExtractionBlocks {
		
	int REQUEST_maxSIZE_BYTES = 10000000; //10 MB
	String tableName, family, rowKeyBase, metadataVersion, completeRowKey, jsessionid, bonitaToken,
	WIMEDSextractionTimeGV, urlAPI, tablesNamesCSV;
	
	public IndividualExtractionBlocks(){}
	
	//initial func
	public void exportData() throws IOException {
		
		String[] tables = tablesNamesCSV.split(",");
		for(int i = 0; i< tables.length; ++i) {
			System.out.println("extracting "+tables[i]+" data");
			exportWIMEDSdata(tables[i]);
		}
	}
	
	
	public void exportWIMEDSdata(String WIMEDStableName) throws IOException {
		OkHttpClient client = new OkHttpClient().newBuilder()
				.build();
		Request request = new Request.Builder()
				.url(urlAPI+"com.company.model."+WIMEDStableName+"?q=find&p=0&c=1000000")
				.method("GET", null)
				.addHeader("Cookie", "JSESSIONID="+jsessionid+"; X-Bonita-API-Token="+bonitaToken+";")
				.build();

		Response response = client.newCall(request).execute();
		ResponseBody body = response.body();

		String bodyString = response.body().string();

		JSONArray JSONArray = new JSONArray(bodyString); 
		putData(JSONArray, WIMEDStableName);
	}
	
	
	public void putData(JSONArray contentJSONArray, String family) throws IOException {
		int sum = 0;
		//encode data for HBase
		//do block partitions (if necessary).
		//max of 10 MB (server limit)
		//divide the total bytes per 10 MB will give us the number of necessary blocks to send data
		String value = contentJSONArray.toString();
		int sizeInBytes = value.getBytes().length;
		System.out.println("size in bytes of the string (all rows): "+ sizeInBytes);
		//10 MB = 10000000 bytes, so sizeInBytes/10000000
		int numBlocks = sizeInBytes/REQUEST_maxSIZE_BYTES;
		if(sizeInBytes%REQUEST_maxSIZE_BYTES != 0) numBlocks+=1;
		System.out.println("el numero de blocks necessaris per enviar les dades al servidor son: "+numBlocks+" blocks");
		//numBlocks: total number of blocks needed to send data to server
		//blockNum: number of the individual block (identifier)

		for(int blockNum = 0; blockNum < numBlocks; ++blockNum) {

			int maxROWS_PER_BLOCK = contentJSONArray.length()/numBlocks;
			System.out.println("el maxROWS_PER_BLOCK es: "+ maxROWS_PER_BLOCK);
			int start, end;

			if(blockNum == numBlocks-1) {
				start = blockNum*maxROWS_PER_BLOCK;
				end = contentJSONArray.length()-1;//last JSONObject
			}
			else {
				start = blockNum*maxROWS_PER_BLOCK;
				end = (blockNum+1)*maxROWS_PER_BLOCK-1;
			}
			System.out.println("aquest es el start del block "+blockNum+": "+ start);
			System.out.println("aquest es el end del block: "+ end);
			JSONArray partialJSONArray = new JSONArray();
			for(int i = start; i <= end; ++i) {
				if(i == start)System.out.println("start => "+i);
				if(i == end)System.out.println("end => "+i);
				partialJSONArray.put(contentJSONArray.getJSONObject(i));
			}
			System.out.println("el partial JSONArray conté: "+partialJSONArray.length()+" JSONObjects");
			sum+=partialJSONArray.length();
			System.out.println(sum);
			//if blockNum = numBlocks-1 llavors el limit serà de blockNum*REQUEST_maxSIZE_BYTES fins a AdminUnitJSONArray.lenght()-1 (last JSONObject);
			//cridar a fer el encoding
			//String JSONrowPUT = AdministrationUnitDataEncoding(AdminUnitJSONArray, blockNum);
			System.out.println("encoding de les dades del block: "+ blockNum);
			//cridar a fer el encoding de cada un dels partial JSONArray
			//li haig de passar el partialJSONArray i el blockNum al que pertany (per poder generar la rowKey)
			String PUTstring = dataEncoding(partialJSONArray, blockNum, family);
			//un cop tenim el string que hem d'enviar com a body llavors pasem a fer el put mitjançant una REST API call´
			System.out.println("//////////storing "+family+" data, block"+blockNum+"/////////////////");
			exportDataToHBaseTable(PUTstring);
		}
		if(sum==contentJSONArray.length())System.out.println("partial arrays division WELL DONE");

	}
	
	
	public void exportDataToHBaseTable(String content) throws IOException{
		
		OkHttpClient client = new OkHttpClient().newBuilder()
				.build();
		MediaType mediaType = MediaType.parse("application/json");
		RequestBody body = RequestBody.create(content, mediaType);
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
	
	//per no crear la rowKey dins de RequestEncoding
	public void generateRowKey() throws IOException {
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
		
		//DateTime for the completeRowKey
		DateFormat sdf = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS");
		Date date = new Date();
		String currentTime = sdf.format(date).toString();
		//create the complete RowKey, that is, completeRowKey and set it (the other ones will use completeRowKey, global var)
		String crk = rkb+mtdVersion+'$'+currentTime;
		setCompleteRowKey(crk);
		
		//DateTime for new thisWIMEDSextractionDateTime
		String pattern = "yyyy-MM-dd'T'HH:mm:ss.SSSSSS'Z'";
		SimpleDateFormat ZDateFormat = new SimpleDateFormat(pattern);
		WIMEDSextractionTimeGV = ZDateFormat.format(date);
	}
	

	private String dataEncoding(JSONArray adminUnitsJSONArray, int blockNum, String family) {
		
		System.out.println("he entrat a administrationUnitEncoding amb blockNum: "+blockNum);
		//the JSONArray param is a partial JSONArray from the original one.
		//add to the completeRowKey the block identifier $numBlock
		//completeRowKey is set when calling setProps() func
		String stringKey = getCompleteRowKey();
		stringKey = stringKey +'$'+blockNum;
		System.out.println("aquesta es la rowKey del block "+blockNum+": "+stringKey);
		String stringColumn = "Data:"+family;//should be parametrized
		String value = adminUnitsJSONArray.toString();
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
		//update thisWIMEDSExtractionDateTime in control.properties
		props.setProperty("thisWIMEDSextractionDateTime", WIMEDSextractionTimeGV);
		System.out.println("this WIMEDS DB extraction has been done at: "+ WIMEDSextractionTimeGV);
		props.store(out, null);
		out.close();
		/////////
		
		//update completeRowKey for further extractions
		completeRowKeyUpdate(ctrlPath, completeRowKey);
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
		System.out.println(props.getProperty("jsessionid"));
		System.out.println(props.getProperty("bonitaToken"));
		setBonitaToken(props.getProperty("bonitaToken"));
		setWIMEDStablesNames(props.getProperty("WIMEDStablesNames"));
		setURLAPI(props.getProperty("endpointBonitaRESTAPI"));
		//JSESSIONID and X-Bonita-API-Token still not parametrized...I have errors getting the cookie
		in.close();
		//generate the completeRowKey
		generateRowKey();
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
	
	public String getWIMEDStablesNames() {return this.tablesNamesCSV;}
	public void setWIMEDStablesNames(String tablesNamesCSV) {this.tablesNamesCSV = tablesNamesCSV;}
	
	public String getURLAPI() {return this.urlAPI;}
	public void setURLAPI(String urlapi) {this.urlAPI = urlapi;}
	
}
