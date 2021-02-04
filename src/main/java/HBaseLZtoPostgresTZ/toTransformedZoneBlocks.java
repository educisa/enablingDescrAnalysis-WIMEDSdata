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

import org.json.JSONArray;
import org.json.JSONObject;

import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.Response;

public class toTransformedZoneBlocks {
	
	public toTransformedZoneBlocks() {}
	private String ctrlPath;
	private String tz_DBurl, tz_DBusr, tz_DBpwd, HBaseTableURL, completeRowKey, colFam, tablesNamesCSV;
	long tz_thisExtractionTS;
	
	
	//initial func
	public void exportDataToTransformedZone() throws IOException, InterruptedException, ParseException {
		
		String[] tables = tablesNamesCSV.split(",");
		for(int i = 0; i< tables.length; ++i) {
			System.out.println("extracting "+tables[i]+" data");
			JSONArray jsonA = getDataFromHBase(tables[i]);
			String data = jsonA.toString();
			String SQLQuery = "";
			System.out.println("el tamany total de les dades de "+tables[i]+": "+ jsonA.length());
			//ara aqui ja tinc tot el JSONArray de una taula en concret
			//call function in order to generate SQLQueries
			/*depenen de quina taula es tracti faré multithreading o no
			per generar les queries utilitzant multithreading només ho tinc fet per AdministrationUnit i Request
			per les altres de moment NO s'utilitza multithreading i es generaran les SQL queries en una funcio d'aquest fitxer
			TINC en tablesMultiThread a control.properties les taules que vull utilitzar java multithreading
			HA D'ESTAR IMPLEMENTAT...(ara només hi tinc=AdministrationUnit,Request
			*/
			//haig de cridar a un generador de queries o a un altre
			if(tables[i].equals("Request")) {
				System.out.println("generant RequestQuery");
				SQLQuery=getRequestQuery(data);
			}
			else if(tables[i].equals("Manufacturer")) {
				System.out.println("generant ManufacturerQuery");
				SQLQuery=getManufacturerQuery(data);
			}
			else if(tables[i].equals("Disease")) {
				System.out.println("generant DiseaseQuery");
				SQLQuery=getDiseaseQuery(data);
			}
			else if(tables[i].equals("AdministrationUnit")) {
				fullextractAdminUnitmultiThreading orgUnitsExtr = new fullextractAdminUnitmultiThreading();
				orgUnitsExtr.setProperties(ctrlPath);
				SQLQuery= orgUnitsExtr.getSQLQueryAdminUnit();//implemented a part since it is not directly coming from WIMEDS-Table-Data
			}
			
			//if(...)
			
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
	public String getRequestQuery(String requestData) throws InterruptedException, IOException, ParseException {
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
	
	
	
}
