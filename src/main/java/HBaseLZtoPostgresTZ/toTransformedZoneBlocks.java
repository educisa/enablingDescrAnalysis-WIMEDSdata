package HBaseLZtoPostgresTZ;

import java.io.FileInputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.text.ParseException;
import java.util.Properties;

import org.json.JSONArray;
import org.json.JSONObject;

import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.Response;

public class toTransformedZoneBlocks {
	
	public toTransformedZoneBlocks() {}
	private String tz_DBurl, tz_DBusr, tz_DBpwd, HBaseTableURL, completeRowKey, colFam;
	long tz_thisExtractionTS;
	
	
	public JSONArray getDataFromHBase(String ctrlPath, String colQuali) throws IOException{
		
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
				.url("http://who-dev.essi.upc.edu:60000/WIMEDS-Table-Data/"+crk+"/Data:AdministrationUnit") //use parametrized url
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
		
		
		Path path = Paths.get("outBlockssssssss.txt");
		System.out.println("...content extracted...");
		try {
			Files.writeString(path, content, StandardCharsets.UTF_8);
		} catch (IOException ex) {
			System.out.println("exception");
		}
		
		return content;
	}
	
	
	
	
	
	public void setProperties(String ctrlPath) throws IOException{
		
		//////////
		Properties props = new Properties();
        FileInputStream in = new FileInputStream(ctrlPath);

        props.load(in);
		this.setDBurl(props.getProperty("testDBurl"));
		this.setDBusr(props.getProperty("testDBusr"));
		this.setDBpwd(props.getProperty("testDBpwd"));
		this.setHBaseTableURL(props.getProperty("urlWIMEDSdataTable"));
		this.setthiscompleteRowKey(props.getProperty("completeRowKey"));
		this.setColFam(props.getProperty("family1"));
		
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
}
