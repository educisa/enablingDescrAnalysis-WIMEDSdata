package WIMEDSbonitaToHBase;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.CookieHandler;
import java.net.CookieManager;
import java.net.CookiePolicy;
import java.net.URL;
import java.net.URLConnection;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.Locale;
import java.util.Properties;

import org.json.JSONArray;
import org.json.JSONML;
import org.json.JSONObject;

import okhttp3.Cookie;
import okhttp3.CookieJar;
import okhttp3.Headers;
import okhttp3.HttpUrl;
import okhttp3.MediaType;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.RequestBody;
import okhttp3.Response;
import okhttp3.ResponseBody;

public class WIMEDSextraction {
	
	public WIMEDSextraction() {}
	
	public static void main(String[] args) throws IOException, Exception{
		
		String rootPath = Thread.currentThread().getContextClassLoader().getResource("").getPath();
		String ctrlPath = rootPath + "control.properties";
		
		Properties prop = new Properties();
		
		HbaseUtilities hbase = new HbaseUtilities(ctrlPath);
		hbase.createDataTable();
		
		/*cada cop que es faci extraction/update de la data mirar si es necessari o no extreure metadata from the RequestMedicini process
		 * fer crida a bpm REST API de Bonita per que em proporcioni el lastUpdated
		 * si lastUpdated=>thisWIMEDSextractionDateTime llavors s'ha d'extreure metadata i data(canviar la metadataVersion i DateTime a la key)
		 * si lastUpdated<thisWIMEDSextractionDateTime llavors només s'extreu la data(i només canvian el DateTime de la key).
		 */
		
		ReqMedProcessMetadataExtraction rm= new ReqMedProcessMetadataExtraction();
		rm.getLastUpdatedDate(ctrlPath);
		boolean metadataUpdateNeeded = rm.updatemeta(ctrlPath);
		
		/*NOT IMPLEMENTED:
		{ if(metadataUpdateNeeded)=>extract metadata from WIMEDS DB(bonita) and update it in HBase WICD }*/
		
		IndividualRequestExtraction ire = new IndividualRequestExtraction();
		ire.setGlobalVars(ctrlPath);
		
		ire.exportRequests();
		
		String crk = ire.getCompleteRowKey();
		ire.completeRowKeyUpdate(ctrlPath, crk);
		
		ire.exportShipments();
		ire.exportRequestDocuments();
		ire.exportDisease();
		ire.exportRequestStatus();
		ire.exportMedicalSupply();
		ire.exportManufacturer();
		
		ire.updateWIMEDSextractionDateTime(ctrlPath);

	}

}

