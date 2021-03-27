package HBaseToWIMEDSDB;

import java.io.UnsupportedEncodingException;
import java.nio.charset.StandardCharsets;
import java.util.Base64;

import org.json.JSONArray;
import org.json.JSONObject;
import org.json.XML;

public class ParallelWorker extends Thread{
	
	private JSONArray AdminUnit; //JSONArray with all the AdminUnit from the HBase REST API response
	private int low, high;
	private String partialQuery; //partial query inserting AdminUnit in range low,high in AdminUnit JSONArray
	private boolean isUpdate;
	
	public ParallelWorker(JSONArray AdminUnit, int low, int high, boolean isUpdate) {
		this.AdminUnit = AdminUnit;
		this.low = low;
		this.high = high;
		this.isUpdate = isUpdate;
	}
	
	
	@Override
	public void run() {
		partialQuery = "";

		if(isUpdate) {
			for(int i=low; i<high; ++i) {
				//generate query for each adminunit between the range low,high
				JSONObject AdminUnitObj = AdminUnit.getJSONObject(i);
				JSONArray encodedJSONArray = AdminUnitObj.getJSONArray("Cell");
				JSONObject encodedJSONObject = encodedJSONArray.getJSONObject(0);
				String encodedValue = encodedJSONObject.getString("$");
				//decode the encodedValue
				byte[] dataBytes = Base64.getDecoder().decode(encodedValue);
				String decodedValue="";
				try {
					decodedValue = new String(dataBytes, StandardCharsets.UTF_8.name());
				} catch (UnsupportedEncodingException e) {
					e.printStackTrace();
				}
				//transform it to JSON to get the fields we are interested in
				JSONObject resultJSONValue = new JSONObject(decodedValue);


				String parentid = "";
				if(resultJSONValue.has("parent")) {
					JSONObject JSONObjparent = resultJSONValue.getJSONObject("parent");
					parentid = JSONObjparent.getString("id");
				}

				String url = null;//just some organisationUnits have (e.g., http://www.hospitalclinic.org)
				if(resultJSONValue.has("url")) {
					url = resultJSONValue.getString("url");
					url = url.replaceAll("'","''");
				}

				String address = null;//just some organisationUnits have (e.g., calle Villarroel, 170, 08036 Barcelona, Spain)
				if(resultJSONValue.has("address")) {
					address = resultJSONValue.getString("address");
					address = address.replaceAll("'","''");
				}

				String id, name, shortname, datelastupdated;
				Boolean leaf;
				Integer levelnumber;
				id = resultJSONValue.getString("id");
				name = resultJSONValue.getString("name");
				shortname = resultJSONValue.getString("shortName");
				datelastupdated = resultJSONValue.getString("lastUpdated");
				leaf = resultJSONValue.getBoolean("leaf");
				levelnumber = resultJSONValue.getInt("level");
				
				
				partialQuery += "INSERT INTO AdministrationUnit VALUES ('"
						+ id + "','"
						+ parentid + "','"
						+ name.replaceAll("'","''") + "','"
						+ shortname.toString().replaceAll("'","''") + "','"
						+ datelastupdated.substring(0,10) + "',"
						+ leaf + ","
						+ levelnumber + ",'"
						+ address + "','"
						+ url + "') ON CONFLICT ON CONSTRAINT id"
								+ " DO"
								+ "  UPDATE"
								+ "  SET   "
						+ "parentid='"
						+parentid + "',"
						+"name='"
						+name.replaceAll("'","''") + "',"
						+"shortName='"
						+shortname.toString().replaceAll("'","''") + "',"
						+"datelastupdated='"
						+datelastupdated.substring(0,10) + "',"
						+"leaf="
						+leaf + ","
						+"levelnumber="
						+levelnumber+","
						+"address='"
						+address+"',"
						+"url='"
						+url+"'"
						+" WHERE AdministrationUnit.id='"+id+"';\n";
								 
				
				
				//if(id.equals("uo3srkbSp69")) System.out.println(shortname);
			}
		}
		else {
			for(int i=low; i<high; ++i) {
				//generate query for each adminunit between the range low,high
				JSONObject AdminUnitObj = AdminUnit.getJSONObject(i);
				JSONArray encodedJSONArray = AdminUnitObj.getJSONArray("Cell");
				JSONObject encodedJSONObject = encodedJSONArray.getJSONObject(0);
				String encodedValue = encodedJSONObject.getString("$");
				//decode the encodedValue
				byte[] dataBytes = Base64.getDecoder().decode(encodedValue);
				String decodedValue="";
				try {
					decodedValue = new String(dataBytes, StandardCharsets.UTF_8.name());
				} catch (UnsupportedEncodingException e) {
					e.printStackTrace();
				}
				//transform it to JSON to get the fields we are interested in
				JSONObject resultJSONValue = new JSONObject(decodedValue);


				String parentid = "";
				if(resultJSONValue.has("parent")) {
					JSONObject JSONObjparent = resultJSONValue.getJSONObject("parent");
					parentid = JSONObjparent.getString("id");
				}

				String url = null;//just some organisationUnits have (e.g., http://www.hospitalclinic.org)
				if(resultJSONValue.has("url")) {
					url = resultJSONValue.getString("url");
					url = url.replaceAll("'","''");
				}

				String address = null;//just some organisationUnits have (e.g., calle Villarroel, 170, 08036 Barcelona, Spain)
				if(resultJSONValue.has("address")) {
					address = resultJSONValue.getString("address");
					address = address.replaceAll("'","''");
				}

				String id, name, shortname, datelastupdated;
				Boolean leaf;
				Integer levelnumber;
				id = resultJSONValue.getString("id");
				name = resultJSONValue.getString("name");
				shortname = resultJSONValue.getString("shortName");
				datelastupdated = resultJSONValue.getString("lastUpdated");
				leaf = resultJSONValue.getBoolean("leaf");
				levelnumber = resultJSONValue.getInt("level");
				
				
				partialQuery += "INSERT INTO AdministrationUnit VALUES ('"
						+ id + "','"
						+ parentid + "','"
						+ name.replaceAll("'","''") + "','"
						+ shortname.toString().replaceAll("'","''") + "','"
						+ datelastupdated.substring(0,10) + "',"
						+ leaf + ","
						+ levelnumber + ",'"
						+ address + "','"
						+ url + "')";

			}
		}
	}
		
	
	public String getPartialQuery() {
		return this.partialQuery;
	}

	
}
