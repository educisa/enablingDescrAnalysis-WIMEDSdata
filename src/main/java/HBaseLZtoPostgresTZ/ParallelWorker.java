package HBaseLZtoPostgresTZ;

import java.io.UnsupportedEncodingException;
import java.nio.charset.StandardCharsets;
import java.util.Base64;

import org.json.JSONArray;
import org.json.JSONObject;

public class ParallelWorker extends Thread{
	
	private JSONArray content; //JSONArray with all the AdminUnit or Request from the REST API response
	private int low, high;
	private String partialQuery; //partial query inserting AdminUnit in range low,high in AdminUnit JSONArray
	private String whichData;
	
	
	public ParallelWorker(JSONArray content, int low, int high, String whichData) {
		this.content = content;
		this.low = low;
		this.high = high;
		this.whichData = whichData;
	}
	
	
	@Override
	public void run() {
		partialQuery = "";

		if(this.whichData.equals("AdministrationUnit")) {

			for(int i=low; i<high; ++i) {
				if(i == content.length()) {System.out.println("reached the end");}
				else {
					//generate query for each adminunit between the range low,high
					JSONObject AdminUnitObj = content.getJSONObject(i);
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
							+ url + "');\n";

				}
			}
		}
		
		if(this.whichData.equals("Request")) {
			for(int i = low;i<high; ++i) {
				int id, diseaseID, medicalSupplyID, requestStatus, weightInKg, age;
				String countryID, healthFacilityID, requestDateString, healthFacility, phase, transmissionWay;
				
				JSONObject requestJSONObj = content.getJSONObject(i);
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
		
				partialQuery += "INSERT INTO request VALUES ("
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
						+ transmissionWay +"');\n";
			}
			
			
		}
	}
	
	public String getPartialQuery() {
		return this.partialQuery;
	}

}
