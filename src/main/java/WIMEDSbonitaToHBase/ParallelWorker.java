package WIMEDSbonitaToHBase;

import org.json.JSONArray;
import org.json.JSONObject;

public class ParallelWorker extends Thread {
	
	private JSONArray AdminUnit; //JSONArray with all the AdminUnit from the REST API response
	private int low, high;
	private String partialQuery;
	
	
	public ParallelWorker(JSONArray AdminUnit, int low, int high) {
		this.AdminUnit = AdminUnit;
		this.low = low;
		this.high = high;
	}
	
	@Override
	public void run() {
		partialQuery = "";

		for(int i=low; i<high; ++i) {
			if(i == AdminUnit.length()) {System.out.println("reached the end");}
			else {
				//fer totes les operacions per creacio de la query
				JSONObject rowJSONObj = (JSONObject) AdminUnit.get(i);

				String parentid = "";
				if(rowJSONObj.has("parent")) {
					JSONObject JSONObjparent = rowJSONObj.getJSONObject("parent");
					parentid = JSONObjparent.getString("id");
				}

				String url = null;//just some organisationUnits have (e.g., http://www.hospitalclinic.org)
				if(rowJSONObj.has("url")) {
					url = rowJSONObj.getString("url");
					url = url.replaceAll("'","''");
				}

				String address = null;//just some organisationUnits have (e.g., calle Villarroel, 170, 08036 Barcelona, Spain)
				if(rowJSONObj.has("address")) {
					address = rowJSONObj.getString("address");
					address = address.replaceAll("'","''");
				}

				String id, name, shortname, datelastupdated;
				Boolean leaf;
				Integer levelnumber;
				id = rowJSONObj.getString("id");
				name = rowJSONObj.getString("name");
				shortname = rowJSONObj.getString("shortName");
				datelastupdated = rowJSONObj.getString("dateLastUpdated");
				leaf = rowJSONObj.getBoolean("leaf");
				levelnumber = rowJSONObj.getInt("levelNumber");

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
	
	public String getPartialQuery() {
		return this.partialQuery;
	}
}
