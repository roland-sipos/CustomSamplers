package postgresql;

import org.apache.commons.io.*;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;
import java.nio.ByteBuffer;
import java.sql.Connection;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.HashMap;
import java.util.List;
import java.util.Random;
import java.util.TreeMap;

import org.postgresql.largeobject.LargeObject;
import org.postgresql.largeobject.LargeObjectManager;
import org.postgresql.util.PGobject;

import customjdbc.CustomJDBCConfigElement;
import utils.CustomSamplersException;
import utils.QueryHandler;

public class PostgreJSONQueryHandler implements QueryHandler {

	private static Connection connection;
	
	public PostgreJSONQueryHandler(String connectionId) throws CustomSamplersException{
		connection = CustomJDBCConfigElement.getJDBCConnection(connectionId);
		
		if (connection == null)
			throw new CustomSamplersException("JDBCConnection instance with name: " + connectionId + " was not found in config!");
	}
	
	@Override
	public void closeResources() throws CustomSamplersException {
		try {
			connection.close();
		} catch (SQLException e) {
			throw new CustomSamplersException("SQLException occured during read attempt: " + e.toString());
		}
	}

	@Override
	public ByteBuffer getData(String tagName, long since) throws CustomSamplersException {

		String url = "http://postgre-node.cern.ch:3000/iov?data->>name=eq."+tagName +"&data->>since=eq."+since +"";
		
		try {
			URL obj = new URL(url);
			HttpURLConnection con = (HttpURLConnection) obj.openConnection();
			
			con.setRequestMethod("GET");
			//System.out.println("\nSending 'GET' request to URL : " + url);
			
			BufferedReader streamReader = new BufferedReader(new InputStreamReader(con.getInputStream())); 
			StringBuilder responseStrBuilder = new StringBuilder();
			
			String inputStr;
			while ((inputStr = streamReader.readLine()) != null)
			    responseStrBuilder.append(inputStr);
			
			JSONObject jsn = new JSONObject(responseStrBuilder.substring(1, responseStrBuilder.length()-1));
			String hash = jsn.getJSONObject("data").getString("hash");
			//System.out.println(hash);
			//con.disconnect();

			//*********************SECOND CONNECTION****************************
			
			String url1 = "http://postgre-node.cern.ch:3000/payload?data->>hash=eq."+hash;
					
			URL obj1 = new URL(url1);
			HttpURLConnection con1 = (HttpURLConnection) obj1.openConnection();
					
			con1.setRequestMethod("GET");		
			//System.out.println("\nSending 'GET' request to URL : " + url1);
			
			InputStream inpStr = con1.getInputStream();
			byte[] blob = IOUtils.toByteArray(inpStr);
			ByteBuffer buff = ByteBuffer.wrap(blob);
			//con1.disconnect();
			
			return buff;
			
			
		} catch (MalformedURLException e) {
			throw new CustomSamplersException("SQLException occured during read attempt: " + e.toString());
		} catch (IOException e) {
			throw new CustomSamplersException("SQLException occured during read attempt: " + e.toString());
		} catch (JSONException e) {
			throw new CustomSamplersException("SQLException occured during read attempt: " + e.toString());
		}
		
	}
	
	/*public ByteBuffer getData(String tagName, long since) throws CustomSamplersException {
		// TODO Auto-generated method stub
		ByteBuffer result = null;
		try {
			String queryIOV = "SELECT iov.data->>'hash' AS hash FROM iov WHERE iov.data->>'name'='"+tagName +"' AND iov.data->>'since'='"+since +"';";
			String queryPL = "SELECT payload.blob AS blob FROM payload WHERE payload.data->>'hash'="; //?;";
			
			ResultSet res = connection.createStatement().executeQuery(queryIOV);
			
			
			String hashToLookup = "";
			if (res != null) {
				while(res.next()) {
					hashToLookup = res.getString("hash");
				}
			} else {
				throw new CustomSamplersException("Hash not found for "
						+ "TAG=" + tagName + " SINCE=" + since +" !");
			}

			String plQ = new StringBuilder().append(queryPL)
											.append("'").append(hashToLookup).append("'")
											.append(";").toString();

			ResultSet res2 = connection.createStatement().executeQuery(plQ);
			if (res2 != null){
				while (res2.next()) {
					result = ByteBuffer.wrap(res2.getBytes("blob"));
				}
				res2.close();
			} else {
				throw new CustomSamplersException("Payload not found for "+ "hash= " + hashToLookup);
			}
		} catch (SQLException e) {
			throw new CustomSamplersException("SQLException occured during read attempt: " + e.toString());
		}
		return result;
	}*/

	@Override
	public void putData(HashMap<String, String> metaInfo, ByteArrayOutputStream payload,
			ByteArrayOutputStream streamerInfo) 
		throws CustomSamplersException {
		// TODO Auto-generated method stub
		try {
			PreparedStatement ps = connection.prepareStatement("INSERT INTO PAYLOAD"
					+ " (data, blob, streamer_info)"
					+ " VALUES (?, ?, ?)");
			String yourJsonString = "{\"hash\":\""+metaInfo.get("payload_hash") +"\",\"object_type\":\""+metaInfo.get("object_type") +"\",\"version\":\"" +metaInfo.get("version") + "\",\"creation_time\":"+System.currentTimeMillis() +",\"csmsw_release\":\""+metaInfo.get("cmssw_release") +"\"}";                                 
			PGobject jsonObject = new PGobject();
			jsonObject.setType("json");
			jsonObject.setValue(yourJsonString);			
			ps.setObject(1, jsonObject);
			ps.setBytes(2, payload.toByteArray());
			ps.setBytes(3, streamerInfo.toByteArray());
			ps.execute();	
			ps.close();
		} catch (SQLException se) {
			throw new CustomSamplersException("SQLException occured during write attempt: " + se.toString());
		}
		try {
			/*PreparedStatement ps = connection.prepareStatement("INSERT INTO IOV"
					+ " (data) VALUES (?)");
			String yourJsonString = "{\"name\":\""+metaInfo.get("tag_name") +"\",\"since\":"+metaInfo.get("since") +",\"hash\":\"" +metaInfo.get("payload_hash") + "\",\"creation_time\":"+System.currentTimeMillis() +"}";                                 
			PGobject jsonObject = new PGobject();
			jsonObject.setType("json");
			jsonObject.setValue(yourJsonString);			
			ps.setObject(1, jsonObject);
			ps.execute();
			ps.close();*/
			PreparedStatement ps = connection.prepareStatement("INSERT INTO IOV"
					+ " (TAG_NAME, SINCE, PAYLOAD_HASH, INSERT_TIME) VALUES (?, ?, ?, ?)");
			ps.setString(1, metaInfo.get("tag_name"));
			ps.setLong(2, Long.parseLong(metaInfo.get("since")));
			ps.setString(3, metaInfo.get("payload_hash"));
			ps.setDate(4, new Date(System.currentTimeMillis()));
			ps.execute();
			ps.close();
		} catch (SQLException se) {
			throw new CustomSamplersException("SQLException occured during write attempt: " + se.toString());
		}

	}

	@Override
	public TreeMap<Integer, ByteBuffer> getChunks(String tagName, long since) throws CustomSamplersException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void putChunks(HashMap<String, String> metaInfo, List<ByteArrayOutputStream> chunks)
			throws CustomSamplersException {
		// TODO Auto-generated method stub

	}

}
