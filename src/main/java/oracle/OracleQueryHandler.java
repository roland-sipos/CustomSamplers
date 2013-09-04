package oracle;

import java.io.ByteArrayInputStream;
import java.sql.Connection;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.HashMap;

import customjdbc.CustomJDBCConfigElement;

import utils.CustomSamplersException;
import utils.NotFoundInDBException;
import utils.QueryHandler;

public class OracleQueryHandler implements QueryHandler {

  private static Connection connection;
  private static String payloadTableName = "PAYLOAD";
  private static String tagTableName = "TAG";
  private static String iovTableName = "IOV";
	
  public OracleQueryHandler(String databaseName) 
          throws CustomSamplersException, NotFoundInDBException {
    connection = CustomJDBCConfigElement.getJDBCConnection(databaseName);
    if (connection == null) {
      throw new NotFoundInDBException("JDBCConnection instance with name: " + databaseName + " was not found in config!");
    }
  }

  @Override
  public void writeBinary(String binaryID, String chunkID, String hash,
                          byte[] fileContent, boolean isSpecial)
          throws CustomSamplersException {
    // DEPRECATED.
  }

  @Override
  public byte[] readBinary(String binaryID, String chunkID, String hash, boolean isSpecial)
          throws CustomSamplersException {
    // DEPRECATED.
    return null;
  }

  @Override
  public void writePayload(HashMap<String, String> metaInfo,
                           byte[] payload, byte[] streamerInfo, boolean isSpecial)
          throws CustomSamplersException {
    PreparedStatement ps;
    try {
      ps = connection.prepareStatement("INSERT INTO " + payloadTableName
         + " (HASH, OBJECT_TYPE, DATA, STREAMER_INFO, VERSION, CREATION_TIME, CMSSW_RELEASE)"
         + " VALUES (?, ?, ?, ?, ?, ?, ?)");
      ps.setString(1, metaInfo.get("hash"));
      ps.setString(2, metaInfo.get("object_type"));
      ps.setBinaryStream(3, new ByteArrayInputStream(payload), payload.length);
      ps.setBinaryStream(4, new ByteArrayInputStream(streamerInfo), streamerInfo.length);
      ps.setString(5, metaInfo.get("version"));
      ps.setTimestamp(6, new Timestamp(System.currentTimeMillis()));
      ps.setString(6, metaInfo.get("creation_time"));
      ps.setString(7, metaInfo.get("cmssw_release"));
      ps.execute();
      ps.close();
    } catch (Exception se) { // ORIGINAL: SQL exception.
      throw new CustomSamplersException("SQLException occured during write attempt: " + se.toString());
    }
  }

  @Override
  public byte[] readPayload(HashMap<String, String> metaMap, boolean isSpecial) 
          throws CustomSamplersException {
    PreparedStatement ps;
    byte[] result = null;
    try {
      ps = connection.prepareStatement("SELECT DATA FROM " + payloadTableName + " WHERE HASH=?");
      ps.setString(1, metaMap.get("hash"));
      ResultSet rs = ps.executeQuery();

      if (rs != null) {
        int counter = 0;
        while(rs.next()) {
          result = rs.getBytes("DATA");
          counter++;
        }
        if (counter > 1) {
          throw new CustomSamplersException("More than one row found with hash="
                  + metaMap.get("hash") + " in " + payloadTableName + " !");
        }
        rs.close();

      } else {

        throw new CustomSamplersException("The row with hash=" + metaMap.get("hash")
                + " not found in the database!");
      }

      ps.close();
    } catch (SQLException e) {
      throw new CustomSamplersException("SQLException occured during read attempt: " + e.toString());
    }
    return result;
  }

  public void writeTag(HashMap<String, String> stringMetaInfo, HashMap<String, Integer> intMetaInfo, boolean isSpecial)
          throws CustomSamplersException {
    PreparedStatement ps;
    try {
      ps = connection.prepareStatement("INSERT INTO " + tagTableName
              + " (NAME, REVISION, REVISION_TIME, COMMENT, TIME_TYPE, OBJECT_TYPE,"
              + " LAST_VALIDATED, END_OF_VALIDITY, LAST_SINCE, LAST_SINCE_PID, CREATION_TIME)"
              + " VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)");
      ps.setString(1, stringMetaInfo.get("name"));
      ps.setInt(2, intMetaInfo.get("revision"));
      ps.setDate(3, new Date(Long.parseLong(stringMetaInfo.get("revision_time"))));
      ps.setString(4, stringMetaInfo.get("comment"));
      ps.setInt(5, intMetaInfo.get("time_type"));
      ps.setString(6, stringMetaInfo.get("object_type"));
      ps.setInt(7, intMetaInfo.get("last_validated_time"));
      ps.setInt(8, intMetaInfo.get("end_of_validity"));
      ps.setInt(9, intMetaInfo.get("last_since"));
      ps.setInt(10, intMetaInfo.get("last_since_pid"));
      ps.setTimestamp(11, new Timestamp(System.currentTimeMillis()));
    } catch (SQLException se) {
      throw new CustomSamplersException("SQLException occured during write attempt: " + se.toString());
    }
  }

  @Override
  public void writeIov(HashMap<String, String> keyAndMetaMap)
          throws CustomSamplersException {
    PreparedStatement ps;
    try {
      ps = connection.prepareStatement("INSERT INTO " + iovTableName
              + " (TAG_NAME, SINCE, PAYLOAD_HASH, INSERT_TIME) VALUES (?, ?, ?, ?)");
      ps.setString(1, keyAndMetaMap.get("tag_name"));
      ps.setInt(2, Integer.parseInt(keyAndMetaMap.get("since")));
      ps.setString(3, keyAndMetaMap.get("payload_hash"));
      ps.setDate(4, new Date(System.currentTimeMillis()));
    } catch (SQLException se) {
      throw new CustomSamplersException("SQLException occured during write attempt: " + se.toString());
    }
  }

  @Override
  public String readIov(HashMap<String, String> keyMap)
          throws CustomSamplersException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public void writeTag(HashMap<String, String> metaMap)
          throws CustomSamplersException {
    // TODO Auto-generated method stub
  }

  @Override
  public HashMap<String, Object> readTag(String tagKey)
          throws CustomSamplersException {
    // TODO Auto-generated method stub
    return null;
  }

}

