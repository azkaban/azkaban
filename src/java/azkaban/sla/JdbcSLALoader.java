/*
 * Copyright 2012 LinkedIn Corp.
 * 
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package azkaban.sla;

import java.io.IOException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.commons.dbutils.DbUtils;
import org.apache.commons.dbutils.QueryRunner;
import org.apache.commons.dbutils.ResultSetHandler;
import org.apache.log4j.Logger;
import org.joda.time.DateTime;

import azkaban.sla.SLA.SlaRule;
import azkaban.database.AbstractJdbcLoader;
import azkaban.utils.GZIPUtils;
import azkaban.utils.JSONUtils;
import azkaban.utils.Props;

public class JdbcSLALoader extends AbstractJdbcLoader implements SLALoader {
	private static final Logger logger = Logger.getLogger(JdbcSLALoader.class);
	private EncodingType defaultEncodingType = EncodingType.GZIP;
	
	private static String slaTblName = "active_sla";
	
	final private static String LOAD_ALL_SLA =
			"SELECT exec_id, job_name, check_time, rule, enc_type, options FROM " + slaTblName;
	
	final private static String INSERT_SLA = 
			"INSERT INTO " + slaTblName + " ( exec_id, job_name, check_time, rule, enc_type, options) values (?,?,?,?,?,?)";

//	final private static String UPDATE_SLA = 
//			"UPDATE " + slaTblName + " SET exec_id, job_name, check_time, rule, enc_type, options) values (?,?,?,?,?,?)";
//	
	// use "" for flow
	final private static String REMOVE_SLA = 
			"DELETE FROM " + slaTblName + " WHERE exec_id=? AND job_name=? AND check_time=? AND rule=?";
	
	public JdbcSLALoader(Props props) {
		super(props);
	}

	private Connection getConnection() throws SLAManagerException {
		Connection connection = null;
		try {
			connection = super.getDBConnection(false);
		} catch (Exception e) {
			DbUtils.closeQuietly(connection);
			throw new SLAManagerException("Error getting DB connection.", e);
		}
		
		return connection;
	}
	
	public EncodingType getDefaultEncodingType() {
		return defaultEncodingType;
	}

	public void setDefaultEncodingType(EncodingType defaultEncodingType) {
		this.defaultEncodingType = defaultEncodingType;
	}

	@Override
	public List<SLA> loadSLAs() throws SLAManagerException {
		logger.info("Loading all SLAs from db.");
		
		Connection connection = getConnection();
		List<SLA> SLAs;
		try{
			SLAs= loadSLAs(connection, defaultEncodingType);
		}
		catch(SLAManagerException e) {
			throw new SLAManagerException("Failed to load SLAs" + e.getCause());
		}
		finally{
			DbUtils.closeQuietly(connection);
		}
				
		logger.info("Loaded " + SLAs.size() + " SLAs.");
		
		return SLAs;

	}

	public List<SLA> loadSLAs(Connection connection, EncodingType encType) throws SLAManagerException {
		logger.info("Loading all SLAs from db.");
		
		QueryRunner runner = new QueryRunner();
		ResultSetHandler<List<SLA>> handler = new SLAResultHandler();
		List<SLA> SLAs;
		
		try {
			SLAs = runner.query(connection, LOAD_ALL_SLA, handler);
		} catch (SQLException e) {
			logger.error(LOAD_ALL_SLA + " failed.");
			throw new SLAManagerException("Load SLAs from db failed. "+ e.getCause());
		}
		
		return SLAs;

	}
	
	@Override
	public void removeSLA(SLA s) throws SLAManagerException {
		Connection connection = getConnection();
		try{
			removeSLA(connection, s);
		}
		catch(SLAManagerException e) {
			logger.error(LOAD_ALL_SLA + " failed.");
			throw new SLAManagerException("Load SLAs from db failed. "+ e.getCause());
		}
		finally {
			DbUtils.closeQuietly(connection);
		}
		
	}
	
	private void removeSLA(Connection connection, SLA s) throws SLAManagerException {

		logger.info("Removing SLA " + s.toString() + " from db.");

		QueryRunner runner = createQueryRunner();
	
		try {
			int removes =  runner.update(REMOVE_SLA, s.getExecId(), s.getJobName(), s.getCheckTime().getMillis(), s.getRule().getNumVal());
			if (removes == 0) {
				throw new SLAManagerException("No schedule has been removed.");
			}
		} catch (SQLException e) {
			logger.error("Remove SLA failed. " + s.toString());
			throw new SLAManagerException("Remove SLA " + s.toString() + " from db failed. "+ e.getCause());
		}
		
	}

	@Override
	public	void insertSLA(SLA s) throws SLAManagerException {
		Connection connection = getConnection();
		try{
			insertSLA(connection, s, defaultEncodingType);
		}
		catch(SLAManagerException e){
			logger.error("Insert SLA failed. " + s.toString());
			throw new SLAManagerException("Insert SLA " + s.toString() + " into db failed. "+ e.getCause() + e.getMessage() + e.getStackTrace());
		}
		finally {
			DbUtils.closeQuietly(connection);
		}
	}
	
	private void insertSLA(Connection connection, SLA s, EncodingType encType) throws SLAManagerException {

		logger.debug("Inserting new SLA into DB. " + s.toString());
		
		QueryRunner runner = new QueryRunner();

		String json = JSONUtils.toJSON(s.optionToObject());
		byte[] data = null;
		try {
			byte[] stringData = json.getBytes("UTF-8");
			data = stringData;
	
			if (encType == EncodingType.GZIP) {
				data = GZIPUtils.gzipBytes(stringData);
			}
			logger.debug("NumChars: " + json.length() + " UTF-8:" + stringData.length + " Gzip:"+ data.length);
		}
		catch (IOException e) {
			throw new SLAManagerException("Error encoding the sla options.");
		}
		
		try {
			int inserts = runner.update(connection, INSERT_SLA, s.getExecId(), s.getJobName(), s.getCheckTime().getMillis(), s.getRule().getNumVal(), encType.getNumVal(), data);

			if (inserts == 0) {
				throw new SLAManagerException("No SLA has been inserted. Insertion failed.");
			}
		} catch (SQLException e) {
			logger.error("Insert SLA failed.");
			throw new SLAManagerException("Insert sla " + s.toString() + " into db failed. " + e.getCause());
		}
		
	}
	
//	@Override
//	public	void updateSLA(SLA s) throws SLAManagerException {
//		Connection connection = getConnection();
//		try{
//			insertSLA(connection, s, defaultEncodingType);
//		}
//		catch(SLAManagerException e){
//			logger.error("Insert SLA failed. " + s.toString());
//			throw new SLAManagerException("Insert SLA " + s.toString() + " into db failed. "+ e.getCause() + e.getMessage() + e.getStackTrace());
//		}
//		finally {
//			DbUtils.closeQuietly(connection);
//		}
//	}
//	
//	private void updateSLA(Connection connection, SLA s, EncodingType encType) throws SLAManagerException {
//
//		logger.debug("Inserting new SLA into DB. " + s.toString());
//		
//		QueryRunner runner = new QueryRunner();
//
//		String json = JSONUtils.toJSON(s.optionToObject());
//		byte[] data = null;
//		try {
//			byte[] stringData = json.getBytes("UTF-8");
//			data = stringData;
//	
//			if (encType == EncodingType.GZIP) {
//				data = GZIPUtils.gzipBytes(stringData);
//			}
//			logger.debug("NumChars: " + json.length() + " UTF-8:" + stringData.length + " Gzip:"+ data.length);
//		}
//		catch (IOException e) {
//			throw new SLAManagerException("Error encoding the sla options.");
//		}
//		
//		try {
//			int inserts = runner.update(connection, INSERT_SLA, s.getExecId(), s.getJobName(), s.getCheckTime().getMillis(), s.getRule().getNumVal(), encType.getNumVal(), data);
//
//			if (inserts == 0) {
//				throw new SLAManagerException("No SLA has been inserted. Insertion failed.");
//			}
//		} catch (SQLException e) {
//			logger.error("Insert SLA failed.");
//			throw new SLAManagerException("Insert sla " + s.toString() + " into db failed. " + e.getCause());
//		}
//		
//	}
	
	public class SLAResultHandler implements ResultSetHandler<List<SLA>> {
		@Override
		public List<SLA> handle(ResultSet rs) throws SQLException {
			if (!rs.next()) {
				return Collections.<SLA>emptyList();
			}
			
			ArrayList<SLA> SLAs = new ArrayList<SLA>();
			
			do {
				int execId = rs.getInt(1);
				String jobName = rs.getString(2);
				DateTime checkTime = new DateTime(rs.getLong(3));
				SlaRule rule = SlaRule.fromInteger(rs.getInt(4));
				int encodingType = rs.getInt(5);
				byte[] data = rs.getBytes(6);
				
				SLA s = null;

				EncodingType encType = EncodingType.fromInteger(encodingType);
				Object optsObj;
				try {
					// Convoluted way to inflate strings. Should find common package or helper function.
					if (encType == EncodingType.GZIP) {
						// Decompress the sucker.
						String jsonString = GZIPUtils.unGzipString(data, "UTF-8");
						optsObj = JSONUtils.parseJSONFromString(jsonString);
					}
					else {
						String jsonString = new String(data, "UTF-8");
						optsObj = JSONUtils.parseJSONFromString(jsonString);
					}						
					s = SLA.createSlaFromObject(execId, jobName, checkTime, rule, optsObj);
				} catch (IOException e) {
					throw new SQLException("Error reconstructing SLA options. " + execId + " " + jobName + " " + checkTime.toDateTimeISO() + e.getCause());
				}
				SLAs.add(s);

			} while (rs.next());
			
			return SLAs;
		}
		
	}

}
