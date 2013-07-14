package com.zhimatech.mofi.datatrim;

import com.zhimatech.mofi.util.*;
import java.sql.ResultSet;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

public class DataProvider {
	private static final String DATABASE_TABLE_MOFIADATA= "mofidata";
	private static final String DATABASE_TABLE_RAW_FLOW = "raw_flow";
	private static final String DATABASE_TABLE_CUSTOMER_FLOW = "customer_flow";
	private static final String DATABASE_TABLE_STAY_TIME_AVG = "stay_time_avg";

	private static final String MOFIADATA_MAC_COLUMN = "mac";
	private static final String MOFIADATA_DEVICESID_COLUMN = "loc";
	private static final String MOFIADATA_STAMPTIME_COLUMN = "stamptime";

	private static final String RAW_FLOW_DEVICESID_COLUMN = "devicesID";
	private static final String RAW_FLOW_DATE_COLUMN = "date";

	private static final String CUSTOMER_FLOW_DEVICESID_COLUMN = "devicesID";
	private static final String CUSTOMER_FLOW_DATE_COLUMN = "date";
	
	private static final String STAY_TIME_AVG_DEVICESID_COLUMN = "devicesID";
	private static final String STAY_TIME_AVG_DATE_COLUMN = "date";
	private static final String STAY_TIME_AVG_TIME_COLUMN = "avg_time";
	
	private static final String HOUR_COLUMNS = "hour_01,hour_02,hour_03,hour_04,hour_05,hour_06,hour_07,hour_08,hour_09,hour_10,hour_11,hour_12" +
			",hour_13,hour_14,hour_15,hour_16,hour_17,hour_18,hour_19,hour_20,hour_21,hour_22,hour_23,hour_24";

	private SimpleDateFormat DATAFORMAT = new SimpleDateFormat("yyyyMMdd");

	private static final String TAG = "DataProvider";
	private static final boolean DBG = false;
	private static final boolean VDBG = true;

	private ArrayList<String> mDevices = new ArrayList<String>();
	private ArrayList<String> mDate = new ArrayList<String>();
	private HashMap<String, ArrayList<String>> mdateWithDevices = new HashMap<String, ArrayList<String>>(); //Used to update all from mofidata
	private HashMap<String, ArrayList<String>> mNeedUpdateDateWithDevices = new HashMap<String, ArrayList<String>>(); //used to update the changed info from mofidata

	public void queryDevices() {
		MySQL db = new MySQL();
		if(db.connSQL()) {
			String sql = "select distinct loc from "+ DATABASE_TABLE_MOFIADATA;;
			ResultSet rs = db.selectSQL(sql);
			try {
				while(rs.next()) {
					log("Query devices from mofidata: " + rs.getString("loc"));
					//String sDevices = new String(rs.getString("loc"));
					if(!mDevices.contains(rs.getString("loc"))) {
						mDevices.add(rs.getString("loc"));
					}
				}
			} catch (Exception e) {
				log("Exception in query devices in datamofi");
			}
			db.deconnSQL();
		}
	}
	public boolean isExist(String devicesID, String date, String dataBaseTable) {
		boolean isExist = false;
		MySQL db = new MySQL();
		if(db.connSQL()) {
			String sql = "select * from "+ dataBaseTable +" where devicesID='" + devicesID + "' and DATE_FORMAT(date, '%Y%m%d')='" + date + "'";
			log("isExit sql= " + sql);
			ResultSet rs = db.selectSQL(sql);
			try {
				if(rs.next()) {
					isExist = true;
				}
			} catch (Exception e) {
			}
			db.deconnSQL();
		}
		return isExist;
	}
	public void queryDate() {
		MySQL db = new MySQL();
		if(db.connSQL()) {
			String sql = "select distinct stamptime from "+ DATABASE_TABLE_MOFIADATA;
			ResultSet rs = db.selectSQL(sql);
			try {
				while(rs.next()) {
					log("Query stamptime from mofidata: " + rs.getDate("stamptime"));
					String sDate = DATAFORMAT.format(rs.getDate("stamptime"));
					log("query date sDate= " + sDate);
					if(mDate.contains(sDate)) {
						log("query already has this date in mDate");
					} else {
						mDate.add(sDate);
					}
				}
			} catch (Exception e) {
				log("Exception in query date in datamofi");
			}
			db.deconnSQL();
		}
	}
	public void queryDateWithDevices() {
		MySQL db = new MySQL();
		for(int i = 0; i < mDevices.size(); i++) {
			if(db.connSQL()) {
				String devicesID = mDevices.get(i);
				ArrayList<String> dateArrayList = mdateWithDevices.get(devicesID);
				String sql = "select distinct stamptime from "+ DATABASE_TABLE_MOFIADATA + " where loc='" + devicesID + "'";
				log("queryDateWithDevices sql= " + sql);
				ResultSet rs = db.selectSQL(sql);
				try {
					while(rs.next()) {
						log("Query stamptime from mofidata with devicesID: " + rs.getDate("stamptime"));
						String sDate = DATAFORMAT.format(rs.getDate("stamptime"));
						log("query date sDate= " + sDate);
						if(dateArrayList == null) {
							dateArrayList = new ArrayList<String>();
						}
						if(dateArrayList.contains(sDate)) {
							log("query already has this date in mdateWithDevices");
						} else {
							dateArrayList.add(sDate);
							mdateWithDevices.put(devicesID, dateArrayList);
						}
					}
				} catch (Exception e) {
					log("Exception in query date in datamofi");
				}
				db.deconnSQL();
			}
		}
	}
	public int[] getNumberHour(String deviceID, String date) {
		HashMap<Integer, Integer> numberHour = new HashMap<Integer, Integer>();
		int[] numOfHour = new int[24];
		MySQL db = new MySQL();
		for(int i = 0; i < 24; i++) {
			if(db.connSQL()) {
				String str1 = String.format("%02d", i);
				String sql = "select count(*) from " + DATABASE_TABLE_MOFIADATA + " where loc='" + deviceID + "' and DATE_FORMAT(stamptime, '%Y%m%d%H')='" + date + str1 + "'";
				log("getNumberHour sql: " + sql);
				ResultSet rs = db.selectSQL(sql);
				try {
					while(rs.next()) {
						log("Query devices from mofidata: " + sql + "\t" + rs.getInt("count(*)"));
						numberHour.put(i, rs.getInt("count(*)"));
						numOfHour[i] = rs.getInt("count(*)");
					}
				} catch (Exception e) {
					log("Exception in getNumberHour in datamofi");
				}
				db.deconnSQL();
			}
		}
		return numOfHour;
	}
	public int[] getNumberHourDistinct(String deviceID, String date) {
		HashMap<Integer, Integer> numberHour = new HashMap<Integer, Integer>();
		int[] numOfHour = new int[24];
		MySQL db = new MySQL();
		for(int i = 0; i < 24; i++) {
			if(db.connSQL()) {
				String str1 = String.format("%02d", i);
				String sql = "SELECT count(*) FROM (SELECT distinct " + MOFIADATA_MAC_COLUMN + " FROM " + DATABASE_TABLE_MOFIADATA + " WHERE loc='" + deviceID + "' and DATE_FORMAT(" + MOFIADATA_STAMPTIME_COLUMN + ", '%Y%m%d%H') ='" + date + str1 + "') as COUNT";
//				String sql = "select count(*) from " + DATABASE_TABLE_MOFIADATA + " where loc='" + deviceID + "' and DATE_FORMAT(stamptime, '%Y%m%d%H')='" + date + str1 + "'";
				log("getNumberHour sql: " + sql);
				ResultSet rs = db.selectSQL(sql);
				try {
					while(rs.next()) {
						log("Query devices from mofidata: " + sql + "\t" + rs.getInt("count(*)"));
						numberHour.put(i, rs.getInt("count(*)"));
						numOfHour[i] = rs.getInt("count(*)");
					}
				} catch (Exception e) {
					log("Exception in getNumberHour in datamofi");
				}
				db.deconnSQL();
			}
		}
		return numOfHour;
	}
	public void updateRawCustomerFlowTable() {
		log("updateRawFlowTable======");
		for(int i = 0; i < mDevices.size(); i++) {
			log("Devices " + i + " is " + mDevices.get(i));
			String devicesID = mDevices.get(i);
			ArrayList<String> dataArrayList = mdateWithDevices.get(devicesID);
			//FIXME: This is hard code for test.
			for(int j = 0; j < dataArrayList.size(); j++) {
				String insertRawSQL = "insert into " + DATABASE_TABLE_RAW_FLOW + " (devicesID,date," +
						HOUR_COLUMNS + ") values ('" + devicesID + "','" + dataArrayList.get(j) + "'";
				String insertCustomerSQL = "insert into " + DATABASE_TABLE_CUSTOMER_FLOW + " (devicesID,date," +
						HOUR_COLUMNS + ") values ('" + devicesID + "','" + dataArrayList.get(j) + "'";
				String updateRawFlowSQL = "update " + DATABASE_TABLE_RAW_FLOW + " set ";
				String updateCustomerFlowSQL = "update " + DATABASE_TABLE_CUSTOMER_FLOW + " set ";
				int[] numOfHour = getNumberHour(devicesID, dataArrayList.get(j));
				int[] numOfHourDistinct = getNumberHourDistinct(devicesID, dataArrayList.get(j));

				for(int k = 0; k < 24; k++) {
					insertRawSQL = insertRawSQL + "," + numOfHour[k];
					insertCustomerSQL = insertCustomerSQL + "," + numOfHourDistinct[k];
					updateRawFlowSQL = updateRawFlowSQL + "hour_" + String.format("%02d", k+1) + "='" + numOfHour[k] + "'";
					updateCustomerFlowSQL = updateCustomerFlowSQL + "hour_" + String.format("%02d", k+1) + "='" + numOfHourDistinct[k] + "'";
					if(k != 23) {
						updateRawFlowSQL = updateRawFlowSQL + ",";
						updateCustomerFlowSQL = updateCustomerFlowSQL + ",";
					}
					log("query count hour is " + k + " and number is " + numOfHour[k]);
					log("query count hour distinct is " + k + " and number is " + numOfHourDistinct[k]);
				}
				insertRawSQL = insertRawSQL + ")";
				insertCustomerSQL = insertCustomerSQL + ")";
				updateRawFlowSQL = updateRawFlowSQL + "where " + RAW_FLOW_DEVICESID_COLUMN + "='" + devicesID + "' and " + RAW_FLOW_DATE_COLUMN + "='" + dataArrayList.get(j) +"'";
				updateCustomerFlowSQL = updateCustomerFlowSQL + "where " + CUSTOMER_FLOW_DEVICESID_COLUMN + "='" + devicesID + "' and " + CUSTOMER_FLOW_DATE_COLUMN + "='" + dataArrayList.get(j) +"'";
				
				log(insertRawSQL);
				log(insertCustomerSQL);
				log(updateRawFlowSQL);
				log(updateCustomerFlowSQL);

				MySQL db = new MySQL();
				if(db.connSQL()) {
//					String insert = rawSQL;
					if(isExist(devicesID, dataArrayList.get(j), DATABASE_TABLE_RAW_FLOW)) {
						if (db.updateSQL(updateRawFlowSQL) == true) {
							log("update raw_flow successfully");
						}
					} else {
						if (db.insertSQL(insertRawSQL) == true) {
							log("insert raw_flow successfully");
						}
					}
					if(isExist(devicesID, dataArrayList.get(j), DATABASE_TABLE_CUSTOMER_FLOW)) {
						if (db.updateSQL(updateCustomerFlowSQL) == true) {
							log("update customer_flow successfully");
						}
					} else {
						if (db.insertSQL(insertCustomerSQL) == true) {
							log("insert customer_flow successfully");
						}
					}
					db.deconnSQL();
				}
			}
		}
	}
	/*
	 * Author:hongsong
	 * Time:2013-07-07 
	 * Function:calculate the avg time of every devices & every day,and update the stay_time_avg table;
	 * Input paras:null 
	 * Output paras:null
	 * 
	 */
	public void updateStayTimeAvgTable() {
		log("updateStayTimeAvgTable======");
		MySQL db = new MySQL();
		
		//For cal the avg time ,form the tmp table 
		String tmpSelectSQL = null ;
		String tmpFormat = "select avg(timestampdiff(second,min,max)) as avgtime from (select max(" + MOFIADATA_STAMPTIME_COLUMN + ") as max , min(" + MOFIADATA_STAMPTIME_COLUMN + ") as min from " + DATABASE_TABLE_MOFIADATA
				+ " where " + MOFIADATA_DEVICESID_COLUMN + " ='%s' and date(" + MOFIADATA_STAMPTIME_COLUMN + ")='%s' group by " + MOFIADATA_MAC_COLUMN + " ) as t" ;
		ResultSet rs = null ;
		
		float avgtime = 0 ;
		
		if(db.connSQL()){
			for(int i = 0; i < mDevices.size(); i++) {
				//get the total devices
				log("Devices " + i + " is " + mDevices.get(i));
				String devicesID = mDevices.get(i);
				
				//get the available date depend on the individual device
				ArrayList<String> dataArrayList = mdateWithDevices.get(devicesID);
				
				//cal the avg time of every date
				for(int j = 0; j < dataArrayList.size(); j++) {
					/* tmpSelectSQL should be like this: 
					 * select mac,avg(timestampdiff(second,min,max)) as avgtime 
					 * from (select mac ,max(stamptime) as max,min(stamptime) as min from mofidata 
					 * where loc = '0001000011' and date(stamptime)='2013-07-06' group by mac limit 20)
					*/
					dLog("devicesID= " + devicesID + "dataArrayList[" + j + "]= " + dataArrayList.get(j));
					tmpSelectSQL = String.format( tmpFormat, devicesID , dataArrayList.get(j) );
					
					dLog(tmpSelectSQL);
					rs = db.selectSQL(tmpSelectSQL);
					try {
						while(rs.next()) {
							log("cal the avgtime from mofidata: " + devicesID +'\t'+ dataArrayList.get(j) +'\t' + rs.getFloat("avgtime"));
							avgtime = rs.getFloat(1);
						}
					} catch (Exception e) {
						log("Exception in getavgtime from datamofi");
					}
					
					String insertAvgSQL = "insert into " + DATABASE_TABLE_STAY_TIME_AVG + " ("+ STAY_TIME_AVG_DEVICESID_COLUMN + "," + STAY_TIME_AVG_DATE_COLUMN + "," +
							STAY_TIME_AVG_TIME_COLUMN + ") values ('" + devicesID + "','" + dataArrayList.get(j) + "','" + avgtime + "')" ;
					String updateAvgSQL = "update " + DATABASE_TABLE_STAY_TIME_AVG + " set " + STAY_TIME_AVG_TIME_COLUMN + "=" + avgtime ;
					updateAvgSQL = updateAvgSQL + " where " + STAY_TIME_AVG_DEVICESID_COLUMN + "='" + devicesID + "' and " + STAY_TIME_AVG_DATE_COLUMN + "='" + dataArrayList.get(j) +"'";
									
					log(insertAvgSQL);
					log(updateAvgSQL);

					if(isExist(devicesID, dataArrayList.get(j), DATABASE_TABLE_STAY_TIME_AVG)) {
						if (db.updateSQL(updateAvgSQL) == true) {
							log("update stay_time_avg successfully");
						}
					} else {
						if (db.insertSQL(insertAvgSQL) == true) {
							log("insert stay_time_avg successfully");
						}
					}
					
				}//end of inner loop
			}//end of outter loop
			
			db.deconnSQL();
			
		}else{
			log("can't connect the mysql database!");
		}
	}
	
	public void adjustDataNeedUpdate() {
		
	}

	public void dump() {
		dLog("========dump mDevices start=======");
		for(int i = 0; i < mDevices.size(); i++) {
			dLog(mDevices.get(i).toString());
		}
		dLog("========dump mDevices end=======");
		
		dLog("========dump mDate start=======");
		for(int i = 0; i < mDate.size(); i++) {
			dLog(mDate.get(i).toString());
		}
		dLog("========dump mDate end=======");
		dLog("========dump mdateWithDevices start=======");
		Iterator iter = mdateWithDevices.entrySet().iterator();
		while (iter.hasNext()) {
			Map.Entry entry = (Map.Entry) iter.next();
			String devicesID = entry.getKey().toString();
			dLog("devicesID-> " + devicesID);
			ArrayList<String> dateArray = (ArrayList<String>)entry.getValue();
			if(dateArray == null) {
				dLog("\tdateArray is null");
			} else {
				for(int i = 0; i < dateArray.size(); i++) {
					dLog("\tdateArray[" + i + "]= " + dateArray.get(i));
				}
			}
		}
		dLog("========dump mdateWithDevices end=======");
	}
	
	public void init() {
		queryDevices();
		queryDate();
		queryDateWithDevices();
	}

	private void log(String data) {
		if(DBG) {
			Util.Log(TAG, data);
		}
	}
	private void dLog(String data) {
		if(VDBG) {
			Util.Log(TAG, data);
		}
	}
	public static void main(String args[]) {
//		DataProvider test = new DataProvider();
//		test.queryDate();
//		test.queryDevices();
//		test.queryDateWithDevices();
//		test.dump();
//		test.updateRawCustomerFlowTable();
	}
}
