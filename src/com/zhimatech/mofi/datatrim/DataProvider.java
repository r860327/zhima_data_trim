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
	
	private static final String TAG = "DataProvider";
	
	private ArrayList mdevices = new ArrayList();
	private HashMap<String, Integer> mDevicesHash = new HashMap<String, Integer>(); 
	
	public void queryDevices() {
		MySQL db = new MySQL();
		if(db.connSQL()) {
			String sql = "select distinct loc from "+ DATABASE_TABLE_MOFIADATA;;
			ResultSet rs = db.selectSQL(sql);
			try {
				while(rs.next()) {
					DataUtil.Log(TAG, "Query devices from mofidata: " + rs.getString("loc"));
					mdevices.add(rs.getString("loc"));
				}
			} catch (Exception e) {
				DataUtil.Log(TAG, "Exception in query devices in datamofi");
			}
			db.deconnSQL();
		}
		for(int i = 0; i < mdevices.size(); i++) {
			DataUtil.Log(TAG, "Devices " + i + " is " + mdevices.get(i));
			String sql = "insert into " + DATABASE_TABLE_RAW_FLOW + " (devicesID,date," +
					"hour_01,hour_02,hour_03,hour_04,hour_05,hour_06,hour_07,hour_08,hour_09,hour_10,hour_11,hour_12" +
					",hour_13,hour_14,hour_15,hour_16,hour_17,hour_18,hour_19,hour_20,hour_21,hour_22,hour_23,hour_24) " +
					"values ('" + mdevices.get(i).toString() + "','20130616'";
			int[] numOfHour = getNumberHour(mdevices.get(i).toString(), new Date(2013-1900, 5, 16));

			for(int j = 0; j < 24; j++) {
				int num = numOfHour[j];
				sql = sql + "," + num;
				DataUtil.Log(TAG, "query count hour is " + j + " and number is " + num);
			}
			sql = sql + ")";
			DataUtil.Log(TAG, sql);

			MySQL db2 = new MySQL();
			if(db2.connSQL()) {
				String insert = sql;
				if (db2.insertSQL(insert) == true) {
					DataUtil.Log(TAG, "insert raw_flow successfully");
				}
				db.deconnSQL();
			}
		}
		
	}
	
	public int[] getNumberHour(String deviceID, Date date) {
		HashMap<Integer, Integer> numberHour = new HashMap<Integer, Integer>();
		int[] numOfHour = new int[24];
		MySQL db = new MySQL();
		for(int i = 0; i < 24; i++) {
			if(db.connSQL()) {
				SimpleDateFormat DATAFORMAT = new SimpleDateFormat("yyyyMMdd");
				String str1 = String.format("%02d", i);
				String sql = "select count(*) from " + DATABASE_TABLE_MOFIADATA + " where loc='" + deviceID + "' and DATE_FORMAT(stamptime, '%Y%m%d%H')='" + DATAFORMAT.format(date) + str1 + "'";
				DataUtil.Log(TAG, "getNumberHour sql: " + sql);
				ResultSet rs = db.selectSQL(sql);
				try {
					while(rs.next()) {
						DataUtil.Log(TAG, "Query devices from mofidata: " + str1 + "\t" + rs.getInt("count(*)"));
						numberHour.put(i, rs.getInt("count(*)"));
						numOfHour[i] = rs.getInt("count(*)");
					}
				} catch (Exception e) {
					DataUtil.Log(TAG, "Exception in getNumberHour in datamofi");
				}
				db.deconnSQL();
			}
		}
		return numOfHour;
	}
}
