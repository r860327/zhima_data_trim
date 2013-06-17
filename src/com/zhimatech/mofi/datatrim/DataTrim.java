package com.zhimatech.mofi.datatrim;

import java.io.*;
import java.net.*;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Date;
import java.util.Calendar;
import java.util.Iterator;
import java.util.Map;
import java.util.TimerTask;
import java.text.SimpleDateFormat;
import java.util.HashMap;
import java.util.Timer;
import com.zhimatech.mofi.datatrim.*;

public class DataTrim {
	private static final String TAG = "zhimaDataTrim";
	
	private Timer timer = new Timer();
	
	class MyTask extends TimerTask{
		@Override
		public void run() {
			// TODO Auto-generated method stub
			
		}
	}
	
	private int selectFromRawDB() {
		return 0;
	}
	private void insertToDB() {
		
	}
	
	public DataTrim() throws IOException {
		timer.schedule(new MyTask(), 1000, 5000);
	}
	
	public static void main(String[] args) throws IOException {
		DataUtil.Log(TAG, "welcome to zhimatech datatrim service");
		
//		mysql h = new mysql();
//		h.connSQL();
//		String s = "select * from ttttt";
//
//		String insert = "insert into ttttt(ID) values('test1')";
//		String update = "update ttttt set ID ='test2' where ID= 'test1'";
//		String delete = "delete from ttttt where ID= 'test1'";

//		if (h.insertSQL(insert) == true) {
//			System.out.println("insert successfully");
//			ResultSet resultSet = h.selectSQL(s);
//			h.layoutStyle2(resultSet);
//		}
//		if (h.updateSQL(update) == true) {
//			System.out.println("update successfully");
//			ResultSet resultSet = h.selectSQL(s);	
//			h.layoutStyle2(resultSet);
//		}
//		if (h.insertSQL(delete) == true) {
//			System.out.println("delete successfully");
//			ResultSet resultSet = h.selectSQL(s);
//			h.layoutStyle2(resultSet);
//		}

//		new DataTrim();
		DataProvider dp = new DataProvider();
		dp.queryDevices();
	}
}
class DataUtil {
	private static final boolean DBG = true;
	private static final SimpleDateFormat DATAFORMAT = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss");
	
	public static void Log(String tag, String data) {
		if (DBG) {
			System.out.println(DATAFORMAT.format(new Date()) + ", " + tag + ", " + data);
		}
	}
}
