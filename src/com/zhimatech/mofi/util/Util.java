package com.zhimatech.mofi.util;

import java.text.SimpleDateFormat;
import java.util.Date;

public class Util {
	private static final boolean DBG = true;
	private static final SimpleDateFormat DATAFORMAT = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss");
	
	public static void Log(String tag, String data) {
		if (DBG) {
			System.out.println(DATAFORMAT.format(new Date()) + ", " + tag + ", " + data);
		}
	}
}

