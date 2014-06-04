package com.qihoo.sdet.jio.api.util;

import java.io.File;
import java.text.SimpleDateFormat;
import java.util.Calendar;

public class Utils {

	public static String getNowTime() {
		return new SimpleDateFormat("yyyyMMddHHmmss").format(Calendar
				.getInstance().getTime());
	}

	public static void mkdir(String path) throws Exception {
		File dirFile = new File(path);
		if (!dirFile.exists()) {
			if (!dirFile.mkdir()){
				throw new Exception("create dir failure！"
					+ dirFile.getAbsolutePath());
			}
		}
	}
	
	public static void mkdirs(String path) throws Exception {
		File dirFile = new File(path);
		String parent = dirFile.getParent();
		if(parent != null && !(new File(parent).exists())){
			mkdirs(parent);
		}
		mkdir(path);
	}
	
	public static boolean isAndroid() {
        try {
            // Check if its android
            Class.forName("android.app.Application");
            return true;
        } catch (ClassNotFoundException e) {
            //Ignore
        }
        return false;
    }
	
	public static String getName(String path){
		// Window or Linux
		String temp[] = path.replaceAll("\\\\","/").split("/");
		if (temp.length > 1) {
			return temp[temp.length - 1];
		}
		return path;
	}
}
