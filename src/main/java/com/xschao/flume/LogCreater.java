/**
 * com.xschao.flume.LogCreater
 * xushichao
 * 2014年3月19日
 */
package com.xschao.flume;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Random;

import org.apache.log4j.Logger;

/**
 * 不断的产生log
 * 
 * @author xushichao
 * @date 2014年3月19日
 */
public class LogCreater {
	/**
	 * Logger for this class
	 */
	private static final Logger logger = Logger.getLogger(LogCreater.class);

	private static SimpleDateFormat sdf = new SimpleDateFormat(
			"'.'yyyy-MM-dd-HH-mm");

	public static void main(final String[] args) throws InterruptedException {
		// File file = new File("D:\\tmp\\log.txt");
		// File[] listFiles = file.getParentFile().listFiles();
		Random r = new Random(10);
		Date date = new Date();
		System.out.println(sdf.format(new Date(1395219539000l)));
		System.out.println(logFileName("hadoop_flume.log", 1395221699000l));

		System.exit(1);
		while (true) {
			long sleepTime = Math.abs(r.nextInt() % 10000);
			logger.info("sleep:" + sleepTime + ";date:" + date.toString());
			Thread.sleep(sleepTime);
		}
	}

	public static String logFileName(final String baseName, final Long time) {
		if (time == null || time < 10l) {
			return baseName;
		}
		String scheduledFilename = baseName + sdf.format(new Date(time));
		String nowFilename = baseName + sdf.format(new Date());
		if (nowFilename.equals(scheduledFilename)) {
			return baseName;
		}
		return scheduledFilename;
	}
}
