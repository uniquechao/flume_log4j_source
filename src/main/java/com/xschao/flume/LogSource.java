/**
 * com.xschao.flume.LogSource
 * xushichao
 * 2014年3月10日
 */
package com.xschao.flume;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.EventDeliveryException;
import org.apache.flume.PollableSource;
import org.apache.flume.conf.Configurable;
import org.apache.flume.event.SimpleEvent;
import org.apache.flume.source.AbstractSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.xschao.io.XscBufferedReader;

/**
 * 顺序读取log文件，具有故障恢复功能 <br>
 * 接收文件通配符（只时间格式 yyyyMMDD hhmmss）
 * <p>
 * 顺序读取文件，并保存offset到磁盘 ，重启时重读offset
 * </p>
 * 
 * @author xushichao
 * @date 2014年3月10日
 */
public class LogSource extends AbstractSource implements Configurable,
		PollableSource {

	private static final Logger log = LoggerFactory.getLogger(LogSource.class);
	/**
	 * 日志时间戳后缀格式
	 */
	private String fileNameSuffix;
	/**
	 * 日志名字
	 */
	private String fileNameBase;
	/**
	 * 日志文件夹
	 */
	private String logFolder;

	/**
	 * 主机名
	 */
	private String hostName;
	/**
	 * topic
	 */
	private String topic;

	/**
	 * 每次发送日志size
	 */
	private Integer batchSize;
	/**
	 * 轮询时间 （毫秒）
	 */
	private Long rolltime;

	private Long lastFileModifyTimes;
	private Long lastOffset;
	private SimpleDateFormat sdf;
	private XscBufferedReader inReader;
	private String recordFile;

	private FileOutputStream fw = null;

	private int readFileContent(final XscBufferedReader inReader,
			final Long lineNum, final long fileTimes) {
		ArrayList<Event> eventList = new ArrayList<Event>();
		String line = null;
		Event event;
		Map<String, String> headers;
		try {
			if (lineNum != null && lineNum > 0l
					&& lineNum > inReader.currentOffset()) {
				inReader.skip(lineNum - inReader.currentOffset());
			}
			while ((line = inReader.readLine()) != null) {
				event = new SimpleEvent();
				headers = new HashMap<String, String>();
				headers.put("timestamp",
						String.valueOf(System.currentTimeMillis()));
				headers.put("hostname", hostName);
				headers.put("filename", fileNameBase);
				event.setBody(line.getBytes());
				event.setHeaders(headers);
				eventList.add(event);
				if (eventList.size() == batchSize) {
					sendEvents(inReader, fileTimes, eventList);
					try {
						Thread.sleep(rolltime);
					} catch (InterruptedException e) {
					}
				}
			}
			if (eventList.size() > 0) {
				sendEvents(inReader, fileTimes, eventList);
			}
			return 1;
		} catch (IOException e) {
			log.error(" file read error ", e);
			return -1;
		}
	}

	private void sendEvents(final XscBufferedReader inReader,
			final long fileTimes, final ArrayList<Event> eventList)
			throws IOException {
		getChannelProcessor().processEventBatch(eventList);
		log.debug("send events size:" + eventList.size() + ";fileName:"
				+ inReader.getFileName() + ";lastmodifytime:" + fileTimes
				+ ";offset:" + inReader.currentOffset());
		wirteOffset(fileTimes, inReader.currentOffset());
		eventList.clear();
	}

	/**
	 * 根据时间推测log文件的名称
	 * 
	 * @param baseName log文件名
	 * @param subfix 日期后缀名
	 * @param time 时间
	 * @return
	 * @auther xushichao
	 * @date 2014年3月17日
	 */
	private String logFileName(final String baseName, final String subfix,
			final Long time) {
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

	File file = null;

	@Override
	public Status process() throws EventDeliveryException {
		String logFileName = logFileName(fileNameBase, fileNameSuffix,
				lastFileModifyTimes);
		try {
			if (!logFileName.equals(fileNameBase)) {
				file = new File(logFolder + logFileName);
				List<File> historyFiles = new ArrayList<File>();
				File[] listBrothers = file.getParentFile().listFiles();
				for (File brother : listBrothers) {
					if (brother.lastModified() < lastFileModifyTimes
							|| brother.getName().equals(fileNameBase)
							|| !brother.getName().startsWith(fileNameBase)) {
						continue;
					}

					historyFiles.add(brother);
				}
				// 就要补充上一个日志 到现在日志之间的所有日志
				if (historyFiles.size() > 0) {
					long lastFileOffset = lastOffset;
					if (inReader != null) {
						lastFileOffset = inReader.currentOffset();
						inReader.close();
						inReader = null;
					}
					log.debug("debug information： being to read  historyFiles logfiles size:"
							+ historyFiles.size());
					for (File brother : historyFiles) {
						long offset = 0l;
						if (brother.getName().equals(logFileName)) {
							offset = lastFileOffset;
						}
						log.debug("debug information： being to read logfiles:"
								+ brother.getName() + ";offset:" + offset);
						try {
							inReader = new XscBufferedReader(new FileReader(
									brother), brother.getName());
							readFileContent(inReader, offset,
									brother.lastModified());
						} catch (IOException e) {
							log.error("file read error ", e);
						} finally {
							try {
								inReader.close();
								inReader = null;
							} catch (IOException e) {
								log.error("file close error ", e);
							}
						}
						log.debug("debug information： had readed logfiles:"
								+ brother.getName());
					}
					lastOffset = 0l;
				}
			}
			if (inReader == null) {
				file = new File(logFolder + fileNameBase);
				if (!file.exists()
						&& (lastFileModifyTimes != null && file.lastModified() <= lastFileModifyTimes)) {
					return Status.READY;
				}
				inReader = new XscBufferedReader(new FileReader(file),
						file.getName());
				log.debug("debug information： open logfile:"
						+ file.getAbsolutePath());
			}
			if (lastFileModifyTimes == null
					|| file.lastModified() > lastFileModifyTimes) {
				readFileContent(inReader, lastOffset == null ? 0l : lastOffset,
						file.lastModified());
			}
			return Status.READY;
		} catch (FileNotFoundException e) {
			log.error(" file read error ", e);
			return Status.READY;
		} catch (IOException e) {
			log.error(" file read error ", e);
			return Status.BACKOFF;
		}

	}

	@Override
	public void configure(final Context context) {
		topic = context.getString("topic");
		rolltime = context.getLong("rolltime", 300l);
		batchSize = context.getInteger("batchSize", 10);
		fileNameBase = context.getString("fileNameBase");
		fileNameSuffix = context.getString("fileNameSuffix");
		hostName = context.getString("hostName");
		sdf = new SimpleDateFormat(fileNameSuffix);
		recordFile = context.getString("recordFile");
		logFolder = context.getString("logFolder");
		// TODO 参数校验

	}

	/**
	 * (non-Javadoc)
	 * 
	 * @see org.apache.flume.source.AbstractSource#start()
	 */
	@Override
	public synchronized void start() {
		super.start();
		// 打开记录文件
		File file = new File(recordFile);
		BufferedReader recordReader = null;
		try {
			if (!file.exists()) {
				file.createNewFile();
				log.info("DuokuLogSource creat record file "
						+ file.getAbsolutePath());
			} else {
				recordReader = new BufferedReader(new FileReader(file));
				String line = recordReader.readLine();
				if (line != null && line.length() > 2) {
					String[] split = line.split("\t");
					if (split.length > 1) {
						lastFileModifyTimes = Long.valueOf(split[0]);
						lastOffset = Long.valueOf(split[1]);
					}
				}
				log.info("DuokuLogSource read record file "
						+ file.getAbsolutePath() + "line:" + line
						+ ";lastFileModifyTimes:" + lastFileModifyTimes
						+ ";lastOffset:" + lastOffset);
			}
			fw = new FileOutputStream(recordFile);
			fw.getChannel().tryLock();
			log.info("DuokuLogSource locked record file "
					+ file.getAbsolutePath());
		} catch (IOException e) {
			log.error("创建记录文件失败", e);
		} finally {
			if (recordReader != null) {
				try {
					recordReader.close();
				} catch (IOException e) {
				}
			}
		}

		log.info("DuokuLogSource[" + this.getName() + "] start, "
				+ this.toString());
	}

	private void wirteOffset(final Long fileTimes, final Long offset)
			throws IOException {
		if (fileTimes == null || offset == null) {
			return;
		}
		lastFileModifyTimes = fileTimes;
		lastOffset = offset;
		fw.getChannel().position(0);
		fw.write(new StringBuffer().append(lastFileModifyTimes).append("\t")
				.append(lastOffset).toString().getBytes());
		fw.flush();
	}

	@Override
	public synchronized void stop() {
		try {
			wirteOffset(lastFileModifyTimes, lastOffset);
			fw.close();
		} catch (IOException e) {
		}
		super.stop();
	}

	@Override
	public String toString() {
		return "com.xschao.flume.DuokuLogSource  [fileNameSuffix="
				+ fileNameSuffix + ", fileNameBase=" + fileNameBase
				+ ", logFolder=" + logFolder + ", hostName=" + hostName
				+ ", topic=" + topic + ", batchSize=" + batchSize
				+ ", rolltime=" + rolltime + ", lastFileModifyTimes="
				+ lastFileModifyTimes + ", lastOffset=" + lastOffset + ", sdf="
				+ sdf + ", inReader=" + inReader + ", recordFile=" + recordFile
				+ ", fw=" + fw + ", file=" + file + "]" + super.toString();
	}

}
