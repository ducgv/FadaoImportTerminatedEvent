/**
 * 
 */
package com.itpro.airtimedata.importcdr.terminated.process;

import java.io.BufferedReader;
import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.SocketException;
import java.sql.Timestamp;
import java.util.Calendar;
import java.util.Date;
import java.util.Hashtable;
import java.util.Vector;

import org.apache.commons.net.ftp.FTP;
import org.apache.commons.net.ftp.FTPClient;
import org.apache.commons.net.ftp.FTPFile;

import com.itpro.airtimedata.importcdr.terminated.main.Config;
import com.itpro.airtimedata.importcdr.terminated.main.GlobalVars;
import com.itpro.airtimedata.importcdr.terminated.struct.TerminatedEvent;
import com.itpro.util.ProcessingThread;

/**
 * @author Giap Van Duc
 *
 */
public class CDRLogProcess extends ProcessingThread {
	public final int DAY_MILLIS = 86400000; //24*60*60*1000;
	public boolean isConnected = false;
	public FTPClient ftpClient = null;
	Vector<String> fileList = new Vector<String>();
	private long currentFolderDate;
	/* (non-Javadoc)
	 * @see com.itpro.util.ProcessingThread#OnHeartBeat()
	 */
	@Override
	protected void OnHeartBeat() {
		// TODO Auto-generated method stub
		if(!isConnected){
			if(ftpClient!=null){				
				try {
					ftpClient.disconnect();
				} catch (IOException e) {
					// TODO Auto-generated catch block
//					e.printStackTrace();
				}
				ftpClient = null;
			}
			Connect();
		}
	}
	
	private void getFileList(){
		if(!isConnected)
			return;
		long curDate = System.currentTimeMillis();
		if(month(currentFolderDate)==month(curDate)){
			currentFolderDate = curDate;
		}
		try {
			ftpClient.changeWorkingDirectory(Config.ftpRemotePath+getDateString(currentFolderDate)+"/");
		} catch (IOException e) {
			// TODO Auto-generated catch block
			logError("getFileList: changeWorkingDirectory error:"+e.getMessage());
			isConnected = false;
			return;
		}
		FTPFile[] ftpFiles;
		try {
			ftpFiles = ftpClient.listFiles();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			logError("getFileList: listFiles error:"+e.getMessage());
			isConnected = false;
			return;
		}
		
		Hashtable<String, Long> oldFtpFiles = new Hashtable<String, Long>();
		for(FTPFile ftpFile:ftpFiles){
			oldFtpFiles.put(ftpFile.getName(), ftpFile.getSize());
			//logInfo("file:"+ftpFile.getName()+"; fileSize:"+ftpFile.getSize());
		}
		try {
			Thread.sleep(Config.checkFileDelayTime*1000);
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
//			e.printStackTrace();
		}
		
		FTPFile[] newFtpFiles = null;
		try {
			newFtpFiles = ftpClient.listFiles();
		} catch (IOException e) {
			// TODO Auto-generated catch block
//			e.printStackTrace();
			logError("getFileList: listFiles error:"+e.getMessage());
			isConnected = false;
			return;
		}		
		
		for(FTPFile newFtpFile:newFtpFiles){
			long newSize = newFtpFile.getSize();
			Long oldSize = oldFtpFiles.get(newFtpFile.getName());
			if(oldSize!=null && oldSize==newSize && GlobalVars.fileList.getParam(newFtpFile.getName())==null){
				if(newSize>0){
					fileList.add(newFtpFile.getName());
					logInfo("Append to fileList: file:"+newFtpFile.getName()+"; fileSize:"+newFtpFile.getSize());
				}
				else{
					deleteRemoteFile(newFtpFile.getName());
				}
			}
		}
		
		if(fileList.isEmpty()){
			if(isNextFolderAvailable()){
				currentFolderDate = currentFolderDate+DAY_MILLIS;
			}
		}
	}
	
	public boolean isNextFolderAvailable(){
		long nextDate = currentFolderDate+DAY_MILLIS;
		if(month(nextDate) == month(currentFolderDate)){
			return false;
		}
		try {
			ftpClient.changeWorkingDirectory(Config.ftpRemotePath+getDateString(nextDate)+"/");
		} catch (IOException e) {
			// TODO Auto-generated catch block
			logError("Check isNextFolderAvailable, error:"+e.getMessage());
			return false;
		}
		FTPFile[] ftpFiles;
		try {
			ftpFiles = ftpClient.listFiles();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			logError("isNextFolderAvailable, listFiles error:"+e.getMessage());
			return false;
		}
		if(ftpFiles.length>0)
			return true;
		else
			return false;
	}
	
	private void Connect() {
		// TODO Auto-generated method stub
		try {
			ftpClient = new FTPClient();
			ftpClient.connect(Config.ftpHost);
			ftpClient.login(Config.ftpUserName, Config.ftpPassword);
			ftpClient.setFileType(FTP.BINARY_FILE_TYPE);
			isConnected = true;
			logInfo("Connected to FTP Server "+Config.ftpHost);
		} catch (SocketException e) {
			// TODO Auto-generated catch block
			logError("FTP to +"+Config.ftpHost+" error: "+e.getMessage());
			isConnected = false;
		} catch (IOException e) {
			// TODO Auto-generated catch block
			logError("FTP to +"+Config.ftpHost+" error: "+e.getMessage());
			isConnected = false;
		}	
	}

	/* (non-Javadoc)
	 * @see com.itpro.util.ProcessingThread#initialize()
	 */
	@Override
	protected void initialize() {
		// TODO Auto-generated method stub
		setHeartBeatInterval(5000);
		Connect();
		currentFolderDate = new Date(System.currentTimeMillis()).getTime()-24*60*60*1000; //default set currentTime to yesterday;	
	}

	/* (non-Javadoc)
	 * @see com.itpro.util.ProcessingThread#process()
	 */
	@Override
	protected void process() {
		// TODO Auto-generated method stub
		if(!isConnected){
			return;
		}
		
		if(!GlobalVars.fileList.isLoaded)
			return;
		
		if(fileList.isEmpty()){
			getFileList();
		}
		if(!fileList.isEmpty()&&GlobalVars.mysqlDbProcess.queueInsertTerminateEventReq.size()<100){
			String fileName = fileList.remove(0);
			if(retrieveFile(fileName)){
				processFile(fileName);
				//deleteRemoteFile(fileName);
				//if(Config.enableRemoteBackup)
				//	backupFile(fileName);
				deleteLocalFile(fileName);
			}
		}		
	}

	public void deleteLocalFile(String fileName) {
		// TODO Auto-generated method stub
		File file = new File(Config.ftpLocalPath+fileName);
		if(file.delete()){
			logInfo("Delete local file: "+fileName+" success");
		} else {
			logError("Delete local file: "+fileName+" error");
		}
	}

	public void backupFile(String fileName) {
		// TODO Auto-generated method stub
		boolean ok = false;		
		String path=Config.ftpRemoteBackupPath+getDateString(System.currentTimeMillis())+"/";
		do{
			FileInputStream fis = null;
			try {
				ftpClient.makeDirectory(path);
				fis = new FileInputStream(Config.ftpLocalPath+fileName);
				ftpClient.storeFile(path+fileName, fis);
				ok = true;
			} catch (IOException e) {
				// TODO Auto-generated catch block				
				logError("backupFile: uploadFile error:"+e.getMessage());
				isConnected = false;
				Connect();
			}
		} while(!ok);
	}

	private void deleteRemoteFile(String fileName) {
		// TODO Auto-generated method stub
		boolean ok = false;
		do{
			try {				
				ftpClient.deleteFile(Config.ftpRemotePath+getDateString(currentFolderDate)+"/"+fileName);
				ok = true;
			} catch (IOException e) {
				// TODO Auto-generated catch block
				//			e.printStackTrace();
				logError("deleteFile: file: " + fileName + "; error:"+e.getMessage());
				isConnected = false;
				Connect();
			}
		} while(!ok);
	}

	private void processFile(String fileName) {
		// TODO Auto-generated method stub
		try{
			FileInputStream fstream = new FileInputStream(Config.ftpLocalPath+fileName);
			logInfo("processFile: "+Config.ftpLocalPath+fileName);
			DataInputStream in = new DataInputStream(fstream);
			BufferedReader br = new BufferedReader(new InputStreamReader(in));
			String line;
			GlobalVars.fileList.setParam(fileName, "1");
			GlobalVars.mysqlDbProcess.queueInsertTerminatedFileReq.enqueue(fileName);
			while ((line = br.readLine()) != null)   {
				if(!line.equals("")){
					line = line.trim();
					TerminatedEvent terminatedEvent = new TerminatedEvent();
					String[] strArr = line.split(",");
					if(strArr.length==6){
						terminatedEvent.msisdn = strArr[0].startsWith("856")?strArr[0].replaceFirst("856", ""):strArr[0];
						terminatedEvent.date_time = Timestamp.valueOf(strArr[5]);
						logInfo(terminatedEvent.toString());					
						GlobalVars.mysqlDbProcess.queueInsertTerminateEventReq.enqueue(terminatedEvent);
					}
					else{
						logWarning("Invalid format:"+line);	
					}
				}
			}
			in.close();
			br.close();
		} catch (Exception e){
			logError("processFile error: " + e.getMessage());
		}
	}

	private boolean retrieveFile(String fileName) {
		// TODO Auto-generated method stub
		FileOutputStream fos = null;
		try {
			fos = new FileOutputStream(new File(Config.ftpLocalPath+fileName));
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
//			e.printStackTrace();
			logError("retrieveFile: create FileOutputStream for " + fileName + " error:"+e.getMessage());
			return false;
		}
		
		try {
			ftpClient.changeWorkingDirectory(Config.ftpRemotePath+getDateString(currentFolderDate)+"/");					
			ftpClient.retrieveFile(fileName, fos);
			fos.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
//			e.printStackTrace();
			logError("retrieveFile: file " + fileName + " error:"+e.getMessage());
			isConnected = false;
			return false;
		}		
		return true;
	}
	
	public static String getDateString(long timestamp){
		Date date = new Date(timestamp);
		Calendar currentTimestamp = Calendar.getInstance();
		currentTimestamp.setTime(date);
		String result = String.format("%04d%02d",
				currentTimestamp.get(Calendar.YEAR), currentTimestamp.get(Calendar.MONTH)+1);
		return result;
	}
	
	public static int day(long timestamp){
		Date date = new Date(timestamp);
		Calendar currentTimestamp = Calendar.getInstance();
		currentTimestamp.setTime(date);
		return currentTimestamp.get(Calendar.DAY_OF_MONTH);
	}
	
	public static int month(long timestamp){
		Date date = new Date(timestamp);
		Calendar currentTimestamp = Calendar.getInstance();
		currentTimestamp.setTime(date);
		return currentTimestamp.get(Calendar.MONTH)+1;
	}
	public static int year(long timestamp){
		Date date = new Date(timestamp);
		Calendar currentTimestamp = Calendar.getInstance();
		currentTimestamp.setTime(date);
		return currentTimestamp.get(Calendar.YEAR);
	}
}
