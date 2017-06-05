/**
 * 
 */
package com.itpro.airtimedata.importcdr.terminated.main;

import com.itpro.cfgreader.CfgReader;
import com.itpro.log4j.ITProLog4jCategory;

//import com.itpro.mca.OCSLogAnalizer.db.OraDbProcess;
import com.itpro.util.MainForm;

/**
 * @author Giap Van Duc
 *
 */
public class AirTimeDataImportTeminatedEvent extends MainForm {
	private String configFile = "airTimeDataImportTerminatedEvent.cfg";
	public static ITProLog4jCategory logger;
	/* (non-Javadoc)
	 * @see com.itpro.util.MainForm#OnLoadConfig()
	 */
	@Override
	protected void OnLoadConfig() {
		// TODO Auto-generated method stub
		CfgReader cfgReader = new CfgReader();
		String file = AirTimeDataImportTeminatedEvent.getConfigPath()+configFile;
		cfgReader.load(file);		

		cfgReader.setGroup("FTP");
		Config.ftpHost = cfgReader.getString("Host", "10.78.253.21");
		Config.ftpUserName = cfgReader.getString("UserName","data_airtime");
		Config.ftpPassword = cfgReader.getString("Password","airTime_ata1243");
		Config.ftpRemotePath = cfgReader.getString("RemotePath","/u01/CDR/OCS/backup/changestatus_2/");
		Config.ftpRemoteBackupPath = cfgReader.getString("RemoteBackupPath","/u01/CDR/OCS/backup/changestatus_2/backup/");
		Config.ftpLocalPath = cfgReader.getString("LocalPath","/opt/itpro/cdr/terminated/");
		Config.checkFileDelayTime = cfgReader.getInt("CheckFileDelayTime", 10);
		String enableRemoteBackup = cfgReader.getString("EnableRemoteBackup","yes");
		Config.enableRemoteBackup = enableRemoteBackup.equalsIgnoreCase("yes")||enableRemoteBackup.equalsIgnoreCase("true")?true:false;
		
		cfgReader.setGroup("DB");
		Config.dbServerName = cfgReader.getString("ServerIpAddr", "10.120.41.103");
		Config.dbDatabaseName = cfgReader.getString("DbName", "airtime_data");
		Config.dbUserName = cfgReader.getString("UserName", "dataadvance");;
		Config.dbPassword = cfgReader.getString("Password", "khongbiet@dataadvance");
		
		if(cfgReader.isChanged())
			cfgReader.save(file);
	}

	/* (non-Javadoc)
	 * @see com.itpro.util.MainForm#OnStartSystem()
	 */
	@Override
	protected void OnStartSystem() {
		// TODO Auto-generated method stub
		logger = logManager.GetInstance("ImportTerminatedEvent", getLogPath(), "ImportTerminatedEvent", 1, 1, 1, 1, 1, 1, 1, true);
		
		//GlobalVars.oraDbProcess.setLogger(logger);			
		//GlobalVars.oraDbProcess.start();		
		
		GlobalVars.mysqlDbProcess.setLogger(logger);
		GlobalVars.mysqlDbProcess.start();
		
		GlobalVars.cdrLogProcess.setLogger(logger);
		GlobalVars.cdrLogProcess.setLogPrefix("[CDRLog] ");
		GlobalVars.cdrLogProcess.start();
	}

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		// TODO Auto-generated method stub
		AirTimeDataImportTeminatedEvent.setHomePath("/opt/itpro/airTimeData/airTimeDataImportTerminatedEvent/");
		AirTimeDataImportTeminatedEvent.setLogConfig("loggerAirTimeDataImportTerminatedEvent.conf");
		AirTimeDataImportTeminatedEvent main = new AirTimeDataImportTeminatedEvent();				
		main.start();
	}

}
