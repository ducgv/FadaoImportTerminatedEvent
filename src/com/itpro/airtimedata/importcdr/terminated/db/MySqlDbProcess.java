/**
 * 
 */
package com.itpro.airtimedata.importcdr.terminated.db;

import java.sql.SQLException;

import com.itpro.airtimedata.importcdr.terminated.main.Config;
import com.itpro.airtimedata.importcdr.terminated.main.GlobalVars;
import com.itpro.airtimedata.importcdr.terminated.struct.TerminatedEvent;
import com.itpro.util.ProcessingThread;
import com.itpro.util.Queue;

/**
 * @author Giap Van Duc
 *
 */
public class MySqlDbProcess extends ProcessingThread {
	private DbConnection connection = null;	
	public boolean isConnected = false;
	public Queue queueInsertTerminateEventReq = new Queue();
	public Queue queueInsertTerminatedFileReq = new Queue();
	
	private long nextTime;	
	
	private void Connect() {	
		connection = new DbConnection(Config.dbServerName,Config.dbDatabaseName,Config.dbUserName,Config.dbPassword);		
		Exception exception = null;
		try {
			isConnected = connection.connect();
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			exception = e;			
		} catch (InstantiationException e) {
			// TODO Auto-generated catch block
			exception = e;			
		} catch (IllegalAccessException e) {
			// TODO Auto-generated catch block
			exception = e;
		} catch (ClassNotFoundException e) {
			// TODO Auto-generated catch block
			exception = e;
		}
		
		if(exception!=null){
			isConnected = false;
			logError("Connect to DB: error:"+exception.getMessage());
		}
		else{
			logError("connected to DB "+Config.dbDatabaseName);
		}
		if(!GlobalVars.fileList.isLoaded)
			try {
				connection.getParams(GlobalVars.fileList, "terminate_cdr_files");
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				//e.printStackTrace();
				logError("Load terminate_cdr_files error:"+e.getMessage()); 
			}
	}
	
	/* (non-Javadoc)
	 * @see com.logica.smpp.util.ProcessingThread#OnHeartBeat()
	 */
	@Override
	public void OnHeartBeat() {
		// TODO Auto-generated method stub
		if(connection==null){
			Connect();
		}
		else if(!isConnected){
			connection.close();
			Connect();
		}
		if(isConnected){
			try {
				connection.checkConnection();
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				//e.printStackTrace();
				logError("Check DB connection error:"+e.getMessage());
				isConnected = false;
			}
		}
	}

	/* (non-Javadoc)
	 * @see com.logica.smpp.util.ProcessingThread#initialize()
	 */
	@Override
	public void initialize() {
		// TODO Auto-generated method stub
		setHeartBeatInterval(5000);
		setLogPrefix("[InsertDB] ");
		Connect();
	}

	/* (non-Javadoc)
	 * @see com.logica.smpp.util.ProcessingThread#process()
	 */
	@Override
	public void process() {		
		// TODO Auto-generated method stub
		long curTime = System.currentTimeMillis();

		if(connection!=null && isConnected){
			if(curTime>=nextTime){
				try {
					connection.checkConnection();
					nextTime = curTime + 60000;
				} catch (SQLException e) {
					// TODO Auto-generated catch block
					//					e.printStackTrace();
					logError("CheckConnection: error:"+e.getMessage());
					isConnected = false;
				}  catch (Exception e) {
					// TODO Auto-generated catch block
					//					e.printStackTrace();
					logError("CheckConnection: error:"+e.getMessage());
					isConnected = false;
				}
			}
		}

		if(!isConnected)
			return;

		Object message = queueInsertTerminateEventReq.dequeue();
		if(message!=null){			
			try {
				TerminatedEvent rechargeEvent = (TerminatedEvent)message;
				connection.insertTerminatedEvent(rechargeEvent);
				logInfo(rechargeEvent.toString());
				nextTime = System.currentTimeMillis()+60000;
			} catch (SQLException e) {
				// TODO Auto-generated catch block						
				logError("Insert RechargeEvent error: "+e.getMessage());	
				isConnected = false;
				queueInsertTerminateEventReq.enqueue(message);
			} catch (Exception e) {
				// TODO Auto-generated catch block						
				logError("Insert RechargeEvent error: "+e.getMessage());	
				isConnected = false;
				queueInsertTerminateEventReq.enqueue(message);
			}			
		}
		
		String terminatedFile = (String)queueInsertTerminatedFileReq.dequeue();
		if(terminatedFile!=null){			
			try {
				connection.insertTerminatedFile(terminatedFile);
				nextTime = System.currentTimeMillis()+60000;
			} catch (SQLException e) {
				// TODO Auto-generated catch block						
				logError("Insert insertTerminatedFile error: "+e.getMessage());	
				isConnected = false;
				queueInsertTerminatedFileReq.enqueue(terminatedFile);
			} catch (Exception e) {
				// TODO Auto-generated catch block						
				logError("Insert insertTerminatedFile error: "+e.getMessage());	
				isConnected = false;
				queueInsertTerminatedFileReq.enqueue(terminatedFile);
			}			
		}
	}
}
