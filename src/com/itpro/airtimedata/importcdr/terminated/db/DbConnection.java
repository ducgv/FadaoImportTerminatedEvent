/**
 * 
 */
package com.itpro.airtimedata.importcdr.terminated.db;

import java.sql.PreparedStatement;
import java.sql.SQLException;

import com.itpro.airtimedata.importcdr.terminated.struct.TerminatedEvent;
import com.itpro.util.MySQLConnection;


/**
 * @author Giap Van Duc
 *
 */
public class DbConnection extends MySQLConnection {
	
	public DbConnection(String serverIpAddr, String databaseName,
			String userName, String password) {
		super(serverIpAddr, databaseName, userName, password);
		// TODO Auto-generated constructor stub
	}	
	
	public void insertTerminatedEvent(TerminatedEvent terminatedEvent) throws SQLException {
		// TODO Auto-generated method stub
		PreparedStatement ps = null;		
		ps=connection.prepareStatement("INSERT INTO terminated_event(msisdn,date_time) VALUES (?, ?)");
		ps.setString(1, terminatedEvent.msisdn);
		ps.setTimestamp(2, terminatedEvent.date_time);
		ps.execute();
		ps.close();
		
	}
	
	public void insertTerminatedFile(String terminatedFile) throws SQLException {
		// TODO Auto-generated method stub
		PreparedStatement ps = null;		
		ps=connection.prepareStatement("INSERT INTO terminate_cdr_files(param) VALUES (?)");
		ps.setString(1, terminatedFile);
		ps.execute();
		ps.close();
		
	}
}
