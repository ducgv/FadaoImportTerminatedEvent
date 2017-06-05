/**
 * 
 */
package com.itpro.airtimedata.importcdr.terminated.main;

import com.itpro.airtimedata.importcdr.terminated.db.MySqlDbProcess;
import com.itpro.airtimedata.importcdr.terminated.process.CDRLogProcess;
import com.itpro.util.Params;

/**
 * @author Giap Van Duc
 *
 */
public class GlobalVars {
	//public static OraDbProcess oraDbProcess = new OraDbProcess();
	public static MySqlDbProcess mysqlDbProcess=new MySqlDbProcess();
	public static CDRLogProcess cdrLogProcess = new CDRLogProcess();
	public static Params fileList = new Params();
}
