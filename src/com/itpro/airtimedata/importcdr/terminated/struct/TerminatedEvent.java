/**
 * 
 */
package com.itpro.airtimedata.importcdr.terminated.struct;

import java.sql.Timestamp;

/**
 * @author Giap Van Duc
 *
 */
public class TerminatedEvent {
	public String msisdn;
	public Timestamp date_time;
	public String toString(){
		String result = "TerminatedEvent:";
		result += " msisdn:"+msisdn;
		result += "; date_time:"+date_time.toString();
		return result;
	}
}
