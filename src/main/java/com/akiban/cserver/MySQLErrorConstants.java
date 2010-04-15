package com.akiban.cserver;

/**
 * Constants derived from MySQL include/my_base.h
 * Add new MySQL error codes here, as needed.
 * 
 * @author peter
 *
 */
public interface MySQLErrorConstants {
	
	public final static short HA_ERR_KEY_NOT_FOUND = 120;
	public final static short HA_ERR_FOUND_DUPP_KEY = 121;
	public final static short HA_ERR_INTERNAL_ERROR = 122;
	public final static short HA_ERR_RECORD_CHANGED = 123;
	public final static short HA_ERR_RECORD_DELETED = 134;
	public final static short HA_ERR_NO_REFERENCED_ROW = 151;
	public final static short HA_ERR_ROW_IS_REFERENCED = 152; 
	public final static short HA_ERR_NO_SUCH_TABLE = 155;
}
