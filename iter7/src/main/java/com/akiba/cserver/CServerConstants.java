package com.akiba.cserver;

public interface CServerConstants extends MySQLErrorConstants {

	public final static short OK = 1;
	public final static short END = 2;
	public final static short ERR = 100;
	public final static short MISSING_OR_CORRUPT_ROW_DEF = 99;
	
	// From include/my_base.h:
	
	public final static int MAX_VERSIONS_PER_TABLE = 65536;
	public final static int MAX_GROUP_DEPTH = 256;
	
	
}
