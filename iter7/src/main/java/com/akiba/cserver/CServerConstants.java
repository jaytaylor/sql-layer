package com.akiba.cserver;

public interface CServerConstants {

	public final static short OK = 1;
	public final static short END = 2;
	public final static short ERR = 100;
	public final static short MISSING_OR_CORRUPT_ROW_DEF = 99;
	public final static short NON_UNIQUE = 121;	// copied from mysql include/my_base.h (line 377)
	public final static short FOREIGN_KEY_MISSING= 151; // copied from mysql include/my_base.h (line 405)

	public final static int MAX_VERSIONS_PER_TABLE = 65536;
	public final static int MAX_GROUP_DEPTH = 256;
}
