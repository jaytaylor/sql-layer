package com.akiba.cserver;

public interface CServerConstants {

	public final static int OK = 1;
	public final static int END = 2;
	public final static int ERR = 100;
	public final static int MISSING_OR_CORRUPT_ROW_DEF = 99;
	public final static int NON_UNIQUE = 101;
	public final static int FOREIGN_KEY_MISSING= 102;

	public final static int MAX_VERSIONS_PER_TABLE = 65536;
}
