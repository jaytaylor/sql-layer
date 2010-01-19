package com.akiba.cserver;


/**
 * Enumeration of MySQL field types. Not all types are supported yet.
 * 
 * @author peter
 * 
 */
public enum FieldType {
	TINYINT(true, 1, 1) {
	},

	SMALLINT(true, 2, 2) {
	},
	
	MEDIUMINT(true, 3, 3) {
	},

	INT(true, 4, 4) {
	},

	BIGINT(true, 8, 8) {
	},

	FLOAT(true, 4, 4) {

	},

	DOUBLE(true, 8, 8) {

	},

	U_TINYINT(true, 1, 1) {
	},

	U_MEDIUMINT(true, 3, 3) {
	},

	U_SMALLINT(true, 2, 2) {
	},

	U_INT(true, 4, 4) {
	},

	U_BIGINT(false, 8, 8) {
	},

	DATE(true, 4, 4) {
	},

	DATETIME(true, 8, 8) {
	},

	TIMESTAMP(true, 4, 4) {
	},

	YEAR(true, 2, 2) {
	},

	CHAR(false, 1, 255) {
	},

	VARCHAR(false, 1, 65535) {
	},

	// Same as CHAR, but with BINARY mode specified - means case sensitive
	// sorting
	BINCHAR(true, 1, 255) {
	},

	// Same as VARCHAR, but with BINARY mode specified - means case sensitive
	// sorting
	BINVARCHAR(true, 1, 65535) {
	},
	
	TINYBLOB(true, 0, 255) {
		
	},

	BLOB(true, 0, 65536) {
		
	},

	MEDIUMBLOB(true, 0, 0xFFFFFF) {
		
	},

	LONGBLOB(true, 0, Integer.MAX_VALUE) {
		
	},

	TINYTEXT(true, 0, 255) {
		
	},

	TEXT(true, 0, 65536) {
		
	},

	MEDIUMTEXT(true, 0, 0xFFFFFF) {
		
	},

	LONGTEXT(true, 0, Integer.MAX_VALUE) {
		
	},


//	ENUM(true, 2, 2) {
//	},
//
//	SET(true, 0, 128) {
//	}
	;

	FieldType(final boolean exactKeyCoding, final int minWidth,
			final int maxWidth) {
		this.exactKeyCoding = exactKeyCoding;
		this.minWidth = minWidth;
		this.maxWidth = maxWidth;
	}

	private final boolean exactKeyCoding;
	private final int minWidth;
	private final int maxWidth;

	// public abstract void fromKey(final RowData rowData, final int field,
	// final Key key);
	//
	// public abstract void toKey(final RowData rowData, final int field, final
	// Key key);

	public int getMinWidth() {
		return minWidth;
	}

	public int getMaxWidth() {
		return maxWidth;
	}

	public boolean isFixedWidth() {
		return minWidth == maxWidth;
	}
	
}
