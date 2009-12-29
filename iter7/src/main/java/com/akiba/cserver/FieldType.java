package com.akiba.cserver;

import com.persistit.Key;

/**
 * Enumeration of MySQL field types.  Not all types are supported yet.  I got
 * definitions from:
 * 
 * http://help.scibit.com/Mascon/masconMySQL_Field_Types.html
 * 
 * which certainly may not be authoritative!
 * 
 * @author peter
 *
 */
public enum FieldType {
		TINYINT(true, 1, 1) {
		},

		SMALLINT(true, 2, 2) {
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

		U_SMALLINT(true, 2, 2) {
		},
		
		U_INT(true, 4, 4) {
		},

		// Note - not handling correctly as unsigned long at this time.  Need to extend Persistit
		// Key encoding to handle this type.
		U_BIGINT(false, 8, 8) {
		},
		
		DATE(true, 8, 8) {	
		},
		
		DATETIME(true, 8, 8) {	
		},
		
		TIMESTAMP(true, 8, 8) {	
		},
		
		YEAR(true, 2, 2) {	
		},
		
		CHAR(false, 1, 255) {
		},
		
		VARCHAR(false, 1, 255) {
		},
		
		// Same as CHAR, but with BINARY mode specified - means case sensitive sorting
		B_CHAR(true, 1, 255) {
		},
		
		// Same as VARCHAR, but with BINARY mode specified - means case sensitive sorting
		B_VARCHAR(true, 1, 255) {
		},
		
		ENUM(true, 2, 2) {
		},
		
		SET(true, 0, 128) {
		};
		
		
		FieldType(final boolean exactKeyCoding, final int minWidth, final int maxWidth) {
			this.exactKeyCoding = exactKeyCoding;
			this.minWidth = minWidth;
			this.maxWidth = maxWidth;
		}
		
		private final boolean exactKeyCoding;
		private final int minWidth;
		private final int maxWidth;
		
//		public abstract void fromKey(final RowData rowData, final int field, final Key key);
//
//		public abstract void toKey(final RowData rowData, final int field, final Key key);

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
