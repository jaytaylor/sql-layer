package com.akiban.cserver;

import com.akiban.ais.model.Type;
import com.persistit.Key;

public enum Encoding {
	INT {

		@Override
		public int fromKey(FieldDef fieldDef, Key key, byte[] dest, int offset) {
			// TODO Auto-generated method stub
			return 0;
		}

		@Override
		public int fromString(FieldDef fieldDef, String value, byte[] dest,
				int offset) {
			// TODO Auto-generated method stub
			return 0;
		}

		@Override
		public void toKey(FieldDef fieldDef, RowData rowData, Key key) {
			// TODO Auto-generated method stub

		}

		@Override
		public void toString(FieldDef fieldDef, RowData rowData,
				StringBuilder sb) {
			// TODO Auto-generated method stub

		}

		@Override
		public void validate(Type type) {
			// TODO Auto-generated method stub

		}

	},

	U_INT {

		@Override
		public int fromKey(FieldDef fieldDef, Key key, byte[] dest, int offset) {
			// TODO Auto-generated method stub
			return 0;
		}

		@Override
		public int fromString(FieldDef fieldDef, String value, byte[] dest,
				int offset) {
			// TODO Auto-generated method stub
			return 0;
		}

		@Override
		public void toKey(FieldDef fieldDef, RowData rowData, Key key) {
			// TODO Auto-generated method stub

		}

		@Override
		public void toString(FieldDef fieldDef, RowData rowData,
				StringBuilder sb) {
			// TODO Auto-generated method stub

		}

		@Override
		public void validate(Type type) {
			// TODO Auto-generated method stub

		}

	},

	FLOAT {

		@Override
		public int fromKey(FieldDef fieldDef, Key key, byte[] dest, int offset) {
			// TODO Auto-generated method stub
			return 0;
		}

		@Override
		public int fromString(FieldDef fieldDef, String value, byte[] dest,
				int offset) {
			// TODO Auto-generated method stub
			return 0;
		}

		@Override
		public void toKey(FieldDef fieldDef, RowData rowData, Key key) {
			// TODO Auto-generated method stub

		}

		@Override
		public void toString(FieldDef fieldDef, RowData rowData,
				StringBuilder sb) {
			// TODO Auto-generated method stub

		}

		@Override
		public void validate(Type type) {
			// TODO Auto-generated method stub

		}
	},
	U_FLOAT {

		@Override
		public int fromKey(FieldDef fieldDef, Key key, byte[] dest, int offset) {
			// TODO Auto-generated method stub
			return 0;
		}

		@Override
		public int fromString(FieldDef fieldDef, String value, byte[] dest,
				int offset) {
			// TODO Auto-generated method stub
			return 0;
		}

		@Override
		public void toKey(FieldDef fieldDef, RowData rowData, Key key) {
			// TODO Auto-generated method stub

		}

		@Override
		public void toString(FieldDef fieldDef, RowData rowData,
				StringBuilder sb) {
			// TODO Auto-generated method stub

		}

		@Override
		public void validate(Type type) {
			// TODO Auto-generated method stub

		}
	},
	DECIMAL {
		@Override
		public int fromKey(FieldDef fieldDef, Key key, byte[] dest, int offset) {
			// TODO Auto-generated method stub
			return 0;
		}

		@Override
		public int fromString(FieldDef fieldDef, String value, byte[] dest,
				int offset) {
			// TODO Auto-generated method stub
			return 0;
		}

		@Override
		public void toKey(FieldDef fieldDef, RowData rowData, Key key) {
			// TODO Auto-generated method stub

		}

		@Override
		public void toString(FieldDef fieldDef, RowData rowData,
				StringBuilder sb) {
			// TODO Auto-generated method stub

		}

		@Override
		public void validate(Type type) {
			// TODO Auto-generated method stub

		}
	},
	U_DECIMAL {
		@Override
		public int fromKey(FieldDef fieldDef, Key key, byte[] dest, int offset) {
			// TODO Auto-generated method stub
			return 0;
		}

		@Override
		public int fromString(FieldDef fieldDef, String value, byte[] dest,
				int offset) {
			// TODO Auto-generated method stub
			return 0;
		}

		@Override
		public void toKey(FieldDef fieldDef, RowData rowData, Key key) {
			// TODO Auto-generated method stub

		}

		@Override
		public void toString(FieldDef fieldDef, RowData rowData,
				StringBuilder sb) {
			// TODO Auto-generated method stub

		}

		@Override
		public void validate(Type type) {
			// TODO Auto-generated method stub

		}
	},
	CHAR {
		@Override
		public int fromKey(FieldDef fieldDef, Key key, byte[] dest, int offset) {
			// TODO Auto-generated method stub
			return 0;
		}

		@Override
		public int fromString(FieldDef fieldDef, String value, byte[] dest,
				int offset) {
			// TODO Auto-generated method stub
			return 0;
		}

		@Override
		public void toKey(FieldDef fieldDef, RowData rowData, Key key) {
			// TODO Auto-generated method stub

		}

		@Override
		public void toString(FieldDef fieldDef, RowData rowData,
				StringBuilder sb) {
			// TODO Auto-generated method stub

		}

		@Override
		public void validate(Type type) {
			// TODO Auto-generated method stub

		}
	},
	VARCHAR {
		@Override
		public int fromKey(FieldDef fieldDef, Key key, byte[] dest, int offset) {
			// TODO Auto-generated method stub
			return 0;
		}

		@Override
		public int fromString(FieldDef fieldDef, String value, byte[] dest,
				int offset) {
			// TODO Auto-generated method stub
			return 0;
		}

		@Override
		public void toKey(FieldDef fieldDef, RowData rowData, Key key) {
			// TODO Auto-generated method stub

		}

		@Override
		public void toString(FieldDef fieldDef, RowData rowData,
				StringBuilder sb) {
			// TODO Auto-generated method stub

		}

		@Override
		public void validate(Type type) {
			// TODO Auto-generated method stub

		}
	},
	BLOB {
		@Override
		public int fromKey(FieldDef fieldDef, Key key, byte[] dest, int offset) {
			// TODO Auto-generated method stub
			return 0;
		}

		@Override
		public int fromString(FieldDef fieldDef, String value, byte[] dest,
				int offset) {
			// TODO Auto-generated method stub
			return 0;
		}

		@Override
		public void toKey(FieldDef fieldDef, RowData rowData, Key key) {
			// TODO Auto-generated method stub

		}

		@Override
		public void toString(FieldDef fieldDef, RowData rowData,
				StringBuilder sb) {
			// TODO Auto-generated method stub

		}

		@Override
		public void validate(Type type) {
			// TODO Auto-generated method stub

		}
	},
	DATE {
		@Override
		public int fromKey(FieldDef fieldDef, Key key, byte[] dest, int offset) {
			// TODO Auto-generated method stub
			return 0;
		}

		@Override
		public int fromString(FieldDef fieldDef, String value, byte[] dest,
				int offset) {
			// TODO Auto-generated method stub
			return 0;
		}

		@Override
		public void toKey(FieldDef fieldDef, RowData rowData, Key key) {
			// TODO Auto-generated method stub

		}

		@Override
		public void toString(FieldDef fieldDef, RowData rowData,
				StringBuilder sb) {
			// TODO Auto-generated method stub

		}

		@Override
		public void validate(Type type) {
			// TODO Auto-generated method stub

		}
	},
	DATETIME {

		@Override
		public int fromKey(FieldDef fieldDef, Key key, byte[] dest, int offset) {
			// TODO Auto-generated method stub
			return 0;
		}

		@Override
		public int fromString(FieldDef fieldDef, String value, byte[] dest,
				int offset) {
			// TODO Auto-generated method stub
			return 0;
		}

		@Override
		public void toKey(FieldDef fieldDef, RowData rowData, Key key) {
			// TODO Auto-generated method stub

		}

		@Override
		public void toString(FieldDef fieldDef, RowData rowData,
				StringBuilder sb) {
			// TODO Auto-generated method stub

		}

		@Override
		public void validate(Type type) {
			// TODO Auto-generated method stub

		}
	},
	TIMESTAMP {
		@Override
		public int fromKey(FieldDef fieldDef, Key key, byte[] dest, int offset) {
			// TODO Auto-generated method stub
			return 0;
		}

		@Override
		public int fromString(FieldDef fieldDef, String value, byte[] dest,
				int offset) {
			// TODO Auto-generated method stub
			return 0;
		}

		@Override
		public void toKey(FieldDef fieldDef, RowData rowData, Key key) {
			// TODO Auto-generated method stub

		}

		@Override
		public void toString(FieldDef fieldDef, RowData rowData,
				StringBuilder sb) {
			// TODO Auto-generated method stub

		}

		@Override
		public void validate(Type type) {
			// TODO Auto-generated method stub

		}
	},
	YEAR {
		@Override
		public int fromKey(FieldDef fieldDef, Key key, byte[] dest, int offset) {
			// TODO Auto-generated method stub
			return 0;
		}

		@Override
		public int fromString(FieldDef fieldDef, String value, byte[] dest,
				int offset) {
			// TODO Auto-generated method stub
			return 0;
		}

		@Override
		public void toKey(FieldDef fieldDef, RowData rowData, Key key) {
			// TODO Auto-generated method stub

		}

		@Override
		public void toString(FieldDef fieldDef, RowData rowData,
				StringBuilder sb) {
			// TODO Auto-generated method stub

		}

		@Override
		public void validate(Type type) {
			// TODO Auto-generated method stub

		}
	},
	SET {
		@Override
		public int fromKey(FieldDef fieldDef, Key key, byte[] dest, int offset) {
			// TODO Auto-generated method stub
			return 0;
		}

		@Override
		public int fromString(FieldDef fieldDef, String value, byte[] dest,
				int offset) {
			// TODO Auto-generated method stub
			return 0;
		}

		@Override
		public void toKey(FieldDef fieldDef, RowData rowData, Key key) {
			// TODO Auto-generated method stub

		}

		@Override
		public void toString(FieldDef fieldDef, RowData rowData,
				StringBuilder sb) {
			// TODO Auto-generated method stub

		}

		@Override
		public void validate(Type type) {
			// TODO Auto-generated method stub

		}
	},
	ENUM {
		@Override
		public int fromKey(FieldDef fieldDef, Key key, byte[] dest, int offset) {
			// TODO Auto-generated method stub
			return 0;
		}

		@Override
		public int fromString(FieldDef fieldDef, String value, byte[] dest,
				int offset) {
			// TODO Auto-generated method stub
			return 0;
		}

		@Override
		public void toKey(FieldDef fieldDef, RowData rowData, Key key) {
			// TODO Auto-generated method stub

		}

		@Override
		public void toString(FieldDef fieldDef, RowData rowData,
				StringBuilder sb) {
			// TODO Auto-generated method stub

		}

		@Override
		public void validate(Type type) {
			// TODO Auto-generated method stub

		}
	};

	public abstract void validate(final Type type);

	public abstract void toString(final FieldDef fieldDef,
			final RowData rowData, final StringBuilder sb);

	public abstract int fromString(final FieldDef fieldDef, final String value,
			final byte[] dest, final int offset);

	public abstract void toKey(final FieldDef fieldDef, final RowData rowData,
			final Key key);

	public abstract int fromKey(final FieldDef fieldDef, final Key key,
			final byte[] dest, final int offset);

}
