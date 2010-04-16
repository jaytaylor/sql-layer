package com.akiban.cserver;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import com.akiban.ais.model.Type;
import com.persistit.Key;

public enum Encoding {

	INT {

		@Override
		public int fromObject(FieldDef fieldDef, Object value, byte[] dest,
				int offset) {
			return objectToInt(dest, offset, value, fieldDef.getMaxStorageSize(),
					false);
		}

		@Override
		public void toKey(FieldDef fieldDef, RowData rowData, Key key) {
			final long location = fieldDef.getRowDef().fieldLocation(rowData,
					fieldDef.getFieldIndex());
			long v = rowData.getIntegerValue((int) location,
					(int) (location >>> 32));
			v <<= 64 - (fieldDef.getMaxStorageSize() * 8);
			v >>= 64 - (fieldDef.getMaxStorageSize() * 8);
			key.append(v);
		}

        @Override
        public void toKey(FieldDef fieldDef, Object value, Key key)
        {
            long v = ((Number)value).longValue();
            v <<= 64 - (fieldDef.getMaxStorageSize() * 8);
            v >>= 64 - (fieldDef.getMaxStorageSize() * 8);
            key.append(v);
        }

        @Override
		public void toString(FieldDef fieldDef, RowData rowData,
				StringBuilder sb, final Quote quote) {
			final long location = fieldDef.getRowDef().fieldLocation(rowData,
					fieldDef.getFieldIndex());
			long v = rowData.getIntegerValue((int) location,
					(int) (location >>> 32));
			//
			// getIntegerValue returns the unsigned interpretation of the value.
			// This pair of shift operations sign-extends the upper bit.
			//
			v <<= 64 - (fieldDef.getMaxStorageSize() * 8);
			v >>= 64 - (fieldDef.getMaxStorageSize() * 8);
			sb.append(v);
		}

		@Override
		public int widthFromObject(final FieldDef fieldDef, final Object value) {
			return fieldDef.getMaxStorageSize();
		}

		@Override
		public boolean validate(Type type) {
			long w = type.maxSizeBytes();
			return type.fixedSize() && w == 1 || w == 2 || w == 3 || w == 4
					|| w == 8;
		}
	},
	U_INT {

		@Override
		public int fromObject(FieldDef fieldDef, Object value, byte[] dest,
				int offset) {
			return objectToInt(dest, offset, value, fieldDef.getMaxStorageSize(),
					true);
		}

		@Override
		public void toKey(FieldDef fieldDef, RowData rowData, Key key) {
			final long location = fieldDef.getRowDef().fieldLocation(rowData,
					fieldDef.getFieldIndex());
			long v = rowData.getIntegerValue((int) location,
					(int) (location >>> 32));
			key.append(v);
		}

        @Override
        public void toKey(FieldDef fieldDef, Object value, Key key)
        {
            long v = ((Number)value).longValue();
            v <<= 64 - (fieldDef.getMaxStorageSize() * 8);
            v >>= 64 - (fieldDef.getMaxStorageSize() * 8);
            // TODO: unsigned long
            key.append(v);
        }

		@Override
		public void toString(FieldDef fieldDef, RowData rowData,
				StringBuilder sb, final Quote quote) {
			final long location = fieldDef.getRowDef().fieldLocation(rowData,
					fieldDef.getFieldIndex());
			sb.append(rowData.getIntegerValue((int) location,
					(int) (location >>> 32)));
		}

		@Override
		public int widthFromObject(final FieldDef fieldDef, final Object value) {
			return fieldDef.getMaxStorageSize();
		}

		@Override
		public boolean validate(Type type) {
			long w = type.maxSizeBytes();
			return type.fixedSize() && w == 1 || w == 2 || w == 3 || w == 4
					|| w == 8;
		}

	},
	FLOAT {

		@Override
		public int fromObject(FieldDef fieldDef, Object value, byte[] dest,
				int offset) {
			switch (fieldDef.getMaxStorageSize()) {
			case 4:
				return objectToFloat(dest, offset, value, false);
			case 8:
				return objectToDouble(dest, offset, value, false);
			default:
				throw new Error("Missing case");
			}
		}

		@Override
		public void toKey(FieldDef fieldDef, RowData rowData, Key key) {
			throw new UnsupportedOperationException();
		}

        @Override
        public void toKey(FieldDef fieldDef, Object value, Key key) {
            throw new UnsupportedOperationException();
        }

        @Override
		public void toString(FieldDef fieldDef, RowData rowData,
				StringBuilder sb, final Quote quote) {
			final long location = fieldDef.getRowDef().fieldLocation(rowData,
					fieldDef.getFieldIndex());
			long v = rowData.getIntegerValue((int) location,
					(int) (location >>> 32));
			switch (fieldDef.getMaxStorageSize()) {
			case 4:
				sb.append(Float.intBitsToFloat((int) v));
				break;
			case 8:
				sb.append(Double.longBitsToDouble(v));
				break;
			default:
				throw new Error("Missing case");
			}
		}

		@Override
		public int widthFromObject(final FieldDef fieldDef, final Object value) {
			return fieldDef.getMaxStorageSize();
		}

		@Override
		public boolean validate(Type type) {
			long w = type.maxSizeBytes();
			return type.fixedSize() && w == 4 || w == 8;
		}

	},
	U_FLOAT {

		@Override
		public int fromObject(FieldDef fieldDef, Object value, byte[] dest,
				int offset) {
			switch (fieldDef.getMaxStorageSize()) {
			case 4:
				return objectToFloat(dest, offset, value, true);
			case 8:
				return objectToDouble(dest, offset, value, true);
			default:
				throw new Error("Missing case");
			}
		}

		@Override
		public void toKey(FieldDef fieldDef, RowData rowData, Key key) {
			throw new UnsupportedOperationException();
		}

        @Override
        public void toKey(FieldDef fieldDef, Object value, Key key) {
            throw new UnsupportedOperationException();
        }

        @Override
		public void toString(FieldDef fieldDef, RowData rowData,
				StringBuilder sb, final Quote quote) {
			final long location = fieldDef.getRowDef().fieldLocation(rowData,
					fieldDef.getFieldIndex());
			long v = rowData.getIntegerValue((int) location,
					(int) (location >>> 32));
			switch (fieldDef.getMaxStorageSize()) {
			case 4:
				sb.append(Float.intBitsToFloat((int) v));
				break;
			case 8:
				sb.append(Double.longBitsToDouble(v));
				break;
			default:
				throw new Error("Missing case");
			}
		}

		@Override
		public int widthFromObject(final FieldDef fieldDef, final Object value) {
			return fieldDef.getMaxStorageSize();
		}

		@Override
		public boolean validate(Type type) {
			long w = type.maxSizeBytes();
			return type.fixedSize() && w == 4 || w == 8;
		}

	},
	DECIMAL {

		@Override
		public int fromObject(FieldDef fieldDef, Object value, byte[] dest,
				int offset) {
			throw new UnsupportedOperationException();
		}

		@Override
		public void toKey(FieldDef fieldDef, RowData rowData, Key key) {
			throw new UnsupportedOperationException();
		}

        @Override
        public void toKey(FieldDef fieldDef, Object value, Key key) {
            throw new UnsupportedOperationException();
        }

        @Override
		public void toString(FieldDef fieldDef, RowData rowData,
				StringBuilder sb, final Quote quote) {
			throw new UnsupportedOperationException();
		}

		@Override
		public int widthFromObject(final FieldDef fieldDef, final Object value) {
			throw new UnsupportedOperationException();
		}

		@Override
		public boolean validate(Type type) {
			throw new UnsupportedOperationException();
		}
	},
	U_DECIMAL {

		@Override
		public int fromObject(FieldDef fieldDef, Object value, byte[] dest,
				int offset) {
			throw new UnsupportedOperationException();
		}

		@Override
		public void toKey(FieldDef fieldDef, RowData rowData, Key key) {
			throw new UnsupportedOperationException();
		}

        @Override
        public void toKey(FieldDef fieldDef, Object value, Key key) {
            throw new UnsupportedOperationException();
        }

        @Override
		public void toString(FieldDef fieldDef, RowData rowData,
				StringBuilder sb, final Quote quote) {
			throw new UnsupportedOperationException();
		}

		@Override
		public int widthFromObject(final FieldDef fieldDef, final Object value) {
			throw new UnsupportedOperationException();
		}

		@Override
		public boolean validate(Type type) {
			throw new UnsupportedOperationException();
		}
	},
	VARCHAR {

		@Override
		public int fromObject(FieldDef fieldDef, Object value, byte[] dest,
				int offset) {
			return objectToString(value, dest, offset, fieldDef);
		}

		@Override
		public void toKey(FieldDef fieldDef, RowData rowData, Key key) {
			toKeyStringEncoding(fieldDef, rowData, key);
		}

        @Override
        public void toKey(FieldDef fieldDef, Object value, Key key)
        {
            key.append(value);
        }

        @Override
		public void toString(FieldDef fieldDef, RowData rowData,
				StringBuilder sb, final Quote quote) {
			final long location = fieldDef.getRowDef().fieldLocation(rowData,
					fieldDef.getFieldIndex());
			quote.append(sb, rowData.getStringValue((int) location,
					(int) (location >>> 32), fieldDef));
		}

		@Override
		public int widthFromObject(final FieldDef fieldDef, final Object value) {
			int prefixWidth = fieldDef.getPrefixSize();
			final String s = value == null ? "" : value.toString();
			return stringByteLength(s) + prefixWidth;
		}

		@Override
		public boolean validate(Type type) {
			long w = type.maxSizeBytes();
			return !type.fixedSize() && w < 65536 * 3;
		}

	},
	BLOB {
		// TODO - temporarily we handle just like VARCHAR
		@Override
		public int fromObject(FieldDef fieldDef, Object value, byte[] dest,
				int offset) {
			return objectToString(value, dest, offset, fieldDef);
		}

		@Override
		public void toKey(FieldDef fieldDef, RowData rowData, Key key) {
			toKeyStringEncoding(fieldDef, rowData, key);
		}

        @Override
        public void toKey(FieldDef fieldDef, Object value, Key key) {
            key.append(value);
        }

        @Override
		public void toString(FieldDef fieldDef, RowData rowData,
				StringBuilder sb, final Quote quote) {
			final long location = fieldDef.getRowDef().fieldLocation(rowData,
					fieldDef.getFieldIndex());
			quote.append(sb, rowData.getStringValue((int) location,
					(int) (location >>> 32), fieldDef));
		}

		@Override
		public int widthFromObject(final FieldDef fieldDef, final Object value) {
			final String s = value == null ? "" : value.toString();
			return s.length() + fieldDef.getPrefixSize();
		}

		@Override
		public boolean validate(Type type) {
			long w = type.maxSizeBytes();
			return !type.fixedSize();
		}
	},
	// TODO - temporarily we handle just like VARCHAR
	TEXT {
		@Override
		public int fromObject(FieldDef fieldDef, Object value, byte[] dest,
				int offset) {
			return objectToString(value, dest, offset, fieldDef);
		}

		@Override
		public void toKey(FieldDef fieldDef, RowData rowData, Key key) {
			toKeyStringEncoding(fieldDef, rowData, key);
		}

        @Override
        public void toKey(FieldDef fieldDef, Object value, Key key) {
            key.append(value);
        }

		@Override
		public void toString(FieldDef fieldDef, RowData rowData,
				StringBuilder sb, final Quote quote) {
			final long location = fieldDef.getRowDef().fieldLocation(rowData,
					fieldDef.getFieldIndex());
			quote.append(sb, rowData.getStringValue((int) location,
					(int) (location >>> 32), fieldDef));
		}

		@Override
		public int widthFromObject(final FieldDef fieldDef, final Object value) {
			final String s = value == null ? "" : value.toString();
			return s.length() + fieldDef.getPrefixSize();
		}

		@Override
		public boolean validate(Type type) {
			long w = type.maxSizeBytes();
			return !type.fixedSize();
		}
	},
	DATE {

		@Override
		public int fromObject(FieldDef fieldDef, Object value, byte[] dest,
				int offset) {
			final Date date;
			if (value instanceof String) {
				try {
					date = getDateFormat(SDF_DATE).parse((String) value);
				} catch (ParseException e) {
					throw new RuntimeException(e);
				}
			} else if (value instanceof Date) {
				date = (Date) value;
			} else {
				throw new IllegalArgumentException(
						"Requires a String or a Date");
			}
			int v = dateAsInt(date);
			return putUInt(dest, offset, v, 3);
		}

		@Override
		public void toKey(FieldDef fieldDef, RowData rowData, Key key) {
			final long location = fieldDef.getRowDef().fieldLocation(rowData,
					fieldDef.getFieldIndex());
			long v = rowData.getIntegerValue((int) location,
					(int) (location >>> 32));
			key.append(v);
		}

        @Override
        public void toKey(FieldDef fieldDef, Object value, Key key) {
            key.append(dateAsInt((Date) value));
        }

        @Override
		public void toString(FieldDef fieldDef, RowData rowData,
				StringBuilder sb, final Quote quote) {
			final long location = fieldDef.getRowDef().fieldLocation(rowData,
					fieldDef.getFieldIndex());
			final int v = (int) rowData.getIntegerValue((int) location, 3);
			final int year = v / (32 * 16);
			final int month = (v / 32) % 16;
			final int day = v % 32;
			final Date date = new Date(year - 1900, month - 1, day);
			quote.append(sb, getDateFormat(SDF_DATE).format(date));
		}

		@Override
		public int widthFromObject(final FieldDef fieldDef, final Object value) {
			return fieldDef.getMaxStorageSize();
		}

		@Override
		public boolean validate(Type type) {
			long w = type.maxSizeBytes();
			return type.fixedSize() && w == 3 || w == 4 || w == 8;
		}

        private int dateAsInt(Date date)
        {
            // This formula is specified here: http://dev.mysql.com/doc/refman/5.4/en/storage-requirements.html
            return ((date.getYear() + 1900) * 32 * 16) + (date.getMonth() * 32) + date.getDate();
        }
	},
	TIME {

		@Override
		public int fromObject(FieldDef fieldDef, Object value, byte[] dest,
				int offset) {
			final Date date;
			if (value instanceof String) {
				try {
					date = getDateFormat(SDF_TIME).parse((String) value);
				} catch (ParseException e) {
					throw new RuntimeException(e);
				}
			} else if (value instanceof Date) {
				date = (Date) value;
			} else {
				throw new IllegalArgumentException(
						"Requires a String or a Date");
			}
			int v = timeAsInt(date);
			return putUInt(dest, offset, v, 3);
		}

		@Override
		public void toKey(FieldDef fieldDef, RowData rowData, Key key) {
			final long location = fieldDef.getRowDef().fieldLocation(rowData,
					fieldDef.getFieldIndex());
			long v = rowData.getIntegerValue((int) location,
					(int) (location >>> 32));
			key.append(v);
		}

        @Override
        public void toKey(FieldDef fieldDef, Object value, Key key)
        {
            key.append(timeAsInt((Date) value));
        }
        
        @Override
		public void toString(FieldDef fieldDef, RowData rowData,
				StringBuilder sb, final Quote quote) {
			final long location = fieldDef.getRowDef().fieldLocation(rowData,
					fieldDef.getFieldIndex());
			final int v = (int) rowData.getIntegerValue((int) location, 3);
			// Note: reverse engineered; this does not match documentation
			// at http://dev.mysql.com/doc/refman/5.5/en/storage-requirements.html
			final int day = v / 1000000;
			final int hour = (v / 10000) % 100;
			final int minute = (v / 100) % 100;
			final int second = v % 100;
			final Date date = new Date(0, 0, day, hour, minute, second);
			quote.append(sb, getDateFormat(SDF_TIME).format(date));
		}

		@Override
		public int widthFromObject(final FieldDef fieldDef, final Object value) {
			return fieldDef.getMaxStorageSize();
		}

		@Override
		public boolean validate(Type type) {
			long w = type.maxSizeBytes();
			return type.fixedSize() && w == 3 || w == 4 || w == 8;
		}

        private int timeAsInt(Date date)
        {
            // This formula is specified here: http://dev.mysql.com/doc/refman/5.4/en/storage-requirements.html
            return date.getDate() * 24 * 3600 + date.getHours() * 3600 + date.getMinutes() * 60 + date.getSeconds();
        }
	},
	DATETIME {

		@Override
		public int fromObject(FieldDef fieldDef, Object value, byte[] dest,
				int offset) {
			final Date date;
			if (value instanceof String) {
				try {
					date = getDateFormat(SDF_DATETIME).parse((String) value);
				} catch (ParseException e) {
					throw new RuntimeException(e);
				}
			} else if (value instanceof Date) {
				date = (Date) value;
			} else {
				throw new IllegalArgumentException(
						"Requires a String or a Date");
			}
			long v = dateTimeAsLong(date);
			return putUInt(dest, offset, v, 8);
		}

		@Override
		public void toKey(FieldDef fieldDef, RowData rowData, Key key) {
			final long location = fieldDef.getRowDef().fieldLocation(rowData,
					fieldDef.getFieldIndex());
			long v = rowData.getIntegerValue((int) location,
					(int) (location >>> 32));
			key.append(v);
		}

        @Override
        public void toKey(FieldDef fieldDef, Object value, Key key) {
            key.append(dateTimeAsLong((Date) value));
        }

        @Override
		public void toString(FieldDef fieldDef, RowData rowData,
				StringBuilder sb, final Quote quote) {
			final long location = fieldDef.getRowDef().fieldLocation(rowData,
					fieldDef.getFieldIndex());
			final long v = rowData.getIntegerValue((int) location, 8);
			// Note: reverse engineered; this does not match documentation
			// at http://dev.mysql.com/doc/refman/5.5/en/storage-requirements.html
			final int year = (int) (v / LONG_1_E10);
			final int month = (int) ((v / LONG_1_E8) % 100);
			final int day = (int) ((v / LONG_1_E6) % 100);
			final int hour = (int) ((v / LONG_1_E4) % 100);
			final int minute = (int) ((v / LONG_100) % 100);
			final int second = (int) (v % LONG_100);
			final Date date = new Date(year - 1900, month - 1, day, hour,
					minute, second);
			quote.append(sb, getDateFormat(SDF_DATETIME).format(date));
		}

		@Override
		public int widthFromObject(final FieldDef fieldDef, final Object value) {
			return fieldDef.getMaxStorageSize();
		}

		@Override
		public boolean validate(Type type) {
			long w = type.maxSizeBytes();
			return type.fixedSize() && w == 8;
		}

        private long dateTimeAsLong(Date date)
        {
            // This is NOT what the documentation says: http://dev.mysql.com/doc/refman/5.4/en/storage-requirements.html.
            // This formula is based on Peter's reverse engineering of mysql packed data.
            return ((date.getYear() + 1900) * LONG_1_E10) +
                   (date.getMonth() * LONG_1_E8) +
                   (date.getDate() * LONG_1_E6) +
                   (date.getHours() * LONG_1_E4) +
                   (date.getMinutes() * LONG_100) +
                   (date.getSeconds());
        }

        private static final long LONG_100   = 100;
        private static final long LONG_1_E4  = LONG_100 * 100;
        private static final long LONG_1_E6  = LONG_1_E4 * 100;
        private static final long LONG_1_E8  = LONG_1_E6 * 100;
        private static final long LONG_1_E10 = LONG_1_E8 * 100;
	},
	TIMESTAMP {

		@Override
		public int fromObject(FieldDef fieldDef, Object value, byte[] dest,
				int offset) {
			final Date date;
			if (value instanceof String) {
				try {
					date = getDateFormat(SDF_DATETIME).parse((String) value);
				} catch (ParseException e) {
					throw new RuntimeException(e);
				}
			} else if (value instanceof Date) {
				date = (Date) value;
			} else if (value instanceof Number) {
				date = new Date(((Number) value).longValue());
			} else {
				throw new IllegalArgumentException(
						"Requires a String or a Date");
			}
			final int v = (int) (date.getTime() / 1000);
			return putUInt(dest, offset, v, 4);
		}

		@Override
		public void toKey(FieldDef fieldDef, RowData rowData, Key key) {
			final long location = fieldDef.getRowDef().fieldLocation(rowData,
					fieldDef.getFieldIndex());
			long v = rowData.getIntegerValue((int) location,
					(int) (location >>> 32));
			key.append(v);
		}

        @Override
        public void toKey(FieldDef fieldDef, Object value, Key key) {
            key.append(((Date)value).getTime() / 1000);
        }

        @Override
		public void toString(FieldDef fieldDef, RowData rowData,
				StringBuilder sb, final Quote quote) {
			final long location = fieldDef.getRowDef().fieldLocation(rowData,
					fieldDef.getFieldIndex());
			final long time = ((long) rowData
					.getIntegerValue((int) location, 4)) * 1000;
			final Date date = new Date(time);
			quote.append(sb, getDateFormat(SDF_DATETIME).format(date));
		}

		@Override
		public int widthFromObject(final FieldDef fieldDef, final Object value) {
			return fieldDef.getMaxStorageSize();
		}

		@Override
		public boolean validate(Type type) {
			long w = type.maxSizeBytes();
			return type.fixedSize() && w == 4;
		}
	},
	YEAR {

		@Override
		public int fromObject(FieldDef fieldDef, Object value, byte[] dest,
				int offset) {
			final Date date;
			if (value instanceof String) {
				try {
					date = getDateFormat(SDF_YEAR).parse((String) value);
				} catch (ParseException e) {
					throw new RuntimeException(e);
				}
			} else if (value instanceof Date) {
				date = (Date) value;
			} else {
				throw new IllegalArgumentException(
						"Requires a String or a Date");
			}
			int v = (date.getYear() - 1900);
			return putUInt(dest, offset, v, 3);
		}

		@Override
		public void toKey(FieldDef fieldDef, RowData rowData, Key key) {
			final long location = fieldDef.getRowDef().fieldLocation(rowData,
					fieldDef.getFieldIndex());
			long v = rowData.getIntegerValue((int) location,
					(int) (location >>> 32));
			key.append(v);
		}

        @Override
        public void toKey(FieldDef fieldDef, Object value, Key key) {
            key.append(((Date)value).getYear() - 1900);
        }

        @Override
		public void toString(FieldDef fieldDef, RowData rowData,
				StringBuilder sb, final Quote quote) {
			final long location = fieldDef.getRowDef().fieldLocation(rowData,
					fieldDef.getFieldIndex());
			final int year = (int) rowData.getIntegerValue((int) location, 1);
			quote.append(sb, Integer.toString(year + 1900));
		}

		@Override
		public int widthFromObject(final FieldDef fieldDef, final Object value) {
			return fieldDef.getMaxStorageSize();
		}

		@Override
		public boolean validate(Type type) {
			long w = type.maxSizeBytes();
			return type.fixedSize() && w == 1;
		}
	},
	SET {

		@Override
		public int fromObject(FieldDef fieldDef, Object value, byte[] dest,
				int offset) {
			throw new UnsupportedOperationException();
		}

		@Override
		public void toKey(FieldDef fieldDef, RowData rowData, Key key) {
			throw new UnsupportedOperationException();
		}

        @Override
        public void toKey(FieldDef fieldDef, Object value, Key key) {
            throw new UnsupportedOperationException();
        }

        @Override
		public void toString(FieldDef fieldDef, RowData rowData,
				StringBuilder sb, final Quote quote) {
			throw new UnsupportedOperationException();
		}

		@Override
		public int widthFromObject(final FieldDef fieldDef, final Object value) {
			return fieldDef.getMaxStorageSize();
		}

		@Override
		public boolean validate(Type type) {
			throw new UnsupportedOperationException();
		}
	},
	ENUM {

		@Override
		public int fromObject(FieldDef fieldDef, Object value, byte[] dest,
				int offset) {
			throw new UnsupportedOperationException();
		}

		@Override
		public void toKey(FieldDef fieldDef, RowData rowData, Key key) {
			throw new UnsupportedOperationException();
		}

        @Override
        public void toKey(FieldDef fieldDef, Object value, Key key) {
            throw new UnsupportedOperationException();
        }

        @Override
		public void toString(FieldDef fieldDef, RowData rowData,
				StringBuilder sb, final Quote quote) {
			throw new UnsupportedOperationException();
		}

		@Override
		public int widthFromObject(final FieldDef fieldDef, final Object value) {
			return fieldDef.getMaxStorageSize();
		}

		@Override
		public boolean validate(Type type) {
			throw new UnsupportedOperationException();
		}
	},
	BIT {

		@Override
		public int fromObject(FieldDef fieldDef, Object value, byte[] dest,
				int offset) {
			throw new UnsupportedOperationException();
		}

		@Override
		public void toKey(FieldDef fieldDef, RowData rowData, Key key) {
			throw new UnsupportedOperationException();
		}

        @Override
        public void toKey(FieldDef fieldDef, Object value, Key key) {
            throw new UnsupportedOperationException();
        }

        @Override
		public void toString(FieldDef fieldDef, RowData rowData,
				StringBuilder sb, final Quote quote) {
			throw new UnsupportedOperationException();
		}

		@Override
		public int widthFromObject(final FieldDef fieldDef, final Object value) {
			return fieldDef.getMaxStorageSize();
		}

		@Override
		public boolean validate(Type type) {
			throw new UnsupportedOperationException();
		}
	};

	public final static String SDF_DATE = "yyyy-MM-dd";

	public final static String SDF_YEAR = "yyyy";

	public final static String SDF_DATETIME = "yyyy-MM-dd HH:mm:ss";

	public final static String SDF_TIME = "HH:mm:ss";

	private static ThreadLocal<Map<String, SimpleDateFormat>> SDF_MAP_THREAD_LOCAL = new ThreadLocal<Map<String, SimpleDateFormat>>();

	public static SimpleDateFormat getDateFormat(final String pattern) {
		Map<String, SimpleDateFormat> formatMap = SDF_MAP_THREAD_LOCAL.get();
		if (formatMap == null) {
			formatMap = new HashMap<String, SimpleDateFormat>();
			SDF_MAP_THREAD_LOCAL.set(formatMap);
		}
		SimpleDateFormat sdf = formatMap.get(pattern);
		if (sdf == null) {
			sdf = new SimpleDateFormat(pattern);
			formatMap.put(pattern, sdf);
		} else {
			sdf.getCalendar().clear();
		}
		return sdf;
	}

	public abstract boolean validate(final Type type);

	public abstract void toString(final FieldDef fieldDef,
			final RowData rowData, final StringBuilder sb, final Quote quote);

	public abstract int fromObject(final FieldDef fieldDef, final Object value,
			final byte[] dest, final int offset);

	public abstract int widthFromObject(final FieldDef fieldDef,
			final Object value);

	public abstract void toKey(final FieldDef fieldDef, final RowData rowData,
			final Key key);

    public abstract void toKey(final FieldDef fieldDef, final Object value,
            final Key key);

	public int objectToInt(final byte[] bytes, final int offset,
			final Object obj, final int width, final boolean unsigned) {

		final long value;
		if (obj instanceof Number) {
			value = ((Number) obj).longValue();
		} else if (obj instanceof String) {
			value = Long.parseLong((String) obj);
		} else if (obj == null) {
			value = 0;
		} else {
			throw new IllegalArgumentException(obj
					+ " must be a Number or a String");
		}
		if (unsigned) {
			return putUInt(bytes, offset, value, width);
		} else {
			return putInt(bytes, offset, value, width);
		}
	}

	public int objectToFloat(final byte[] bytes, final int offset,
			final Object obj, final boolean unsigned) {
		float f;
		if (obj instanceof Number) {
			f = ((Number) obj).floatValue();
		} else if (obj instanceof String) {
			f = Float.parseFloat((String) obj);
		} else if (obj == null) {
			f = 0f;
		} else
			throw new IllegalArgumentException(obj
					+ " must be a Number or a String");
		if (unsigned) {
			f = Math.max(0f, f);
		}
		return putInt(bytes, offset, Float.floatToIntBits(f), 4);
	}

	public int objectToDouble(final byte[] bytes, final int offset,
			final Object obj, final boolean unsigned) {
		double d;
		if (obj instanceof Number) {
			d = ((Number) obj).doubleValue();
		} else if (obj instanceof String) {
			d = Double.parseDouble((String) obj);
		} else if (obj == null) {
			d = 0d;
		} else
			throw new IllegalArgumentException(obj
					+ " must be a Number or a String");
		if (unsigned) {
			d = Math.max(0d, d);
		}
		return putInt(bytes, offset, Double.doubleToLongBits(d), 8);
	}

	/**
	 * Writes a VARCHAR or CHAR: inserts the correct-sized PREFIX for MySQL
	 * VARCHAR. Assumes US-ASCII encoding, for now. Can be used temporarily for
	 * the BLOB types as well.
	 * 
	 * @param obj
	 * @param bytes
	 * @param offset
	 * @param fieldDef
	 * @return
	 */
	public int objectToString(final Object obj, final byte[] bytes,
			final int offset, final FieldDef fieldDef) {
		final String s = obj == null ? "" : obj.toString();
		int prefixSize = fieldDef.getPrefixSize();
		final byte[] b = stringBytes(s);
		final int size = b.length;
		switch (prefixSize) {
		case 0:
			break;
		case 1:
			CServerUtil.putByte(bytes, offset, size);
			break;
		case 2:
			CServerUtil.putChar(bytes, offset, size);
			break;
		case 3:
			CServerUtil.putMediumInt(bytes, offset, size);
			break;
		case 4:
			CServerUtil.putInt(bytes, offset, size);
			break;
		default:
			throw new Error("Missing case");
		}
		System.arraycopy(b, 0, bytes, offset + prefixSize, size);

		return prefixSize + size;
	}

	int putInt(final byte[] bytes, final int offset, final long value,
			final int width) {
		switch (width) {
		case 1:
			CServerUtil.putByte(bytes, offset, (byte) value);
			break;
		case 2:
			CServerUtil.putShort(bytes, offset, (short) value);
			break;
		case 3:
			CServerUtil.putMediumInt(bytes, offset, (int) value);
			break;
		case 4:
			CServerUtil.putInt(bytes, offset, (int) value);
			break;
		case 8:
			CServerUtil.putLong(bytes, offset, value);
			break;
		default:
			throw new IllegalStateException("Width not supported");
		}
		return width;
	}

	int putUInt(final byte[] bytes, final int offset, final long value,
			final int width) {
		switch (width) {
		case 1:
			CServerUtil.putByte(bytes, offset, (int) (value < 0 ? 0 : value));
			break;
		case 2:
			CServerUtil.putShort(bytes, offset, (int) (value < 0 ? 0 : value));
			break;
		case 3:
			CServerUtil.putMediumInt(bytes, offset, (int) (value < 0 ? 0
					: value));
			break;
		case 4:
			CServerUtil.putInt(bytes, offset, (int) (value < 0 ? 0 : value));
			break;
		case 8:
			CServerUtil.putLong(bytes, offset, value);
			break;
		default:
			throw new IllegalStateException("Width not supported");
		}
		return width;
	}

	void toKeyStringEncoding(final FieldDef fieldDef, final RowData rowData,
			final Key key) {
		final long location = fieldDef.getRowDef().fieldLocation(rowData,
				fieldDef.getFieldIndex());
		key.append(CServerUtil
				.decodeMySQLString(rowData.getBytes(), (int) location,
						(int) (location >>> 32), fieldDef));
	}

	// TODO -
	// These methods destroy character encoding - I added them just to get over
	// a unit test problem in loading xxxxxxxx data in which actual data is
	// loaded.
	// We will need to implement character encoding properly to handle
	// xxxxxxxx data properly.
	//

	int stringByteLength(final String s) {
		return s.length();
	}

	byte[] stringBytes(final String s) {
		final byte[] b = new byte[s.length()];
		for (int i = 0; i < b.length; i++) {
			b[i] = (byte) s.charAt(i);
		}
		return b;
	}

}
