package com.akiba.cserver;

/**
 * Contain the relevant schema information for one version of a table
 * definition. Instances of this class acquire table definition data from the
 * AIS, interpret it, and provide information on how to encode and decode fields
 * from a RowData structure.
 * 
 * 
 * @author peter
 * 
 */
public class RowDef {

	/**
	 * Array of FieldDef, one per column
	 */
	private final FieldDef[] fieldDefs;

	/**
	 * Unique, permanent handle for a version of a table definition
	 */
	private final int rowDefId;

	/**
	 * Field(s) that constitute the primary key for this table. Must not be
	 * empty; will usually have one element. Multiple elements define a compound
	 * primary key.
	 */
	private int[] pkFields;

	/**
	 * Parent table. This denotes the table hierarchy within a group. Value is
	 * zero, meaning there is no parent RowDef, if this table is a root table.
	 */
	private int parentRowDefId;

	/**
	 * Field(s) that constitute the foreign key by which this row is joined to
	 * its parent table.
	 */
	private int[] parentJoinFields;

	/**
	 * Schema's name for this table.
	 */
	private String tableName;

	/**
	 * Cached name of the primary key index tree
	 */
	private String pkTreeName;
	/**
	 * Array computed by the {@link #preComputeFieldCoordinates(FieldDef[])}
	 * method to assist in looking up a field's offset and length.
	 */
	private final int[][] fieldCoordinates;

	/**
	 * Array computed by the {@link #preComputeFieldCoordinates(FieldDef[])}
	 * method to assist in looking up a field's offset and length.
	 */
	private final byte[][] varLenFieldMap;

	public static RowDef createRowDef(final int rowDefId,
			final FieldDef[] fieldDefs, final String tableName,
			final int[] pkFields) {
		final RowDef rowDef = new RowDef(rowDefId, fieldDefs);
		rowDef.setTableName(tableName);
		rowDef.setPkFields(pkFields);
		rowDef.setParentRowDefId(0);
		rowDef.setParentJoinFields(new int[0]);
		return rowDef;
	}

	public static RowDef createRowDef(final int rowDefId,
			final FieldDef[] fieldDefs, final String tableName,
			final int[] pkFields, final int parentRowDefId,
			final int[] parentJoinFields) {
		final RowDef rowDef = new RowDef(rowDefId, fieldDefs);
		rowDef.setTableName(tableName);
		rowDef.setPkFields(pkFields);
		rowDef.setParentRowDefId(parentRowDefId);
		rowDef.setParentJoinFields(parentJoinFields);
		return rowDef;
	}

	public RowDef(final int rowDefId, final FieldDef[] fieldDefs) {
		this.rowDefId = rowDefId;
		this.fieldDefs = fieldDefs;
		fieldCoordinates = new int[(fieldDefs.length + 7) / 8][];
		varLenFieldMap = new byte[(fieldDefs.length + 7) / 8][];
		preComputeFieldCoordinates(fieldDefs);
	}

	public int getFieldCount() {
		return fieldDefs.length;
	}

	public FieldDef getFieldDef(final int index) {
		return fieldDefs[index];
	}

	public int getRowDefId() {
		return rowDefId;
	}

	public int[] getPkFields() {
		return pkFields;
	}

	public void setPkFields(int[] pkFields) {
		this.pkFields = pkFields;
	}

	public int getParentRowDefId() {
		return parentRowDefId;
	}

	public void setParentRowDefId(int parentRowDefId) {
		this.parentRowDefId = parentRowDefId;
	}

	public int[] getParentJoinFields() {
		return parentJoinFields;
	}

	public void setParentJoinFields(int[] parentJoinFields) {
		this.parentJoinFields = parentJoinFields;
	}

	public FieldDef[] getFieldDefs() {
		return fieldDefs;
	}

	public String getPkTreeName() {
		return pkTreeName;
	}

	public String getTableName() {
		return tableName;
	}

	public void setTableName(String tableName) {
		this.tableName = tableName;
		this.pkTreeName = tableName + "_pk";
	}

	/**
	 * Returns the offset relative to the start of the byte array represented by
	 * the supplied {@link RowData} of the field specified by the supplied
	 * fieldIndex. The location is a long which encodes an offset to the data
	 * and its length in bytes. If <code>offset</code> and <code>length</code>
	 * are the offset and length of the data, respectively, then the location is
	 * returned as
	 * 
	 * <code>(long)offset+((long)length << 32)</code>
	 * 
	 * For fixed-length fields like numbers, the length is the fixed length of
	 * the field, e.g., 8 for BIGINT values. For variable- length fields the
	 * length is the number of bytes used in representing the value.
	 * 
	 * @param rowData
	 * @param fieldIndex
	 * @return
	 */

	public long fieldLocation(final RowData rowData, final int fieldIndex) {
		final int fieldCount = fieldDefs.length;
		if (fieldIndex < 0 || fieldIndex >= fieldCount) {
			throw new IllegalArgumentException("Field index out of bounds: "
					+ fieldIndex);
		}
		int dataStart = rowData.getRowStartData();
		//
		// If NullMap bit is set, return zero immediately
		//
		if (((rowData.getColumnMapByte(fieldIndex / 8) >>> (fieldIndex % 8)) & 1) == 1) {
			return 0;
		}

		if (fieldDefs[fieldIndex].isFixedWidth()) {
			//
			// Look up the offset and width of a fixed-width field
			//
			int offset = dataStart;
			int width = 0;
			//
			// Loop over NullMap bytes until reaching fieldIndex
			//
			for (int k = 0; k <= fieldIndex; k += 8) {
				int byteIndex = k >>> 3;
				int mapByte = (~rowData.getColumnMapByte(byteIndex)) & 0xFF;
				//
				// Look up offset and width in the statically-generated
				// fieldCoordinates array.
				//
				int bitCount = fieldIndex - k;
				if (bitCount < 8) {
					mapByte &= (0xFF >>> (7 - bitCount));
				}
				int fc = fieldCoordinates[byteIndex][mapByte];
				//
				// Decode the offset and width fields
				//
				width = ((int) fc) >>> 24;
				offset += (int) (fc & 0xFFFFFF) + width;
			}
			//
			// Encode the width and offset fields
			//
			return (long) ((offset & 0xFFFFFF) - width)
					| (((long) width) << 32);
		} else {
			//
			// Look up the offset and width of a variable-width field.
			// Previous and current refer to the coordinates of the
			// fixed-length fields delimiting the variable-length
			// data.
			//
			int offset = dataStart;
			int width = 0;
			int previous = 0;
			int current = 0;

			for (int k = 0; k < fieldCount; k += 8) {
				if (k <= fieldIndex) {
					current = current + width;
				}
				int byteIndex = k >>> 3;
				int mapByte = (~rowData.getColumnMapByte(byteIndex)) & 0xFF;
				int bitCount1 = fieldIndex - k;
				//
				// Look up offset and width in the statically-generated
				// fieldCoordinates array.
				//
				if (k <= fieldIndex) {
					int mbb1 = mapByte;
					int mask = 0xFF;
					if (bitCount1 < 7) {
						mask >>>= (7 - bitCount1);
						mbb1 &= mask;
					}
					int fc1 = fieldCoordinates[byteIndex][mbb1];
					current = fc1 + (current & 0xFFFFFF);
					int mbb2 = varLenFieldMap[byteIndex][bitCount1 < 8 ? mbb1
							: mbb1 + 256] & 0xFF;
					if (mbb2 != 0) {
						int fc2 = fieldCoordinates[byteIndex][mbb2];
						previous = current + fc2 - fc1;
					}
				}
				//
				// In addition, because the overall size of the fixed-length
				// field array is not encoded, we need to compute the offset
				// of the byte after the last field. That's where the variable-
				// length bytes begin.
				//
				int bitCount2 = fieldCount - k;
				int mbb2 = mapByte;
				if (bitCount2 > 0 && bitCount2 < 8) {
					mbb2 &= (0xFF >>> (8 - bitCount2));
				}
				int fc = fieldCoordinates[byteIndex][mbb2];
				//
				// Decode the offset and width of the last field
				//
				width = ((int) fc) >>> 24;
				offset += (int) (fc & 0xFFFFFF) + width;
			}
			//
			// Compute the starting and ending offsets (from the beginning of
			// the rowData byte array) of the variable-length segment.
			//
			int start = (int) rowData.getIntegerValue((previous & 0xFFFFFF)
					+ dataStart, previous >>> 24);

			int end = (int) rowData.getIntegerValue((current & 0xFFFFFF)
					+ dataStart, current >>> 24);
			//
			// Encode and return the offset and length
			//
			return (long) (start + offset) | ((long) (end - start) << 32);
		}
	}

	/**
	 * Compute lookup tables used to in the {@link #fieldLocation(RowData, int)}
	 * method. This method is invoked once when a RowDef is first constructed.
	 * 
	 * @param fieldDefs
	 */
	void preComputeFieldCoordinates(final FieldDef[] fieldDefs) {
		final int fieldCount = fieldDefs.length;
		int voffset = 0;
		for (int field = 0; field < fieldCount; field++) {
			final FieldDef fieldDef = fieldDefs[field];
			final int byteIndex = field / 8;
			final int bitIndex = field % 8;
			final int bit = 1 << bitIndex;
			if (bitIndex == 0) {
				fieldCoordinates[byteIndex] = new int[256];
				varLenFieldMap[byteIndex] = new byte[512];
			}
			final int width;
			if (fieldDef.isFixedWidth()) {
				width = fieldDef.getMinWidth();
			} else {
				voffset += fieldDef.getMaxWidth();
				width = voffset == 0 ? 0 : voffset < 0x100 ? 1
						: voffset < 0x10000 ? 2 : voffset < 0x1000000 ? 3 : 4;
			}
			for (int i = 0; i < bit; i++) {
				int from = fieldCoordinates[byteIndex][i];
				int to = ((from & 0xFFFFFF) + (from >>> 24)) | (width << 24);
				int k = i + bit;
				fieldCoordinates[byteIndex][k] = to;
				for (int j = bitIndex; --j >= 0;) {
					if ((k & (1 << j)) != 0
							&& !fieldDefs[byteIndex * 8 + j].isFixedWidth()) {
						varLenFieldMap[byteIndex][k] = (byte) (k & ((0xFF >>> (7 - j))));
						break;
					}
				}
				for (int j = bitIndex + 1; --j >= 0;) {
					if ((k & (1 << j)) != 0
							&& !fieldDefs[byteIndex * 8 + j].isFixedWidth()) {
						varLenFieldMap[byteIndex][k + 256] = (byte) (k & ((0xFF >>> (7 - j))));
						break;
					}
				}
			}
		}
	}

	/**
	 * An implementation useful while debugging. TODO - replace with something
	 * less verbose.
	 */
	@Override
	public String toString() {
		return debugToString();
	}

	/**
	 * Display the fieldCoordinates array
	 */
	public String debugToString() {
		StringBuilder sb = new StringBuilder();
		sb.append("[");
		for (int i = 0; i < fieldDefs.length; i++) {
			if (i != 0)
				sb.append(",");
			sb.append(fieldDefs[i]);
		}
		sb.append("]\n");
		for (int i = 0; i < fieldCoordinates.length; i++) {
			sb.append("--- " + i + " ---\n");
			int count = 256;
			int remainingBits = fieldDefs.length - (i * 8);
			if (remainingBits >= 0 && remainingBits < 8) {
				count = 1 << remainingBits;
			}
			for (int j = 0; j < count; j += 16) {
				for (int k = 0; k < 16; k++) {
					sb.append((k % 8) == 0 ? "   " : " ");
					Util.hex(sb, fieldCoordinates[i][j + k], 8);
				}
				sb.append("\n");
			}
		}
		sb.append("\n");
		return sb.toString();
	}
}
