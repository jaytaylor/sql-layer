package com.akiba.cserver;

/**
 * Contain the relevant schema information for one version of a table definition.
 * Instances of this class acquire table definition data from the AIS, interpret
 * it, and provide information on how to encode and decode fields from a RowData
 * structure.
 * 
 * 
 * @author peter
 *
 */
public class TVSchema {


	private final FieldType[] fieldTypes;
	
	private final long[][] fieldOffsets;
	
	public TVSchema(final FieldType[] fieldTypes) {
		this.fieldTypes = fieldTypes;
		this.fieldOffsets = computeFieldOffsets(fieldTypes);
	}
	
	
	public int fieldOffset(final RowData rowData, final int field) {
		final int fieldCount = fieldTypes.length;
		if (field < 0 || field >= fieldCount) {
			throw new IllegalArgumentException("Field index out of bounds: " + field);
		}
		long coordinates = 0;
		for (int k = 0; k < field; k += 8) {
			int mapByte = rowData.getColumnMapByte(k);
			int remainingBits = fieldCount - k;
			if (remainingBits < 8) {
				mapByte &= 0xFF >>> (8 - remainingBits);
			}
			coordinates += fieldOffsets[k << 3][mapByte];
		}
		int offset = (int)coordinates + rowData.getRowStartData();
		int vlenCount = (int)(coordinates >>> 32);
		for (int index = 0; index < vlenCount; index++) {
			int vlen = rowData.getVLen(offset);
			offset += vlen;
		}
		return offset;
	}
	
	private long[][] computeFieldOffsets(final FieldType[] fieldTypes) {
		final int fieldCount = fieldTypes.length;
		long[][] fieldOffsets = new long[(fieldCount + 7) / 8][];
		for (int field = 0; field < fieldTypes.length; field++) {
			final FieldType fieldType = fieldTypes[field];
			final long contribution;
			if (fieldType.isFixedWidth()) {
				contribution = fieldType.getMaxWidth();
			} else {
				contribution = 1 << 32;
			}
			final int a = field / 8;
			final int b = 1 << (field % 8);
			if (b == 1) {
				fieldOffsets[a] = new long[256];
			}
			for (int i = 0; i < b; i++) {
					fieldOffsets[a][i + b] = fieldOffsets[a][i] + contribution;
			}
		}
		return fieldOffsets;
	}
}
