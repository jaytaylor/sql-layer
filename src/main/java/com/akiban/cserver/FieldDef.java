package com.akiban.cserver;

import com.akiban.ais.model.Type;

public class FieldDef {

	private final Type type;

	private final String columnName;

	private final int maxWidth;

	private final Encoding encoding;

	private int fieldIndex;

	private RowDef rowDef;

	public FieldDef(final String name, final Type type) {
		this.columnName = name;
		this.type = type;
		this.encoding = Encoding.valueOf(type.encoding());
		if (!encoding.validate(type)) {
			throw new IllegalArgumentException("Encoding " + encoding
					+ " not valid for type " + type);
		}
		this.maxWidth = type.maxSizeBytes().intValue();
	}

	public FieldDef(final String name, final Type type, final int maxWidth) {
		this.columnName = name;
		this.type = type;
		this.encoding = Encoding.valueOf(type.encoding());
		if (!encoding.validate(type)) {
			throw new IllegalArgumentException("Encoding " + encoding
					+ " not valid for type " + type);
		}
		if (maxWidth <= type.maxSizeBytes()) {
			this.maxWidth = maxWidth;
		} else {
			throw new IllegalArgumentException("MaxWidth value " + maxWidth
					+ " out of bounds for type " + type);
		}
	}

	public String getName() {
		return columnName;
	}

	public Type getType() {
		return type;
	}

	public Encoding getEncoding() {
		return encoding;
	}

	/**
	 * Maximum width of the encoded field in RowData. For VARCHAR and friends,
	 * this is the MySQL max width plus the size of the MySQL prefix.
	 * 
	 * @return
	 */
	public int getMaxRowDataWidth() {
		final int w = getMaxWidth();
		if (isFixedWidth()) {
			return w;
		} else {
			return CServerUtil.varwidth(w) + w;
		}
	}

	/**
	 * Our computation of MySQL's maximum storage width for a column. For
	 * VARCHAR fields, this could be 1x, 2x or 3x the
	 * 
	 * @return
	 */
	public int getMaxWidth() {
		return maxWidth;
	}

	public int getMinWidth() {
		return type.fixedSize() ? maxWidth : 0;
	}

	public int getWidthOverhead() {
		if (isFixedWidth()) {
			return 0;
		} else {
			return CServerUtil.varwidth(maxWidth);
		}
	}

	public boolean isFixedWidth() {
		return type.fixedSize();
	}

	public void setRowDef(RowDef parent) {
		this.rowDef = parent;
	}

	public RowDef getRowDef() {
		return rowDef;
	}

	public int getFieldIndex() {
		return fieldIndex;
	}

	public void setFieldIndex(int fieldIndex) {
		this.fieldIndex = fieldIndex;
	}

	@Override
	public String toString() {
		return columnName + "(" + type + "(" + getMinWidth() + ","
				+ getMaxWidth() + "))";
	}
}
