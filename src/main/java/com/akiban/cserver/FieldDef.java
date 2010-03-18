package com.akiban.cserver;

public class FieldDef {

	private final FieldType type;

	private final String columnName;

	private final int maxWidth;
	
	private RowDef parent;

	public FieldDef(final String name, final FieldType type) {
		this.columnName = name;
		this.type = type;
		this.maxWidth = type.getMaxWidth();
	}

	public FieldDef(final String name, final FieldType type, final int maxWidth) {
		this.columnName = name;
		this.type = type;
		if (maxWidth >= type.getMinWidth() && maxWidth <= type.getMaxWidth()) {
			this.maxWidth = maxWidth;
		} else {
			throw new IllegalArgumentException("MaxWidth value " + maxWidth
					+ " out of bounds for type " + type);
		}
	}

	public String getName() {
		return columnName;
	}

	public FieldType getType() {
		return type;
	}

	public int getMaxWidth() {
		return maxWidth;
	}

	public int getMinWidth() {
		return type.getMinWidth();
	}

	public int getWidthOverhead() {
		if (isFixedWidth()) {
			return 0;
		} else {
			return CServerUtil.varwidth(maxWidth);
		}
	}

	public boolean isFixedWidth() {
		return type.isFixedWidth();
	}


	public void setParent(RowDef parent) {
		this.parent = parent;
	}

	public RowDef getParent() {
		return parent;
	}

	@Override
	public String toString() {
		return columnName + "(" + type + "(" + getMinWidth() + ","
				+ getMaxWidth() + "))";
	}
}
