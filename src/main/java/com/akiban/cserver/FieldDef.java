package com.akiban.cserver;

import com.akiban.ais.model.Type;

public class FieldDef {

    private final Type type;

    private final String columnName;

    private final int maxStorageSize;

    private final int prefixSize;

    private final Encoding encoding;

    private int fieldIndex;

    private RowDef rowDef;

    public FieldDef(final String name, final Type type) {
        this(name, type, (int) (type.maxSizeBytes().longValue()));
    }

    public FieldDef(final String name, final Type type, final int maxStorageSize) {
        this(name, type, maxStorageSize, 0);
    }

    public FieldDef(final String name, final Type type,
            final int maxStorageSize, final int prefixSize) {
        this.columnName = name;
        this.type = type;
        this.encoding = Encoding.valueOf(type.encoding());
        if (!encoding.validate(type)) {
            throw new IllegalArgumentException("Encoding " + encoding
                    + " not valid for type " + type);
        }
        this.maxStorageSize = maxStorageSize;
        this.prefixSize = prefixSize;
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

    public int getMaxStorageSize() {
        return maxStorageSize;
    }

    public int getPrefixSize() {
        return prefixSize;
    }

    public boolean isFixedSize() {
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
        return columnName + "(" + type + "(" + getMaxStorageSize() + "))";
    }
}
