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

    private Long typeParameter1;
    
    private Long typeParameter2;
    
    public FieldDef(final String name, final Type type) {
        this(name, type, (int) (type.maxSizeBytes().longValue()));
    }

    public FieldDef(final String name, final Type type, final int maxStorageSize) {
        this(name, type, maxStorageSize, 0, null, null);
    }
    
    public FieldDef(final String name, final Type type,
            final int maxStorageSize, final int prefixSize) {
        this(name, type, maxStorageSize, prefixSize, null, null);
    }
    
    public FieldDef(final String name, final Type type,
            final int maxStorageSize, final int prefixSize, 
            final Long typeParameter1, final Long typeParameter2) {
        this.columnName = name;
        this.type = type;
        this.encoding = Encoding.valueOf(type.encoding());
        if (!encoding.validate(type)) {
            throw new IllegalArgumentException("Encoding " + encoding
                    + " not valid for type " + type);
        }
        this.maxStorageSize = maxStorageSize;
        this.prefixSize = prefixSize;
        this.typeParameter1 = typeParameter1;
        this.typeParameter2 = typeParameter2;
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

    public Long getTypeParameter1() {
        return typeParameter1;
    }
    
    public Long getTypeParameter2() {
        return typeParameter2;
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
