package com.akiban.cserver;

import com.akiban.ais.model.Type;
import com.akiban.cserver.encoding.EncoderFactory;
import com.akiban.cserver.encoding.Encoding;

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
        this.encoding = EncoderFactory.valueOf(type.encoding(), type);
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

    @Override
    public boolean equals(final Object o) {
        FieldDef def = (FieldDef) o;
        return type == def.type && columnName == def.columnName
                && encoding == def.encoding && fieldIndex == def.fieldIndex
                && CServerUtil.equals(typeParameter1, def.typeParameter1)
                && CServerUtil.equals(typeParameter2, def.typeParameter2);
    }

    @Override
    public int hashCode() {
        return type.hashCode() ^ columnName.hashCode() ^ encoding.hashCode()
                ^ fieldIndex ^ CServerUtil.hashCode(typeParameter1)
                ^ CServerUtil.hashCode(typeParameter2);
    }


}
