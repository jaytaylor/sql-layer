package com.akiban.cserver;

import com.akiban.ais.model.Column;
import com.akiban.ais.model.Type;
import com.akiban.cserver.encoding.EncoderFactory;
import com.akiban.cserver.encoding.Encoding;

public class FieldDef {
    private final Column column;

    private final Type type;

    private final String columnName;

    private final int maxStorageSize;

    private final int prefixSize;

    private final Encoding encoding;

    private RowDef rowDef;

    private Long typeParameter1;

    private Long typeParameter2;

    public FieldDef(String name, Type type) {
        this(name, type, (int) (type.maxSizeBytes().longValue()));
    }

    public FieldDef(String name, Type type, int maxStorageSize) {
        this(null, name, type, maxStorageSize, 0, null, null);
    }

    public FieldDef(String name,
                    Type type,
                    int maxStorageSize,
                    int prefixSize) {
        this(null, name, type, maxStorageSize, prefixSize, null, null);
    }

    public FieldDef(RowDef rowDef, Column column)
    {
        this(column,
             column.getName(),
             column.getType(),
             column.getMaxStorageSize().intValue(),
             column.getPrefixSize(),
             column.getTypeParameter1(),
             column.getTypeParameter2());
        this.rowDef = rowDef;
    }

    public Column column()
    {
        return column;
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
        // setFieldPosition was only done in RowDefCache, not in tests that construct FieldDefs directly.
        assert column != null : this;
        return column.getPosition();
    }

    @Override
    public String toString() {
        return columnName + "(" + type + "(" + getMaxStorageSize() + "))";
    }

    @Override
    public boolean equals(final Object o) {
        FieldDef def = (FieldDef) o;
        return type == def.type && columnName == def.columnName
                && encoding == def.encoding && column.getPosition() == def.column.getPosition()
                && CServerUtil.equals(typeParameter1, def.typeParameter1)
                && CServerUtil.equals(typeParameter2, def.typeParameter2);
    }

    @Override
    public int hashCode() {
        return type.hashCode() ^ columnName.hashCode() ^ encoding.hashCode()
                ^ column.getPosition() ^ CServerUtil.hashCode(typeParameter1)
                ^ CServerUtil.hashCode(typeParameter2);
    }

    private FieldDef(Column column,
                     String name,
                     Type type,
                     int maxStorageSize,
                     int prefixSize,
                     Long typeParameter1,
                     Long typeParameter2) {
        this.column = column;
        this.columnName = name;
        this.type = type;
        this.encoding = EncoderFactory.valueOf(type.encoding(), type);
        if (!encoding.validate(type)) {
            throw new IllegalArgumentException("Encoding " + encoding
                    + " not valid for type " + type);
        }
        this.maxStorageSize = maxStorageSize;
        this.prefixSize = prefixSize;
        this.typeParameter1 = typeParameter1;
        this.typeParameter2 = typeParameter2;
    }
}
