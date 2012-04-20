/**
 * END USER LICENSE AGREEMENT (“EULA”)
 *
 * READ THIS AGREEMENT CAREFULLY (date: 9/13/2011):
 * http://www.akiban.com/licensing/20110913
 *
 * BY INSTALLING OR USING ALL OR ANY PORTION OF THE SOFTWARE, YOU ARE ACCEPTING
 * ALL OF THE TERMS AND CONDITIONS OF THIS AGREEMENT. YOU AGREE THAT THIS
 * AGREEMENT IS ENFORCEABLE LIKE ANY WRITTEN AGREEMENT SIGNED BY YOU.
 *
 * IF YOU HAVE PAID A LICENSE FEE FOR USE OF THE SOFTWARE AND DO NOT AGREE TO
 * THESE TERMS, YOU MAY RETURN THE SOFTWARE FOR A FULL REFUND PROVIDED YOU (A) DO
 * NOT USE THE SOFTWARE AND (B) RETURN THE SOFTWARE WITHIN THIRTY (30) DAYS OF
 * YOUR INITIAL PURCHASE.
 *
 * IF YOU WISH TO USE THE SOFTWARE AS AN EMPLOYEE, CONTRACTOR, OR AGENT OF A
 * CORPORATION, PARTNERSHIP OR SIMILAR ENTITY, THEN YOU MUST BE AUTHORIZED TO SIGN
 * FOR AND BIND THE ENTITY IN ORDER TO ACCEPT THE TERMS OF THIS AGREEMENT. THE
 * LICENSES GRANTED UNDER THIS AGREEMENT ARE EXPRESSLY CONDITIONED UPON ACCEPTANCE
 * BY SUCH AUTHORIZED PERSONNEL.
 *
 * IF YOU HAVE ENTERED INTO A SEPARATE WRITTEN LICENSE AGREEMENT WITH AKIBAN FOR
 * USE OF THE SOFTWARE, THE TERMS AND CONDITIONS OF SUCH OTHER AGREEMENT SHALL
 * PREVAIL OVER ANY CONFLICTING TERMS OR CONDITIONS IN THIS AGREEMENT.
 */

package com.akiban.server.rowdata;

import com.akiban.ais.model.Column;
import com.akiban.ais.model.Type;
import com.akiban.server.AkServerUtil;
import com.akiban.server.encoding.EncoderFactory;
import com.akiban.server.encoding.Encoding;

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

    public static FieldDef pkLessTableCounter(RowDef rowDef)
    {
        FieldDef fieldDef = new FieldDef(null, null, null, -1, -1, null, null);
        fieldDef.rowDef = rowDef;
        return fieldDef;
    }

    public boolean isPKLessTableCounter()
    {
        return rowDef != null && column == null;
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
        if (o == this) {
            return true;
        }
        if (o == null) {
            return false;
        }
        if (!o.getClass().equals(FieldDef.class)) {
            return false;
        }
        FieldDef def = (FieldDef) o;
        return type.equals(type) && columnName.equals(def.columnName)
                && encoding == def.encoding && column.getPosition().equals(def.column.getPosition())
                && AkServerUtil.equals(typeParameter1, def.typeParameter1)
                && AkServerUtil.equals(typeParameter2, def.typeParameter2);
    }

    @Override
    public int hashCode() {
        return type.hashCode() ^ columnName.hashCode() ^ encoding.hashCode()
                ^ column.getPosition() ^ AkServerUtil.hashCode(typeParameter1)
                ^ AkServerUtil.hashCode(typeParameter2);
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
        this.encoding = EncoderFactory.valueOf(type.encoding(), type, column.getCharsetAndCollation().charset());
        this.maxStorageSize = maxStorageSize;
        this.prefixSize = prefixSize;
        this.typeParameter1 = typeParameter1;
        this.typeParameter2 = typeParameter2;
        this.column.setFieldDef(this);
    }
}
