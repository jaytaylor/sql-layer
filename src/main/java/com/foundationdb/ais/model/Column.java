/**
 * Copyright (C) 2009-2013 FoundationDB, LLC
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

package com.foundationdb.ais.model;

import com.foundationdb.ais.model.validation.AISInvariants;
import com.foundationdb.server.collation.AkCollator;
import com.foundationdb.server.rowdata.FieldDef;
import com.foundationdb.server.types.TClass;
import com.foundationdb.server.types.TInstance;
import com.foundationdb.server.types.common.types.DecimalAttribute;
import com.foundationdb.server.types.common.types.StringAttribute;
import com.foundationdb.server.types.common.types.StringFactory;
import com.foundationdb.server.types.common.types.TBinary;
import com.foundationdb.server.types.common.types.TString;

import java.util.UUID;

public class Column implements ColumnContainer, Visitable
{
    public static Column create(Columnar table, String name, Integer position, TInstance type) {
        return create(table, name, position, type, null, null, null);
    }

    public static Column create(Columnar table, String name, Integer position, TInstance type, Long initialAutoIncValue)
    {
        return create(table, name, position, type, initialAutoIncValue,
                      null, null);
    }

    public static Column create(Columnar table, String name, Integer position, TInstance type, Long initialAutoIncValue,
                                Long maxStorageSize, Integer prefixSize)
    {
        table.checkMutability();
        AISInvariants.checkNullName(name, "column", "column name");
        AISInvariants.checkDuplicateColumnsInTable(table, name);
        AISInvariants.checkDuplicateColumnPositions(table, position);
        Column column = new Column(table, name, position, type, initialAutoIncValue,
                                   maxStorageSize, prefixSize);
        table.addColumn(column);
        return column;
    }

    /**
     * Create an independent copy of an existing Column.
     * @param columnar Destination Columnar.
     * @param column Column to copy.
     * @param position Position of the new column, or <code>null</code> to copy from the given column.
     * @return Copy of the Column.
     * */
    public static Column create(Columnar columnar, Column column, String name, Integer position) {
        Integer finalPosition = (position != null) ? position : column.position;
        String finalName = (name != null) ? name :  column.getName();
        Column out = create(columnar, finalName, finalPosition, column.type, column.initialAutoIncrementValue,
                            column.maxStorageSize, column.prefixSize);
        if(column.identityGenerator != null) {
            Sequence newGenerator = columnar.getAIS().getSequence(column.identityGenerator.getSequenceName());
            if(newGenerator != null) {
                out.setDefaultIdentity(column.defaultIdentity);
                out.setIdentityGenerator(newGenerator);
            }
        }
        out.setDefaultValue(column.getDefaultValue());
        out.setDefaultFunction(column.getDefaultFunction());
        out.setUuid(column.getUuid());
        return out;
    }

    @Override
    public Column getColumn() {
        return this;
    }

    @Override
    public String toString()
    {
        return columnar.getName().getTableName() + "." + columnName;
    }

    public void finishCreating() {
    }

    public void setAutoIncrement(Boolean autoIncrement)
    {
        this.initialAutoIncrementValue = autoIncrement ? 1L /* mysql default */ : null;
        columnar.markColumnsStale();
    }

    public final void setDefaultIdentity(Boolean defaultIdentity) {
        this.defaultIdentity = defaultIdentity;
        columnar.markColumnsStale();
    }

    public final void setIdentityGenerator(Sequence identityGenerator) {
        this.identityGenerator = identityGenerator;
        columnar.markColumnsStale();
    }

    public String getDescription()
    {
        StringBuilder buffer = new StringBuilder();
        buffer.append(columnar.getName().getSchemaName());
        buffer.append(".");
        buffer.append(columnar.getName().getTableName());
        buffer.append(".");
        buffer.append(getName());
        return buffer.toString();
    }

    public String getName()
    {
        return columnName;
    }

    public Integer getPosition()
    {
        return position;
    }

    public Columnar getColumnar()
    {
        return columnar;
    }

    public Table getTable()
    {
        return (Table)columnar;
    }

    /**
     * The initial auto-increment value used in the MySQL 
     * generated table identity columns. 
     */
    public Long getInitialAutoIncrementValue()
    {
        return initialAutoIncrementValue;
    }

    public void setInitialAutoIncrementValue(Long initialAutoIncrementValue)
    {
        this.initialAutoIncrementValue = initialAutoIncrementValue;
        columnar.markColumnsStale();
    }

    /**
     * <p>
     * This is a three state boolean:
     * <ul>
     *   <li><b>True</b>: column created with GENERATED BY DEFAULT AS IDENTITY</li>
     *   <li><b>False</b>: Column created with GENERATED ALWAYS AS IDENTITY</li>
     *   <li><b>null</b>: Not generated by identity column</li>
     * </ul>
     * </p>
     * <p>
     * <b>NOTE</b>: It is possible for the GetInitialAutoIncrement to be
     * not null and this to be null, as MySQL generated tables use
     *  auto-increment value, where as the SQL Layer uses identity generators.
     * </p>
     */
    public final Boolean getDefaultIdentity() {
        return defaultIdentity;
    }

    /**
     * The identity generator sequence for this column. 
     * Will be null if not a IDENTITY Column. 
     * The null/not-null state matches the getDefaultIdentity
     */
    public final Sequence getIdentityGenerator() {
        return identityGenerator;
     }

    public TInstance getType() {
        return type;
    }

    public void setType(TInstance type) {
        columnar.checkMutability();
        this.type = type;
        clearMaxAndPrefixSize();
        columnar.markColumnsStale();
    }

    public String getTypeName() {
        return type.typeClass().name().unqualifiedName();
    }

    public UUID getTypeBundleUUID() {
        return type.typeClass().name().bundleId().uuid();
    }

    public int getTypeVersion() {
        return type.typeClass().serializationVersion();
    }

    public String getTypeDescription()
    {
        return type.toStringConcise(true);
    }

    public Boolean getNullable()
    {
        return type.nullability();
    }

    public Long getTypeParameter1()
    {
        return getTypeParameter1(type);
    }

    public Long getTypeParameter2()
    {
        return getTypeParameter2(type);
    }

    public static Long getTypeParameter1(TInstance type)
    {
        if (type.hasAttributes(StringAttribute.class)) {
            return (long) type.attribute(StringAttribute.MAX_LENGTH);
        }
        else if (type.hasAttributes(TBinary.Attrs.class)) {
            return (long) type.attribute(TBinary.Attrs.LENGTH);
        }
        else if (type.hasAttributes(DecimalAttribute.class)) {
            return (long) type.attribute(DecimalAttribute.PRECISION);
        }
        else {
            return null;
        }
    }

    public static Long getTypeParameter2(TInstance type)
    {
        if (type.hasAttributes(DecimalAttribute.class)) {
            return (long) type.attribute(DecimalAttribute.SCALE);
        }
        else {
            return null;
        }
    }

    public boolean hasCharsetAndCollation() {
        return hasCharsetAndCollation(type);
    }

    public static boolean hasCharsetAndCollation(TInstance type) {
        return type.hasAttributes(StringAttribute.class);
    }

    public String getCharsetName() {
        if (hasCharsetAndCollation()) {
            return StringAttribute.charsetName(type);
        }
        else {
            return null;
        }
    }

    public int getCharsetId() {
        if (hasCharsetAndCollation()) {
            return type.attribute(StringAttribute.CHARSET);
        }
        else {
            return StringFactory.NULL_CHARSET_ID;
        }
    }

    public String getCollationName() {
        AkCollator collator = getCollator();
        if (collator != null) {
            return collator.getScheme();
        }
        else {
            return null;
        }
    }

    public int getCollationId() {
        if (hasCharsetAndCollation()) {
            return type.attribute(StringAttribute.COLLATION);
        }
        else {
            return StringFactory.NULL_COLLATION_ID;
        }
    }

    public AkCollator getCollator() {
        if (hasCharsetAndCollation()) {
            return TString.getCollator(type);
        }
        else {
            return null;
        }
    }

    public Long getMaxStorageSize() {
        return getMaxStorageSize(true);
    }

    public Long getMaxStorageSizeWithoutComputing() {
        return getMaxStorageSize(false);
    }

    private Long getMaxStorageSize(boolean compute) {
        if(maxStorageSize == null && compute) {
            synchronized(this) {
                if(maxStorageSize == null) {
                    maxStorageSize = computeStorageSize(false);
                }
            }
        }
        return maxStorageSize;
    }

    public long getAverageStorageSize()
    {
        return computeStorageSize(true);
    }

    public Integer getPrefixSize() {
        return getPrefixSize(true);
    }

    public Integer getPrefixSizeWithoutComputing() {
        return getPrefixSize(false);
    }

    private Integer getPrefixSize(boolean compute) {
        if((prefixSize == null) && compute) {
            synchronized(this) {
                if(prefixSize == null) {
                    prefixSize = computePrefixSize();
                }
            }
        }
        return prefixSize;
    }

    public void clearMaxAndPrefixSize() {
        maxStorageSize = null;
        prefixSize = null;
    }

    public boolean fixedSize() {
        TClass tclass = TInstance.tClass(type);
        return tclass.hasFixedSerializationSize(type);
    }

    // TODO -
    // This is the largest BLOB size that will fit in a message.  Increase or
    // remove this when we are no longer limited by the message size.
    // Note that the Type objects for the BLOB types carry their MySQL-defined
    // values so that the prefix size will be computed correctly.  The
    // cap is imposed by the constructor of a Column object.
    //
    public final static int MAX_STORAGE_SIZE_CAP = 1024 * 1024 - 1024;

    public long computeStorageSize(boolean average) {
        TClass tclass = TInstance.tClass(type);
        if (tclass.hasFixedSerializationSize(type)) {
            return tclass.fixedSerializationSize(type);
        }
        else {
            long maxBytes = tclass.variableSerializationSize(type, average);
            return Math.min(MAX_STORAGE_SIZE_CAP, maxBytes) + prefixSize(maxBytes);
        }
    }

    public int computePrefixSize() {
        TClass tclass = TInstance.tClass(type);
        if (tclass.hasFixedSerializationSize(type)) {
            return 0;
        }
        else {
            long maxBytes = tclass.variableSerializationSize(type, false);
            return prefixSize(maxBytes);
        }
    }

    private int prefixSize(final long byteCount)
    {
        return
            byteCount < 256 ? 1 :
            byteCount < 65536 ? 2 :
            byteCount < 16777216 ? 3 : 4;
    }

    public UUID getUuid() {
        return uuid;
    }

    public void setUuid(UUID uuid) {
        this.uuid = uuid;
    }

    public Boolean isInternalColumn()
    {
        // right now this just calls through, but this way the language is consistent between
        // getColumnsIncludingInternal() and this method
        return isAkibanPKColumn();
    }

    public Boolean isAkibanPKColumn()
    {
        return columnName.equals(ROW_ID_NAME);
    }

    public void setDefaultValue(String defaultValue) {
        this.defaultValue = defaultValue;
        columnar.markColumnsStale();
    }

    public String getDefaultValue() {
        return defaultValue;
    }

    public void setDefaultFunction(String defaultFunction) {
        this.defaultFunction = defaultFunction;
        columnar.markColumnsStale();
    }

    public String getDefaultFunction() {
        return defaultFunction;
    }

    public void setFieldDef(FieldDef fieldDef)
    {
        this.fieldDef = fieldDef;
    }

    public FieldDef getFieldDef()
    {
        return fieldDef;
    }

    private Column(Columnar columnar,
                   String columnName,
                   Integer position,
                   TInstance type,
                   Long initialAutoIncValue,
                   Long maxStorageSize,
                   Integer prefixSize)
    {
        this.columnar = columnar;
        this.columnName = columnName;
        this.position = position;
        this.type = type;
        this.initialAutoIncrementValue = initialAutoIncValue;
        this.maxStorageSize = maxStorageSize;
        this.prefixSize = prefixSize;
    }

    // Visitable

    /** {@link Visitor#visit(Column)} this instance. */
    @Override
    public void visit(Visitor visitor) {
        visitor.visit(this);
    }

    // State

    public static final String ROW_ID_NAME = "__row_id";

    public static boolean isInternalName(String columnName) {
        return ROW_ID_NAME.equals(columnName);
    }

    private final String columnName;
    private final Columnar columnar;
    private final Integer position;
    private TInstance type;
    private UUID uuid;
    private Long initialAutoIncrementValue;

    // TODO: Should be final, but the multi-part construction of a valid Column needs to be cleaned up
    private volatile Long maxStorageSize;
    private Integer prefixSize;

    private FieldDef fieldDef;
    private Boolean defaultIdentity;
    private Sequence identityGenerator;
    private String defaultValue;
    private String defaultFunction;
}
