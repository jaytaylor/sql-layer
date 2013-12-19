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
import com.foundationdb.server.collation.AkCollatorFactory;
import com.foundationdb.server.rowdata.FieldDef;
import com.foundationdb.server.types.TClass;
import com.foundationdb.server.types.TInstance;
import com.foundationdb.server.types.aksql.aktypes.AkBool;
import com.foundationdb.server.types.common.types.StringFactory;
import com.foundationdb.server.types.mcompat.mtypes.MBinary;
import com.foundationdb.server.types.mcompat.mtypes.MDatetimes;
import com.foundationdb.server.types.mcompat.mtypes.MApproximateNumber;
import com.foundationdb.server.types.mcompat.mtypes.MNumeric;
import com.foundationdb.server.types.mcompat.mtypes.MString;

import java.util.UUID;
import java.util.concurrent.atomic.AtomicReference;
import java.nio.charset.Charset;

import com.foundationdb.server.collation.AkCollator;

public class Column implements ColumnContainer, Visitable
{
    public static Column create(Columnar table, String name, Integer position, Type type, Boolean nullable,
                                Long typeParameter1, Long typeParameter2, Long initialAutoIncValue,
                                CharsetAndCollation charsetAndCollation)
    {
        return create(table, name, position, type, nullable, typeParameter1, typeParameter2, initialAutoIncValue,
                      charsetAndCollation, null, null);
    }

    public static Column create(Columnar table, String name, Integer position, Type type, Boolean nullable,
                                Long typeParameter1, Long typeParameter2, Long initialAutoIncValue,
                                CharsetAndCollation charsetAndCollation, Long maxStorageSize, Integer prefixSize)
    {
        table.checkMutability();
        AISInvariants.checkNullName(name, "column", "column name");
        AISInvariants.checkDuplicateColumnsInTable(table, name);
        AISInvariants.checkDuplicateColumnPositions(table, position);
        Column column = new Column(table, name, position, type, nullable, typeParameter1, typeParameter2,
                                   initialAutoIncValue, charsetAndCollation, maxStorageSize, prefixSize);
        table.addColumn(column);
        return column;
    }

    public static Column create(Columnar table, String name, Integer position, Type type) {
        return create(table, name, position, type, null, null, null, null, null);
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
        Column out = create(columnar, finalName, finalPosition, column.type, column.nullable, column.typeParameter1,
                            column.typeParameter2, column.initialAutoIncrementValue, column.charsetAndCollation,
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

    public TInstance tInstance() {
        return tInstance(false);
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
        fillInDefaultParams();
    }

    public void setNullable(Boolean nullable)
    {
        this.nullable = nullable;
        columnar.markColumnsStale();
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

    public void setTypeParameter1(Long typeParameter1)
    {
        if(typeParameter1 != null) {
            this.typeParameter1 = typeParameter1;
        }
        columnar.markColumnsStale();
    }

    public void setTypeParameter2(Long typeParameter2)
    {
        if (typeParameter2 != null) {
            this.typeParameter2 = typeParameter2;
        }
        columnar.markColumnsStale();
    }

    public void setCharsetAndCollation(CharsetAndCollation charsetAndCollation)
    {
        this.charsetAndCollation = charsetAndCollation;
        columnar.markColumnsStale();
    }

    public void setCharset(String charset)
    {
        if (charset != null) {
            this.charsetAndCollation = CharsetAndCollation.intern(charset, getCharsetAndCollation().collation());
        }
        columnar.markColumnsStale();
    }

    public void setCollation(String collation)
    {
        if (collation != null) {
            this.charsetAndCollation = CharsetAndCollation.intern(getCharsetAndCollation().charset(), collation);
        }
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

    public String getTypeDescription()
    {
        StringBuilder columnType = new StringBuilder();
        columnType.append(type.name());
        boolean unsigned = type.name().endsWith(" unsigned");
        if (unsigned)
            columnType.setLength(columnType.length() - 9);
        switch (type.nTypeParameters()) {
            case 0:
                break;
            case 1:
                String str1 = "(" + typeParameter1 + ")";
                columnType.append(str1);
                break;
            case 2:
                String str2 = "(" + typeParameter1 + ", " + typeParameter2 + ")";
                columnType.append(str2);
                break;
        }
        if (unsigned)
            columnType.append(" unsigned");
        return columnType.toString();
    }

    public String getName()
    {
        return columnName;
    }

    public Integer getPosition()
    {
        return position;
    }

    public Type getType()
    {
        return type;
    }

    public Boolean getNullable()
    {
        return nullable;
    }

    public Long getTypeParameter1()
    {
        return typeParameter1;
    }

    public Long getTypeParameter2()
    {
        return typeParameter2;
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
                    maxStorageSize = computeMaxStorageSize();
                }
            }
        }
        return maxStorageSize;
    }

    public long computeMaxStorageSize() {
        // TODO: types3, delegate to TClass#getMaxStorageSize(TIstance)
        long maxStorageSize;
        if (type.equals(Types.VARCHAR) || type.equals(Types.CHAR)) {
            long maxCharacters = paramCheck(typeParameter1);
            final long charWidthMultiplier = maxCharacterWidth();
            long maxBytes = maxCharacters * charWidthMultiplier;
            maxStorageSize = maxBytes + prefixSize(maxBytes);
        } else if (type.equals(Types.VARBINARY)) {
            long maxBytes = paramCheck(typeParameter1);
            maxStorageSize = maxBytes + prefixSize(maxBytes);
        } else if (type.equals(Types.ENUM)) {
        	maxStorageSize = paramCheck(typeParameter1) < 256 ? 1 : 2;
        } else if (type.equals(Types.SET)) {
        	long members = paramCheck(typeParameter1);
            maxStorageSize =
                members <= 8 ? 1 :
        	    members <= 16 ? 2 :
        	    members <= 24 ? 3 :
        	    members <= 32 ? 4 : 8;
        } else if (type.equals(Types.DECIMAL) || type.equals(Types.U_DECIMAL)) {
            final int TYPE_SIZE = 4;
            final int DIGIT_PER = 9;
            final int BYTE_DIGITS[] = { 0, 1, 1, 2, 2, 3, 3, 4, 4, 4 };

            final int precision = getTypeParameter1().intValue();
            final int scale = getTypeParameter2().intValue();

            final int intCount = precision - scale;
            final int intFull = intCount / DIGIT_PER;
            final int intPart = intCount % DIGIT_PER;
            final int fracFull = scale / DIGIT_PER;
            final int fracPart = scale % DIGIT_PER;

            return (long) (intFull + fracFull) * TYPE_SIZE + BYTE_DIGITS[intPart] + BYTE_DIGITS[fracPart];
        } else if (!type.fixedSize()) {
            long maxBytes = type.maxSizeBytes();
            maxStorageSize = Math.min(Types.MAX_STORAGE_SIZE_CAP, maxBytes) 
                + prefixSize(maxBytes);
        } else {
            maxStorageSize = type.maxSizeBytes();
        }
        return maxStorageSize;
    }

    /** Same, but assume that characters take a common number of
     * bytes, not the encoding's max.
     */
    public long getAverageStorageSize()
    {
        if (type.equals(Types.VARCHAR) || type.equals(Types.CHAR)) {
            long maxCharacters = paramCheck(typeParameter1);
            final long charWidthMultiplier = averageCharacterWidth();
            long maxBytes = maxCharacters * charWidthMultiplier;
            return maxBytes + prefixSize(maxBytes);
        }
        else {
            return getMaxStorageSize();
        }
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

    public int computePrefixSize() {
        int prefixSize;
        if (type.equals(Types.VARCHAR) || type.equals(Types.CHAR)) {
            final long maxCharacters = paramCheck(typeParameter1);
            final long charWidthMultiplier = maxCharacterWidth();
            final long maxBytes = maxCharacters * charWidthMultiplier;
            prefixSize = prefixSize(maxBytes);
        } else if (type.equals(Types.VARBINARY)) {
            prefixSize = prefixSize(paramCheck(typeParameter1));
        } else if (!type.fixedSize()) {
            prefixSize = prefixSize(type.maxSizeBytes());
        } else {
            prefixSize = 0;
        }
        return prefixSize;
    }

    public void clearMaxAndPrefixSize() {
        maxStorageSize = null;
        prefixSize = null;
    }

    public UUID getUuid() {
        return uuid;
    }

    public void setUuid(UUID uuid) {
        this.uuid = uuid;
    }

    public Boolean isAkibanPKColumn()
    {
        return columnName.equals(AKIBAN_PK_NAME);
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

    /**
     * Compute the maximum character width.  This is used to determine how many bytes
     * will be reserved to encode the length in bytes of a VARCHAR or other text field.
     * @return maximum size of a single codepoint in the column's character set.
     */
    private int maxCharacterWidth() {
        CharsetAndCollation charsetAndCollation = getCharsetAndCollation();
        if (charsetAndCollation != null) {
            try {
                Charset charset = Charset.forName(charsetAndCollation.charset());
                if ("UTF-8".equals(charset.name()))
                    return 4;       // RFC 3629 (limited to U+10FFFF).
                else
                    return (int)charset.newEncoder().maxBytesPerChar();
            }
            catch (IllegalArgumentException ex) {
            }
        }
        return 1;
    }

    private int averageCharacterWidth() {
        return 1;
    }

    public boolean hasCharsetAndCollation() {
        return type.usesCollator();
    }

    /**
     * @return This column's CharsetAndCollation if it has one, otherwise the owning Columnar's
     */
    public CharsetAndCollation getCharsetAndCollation()
    {
        return
            charsetAndCollation == null
            ? columnar.getCharsetAndCollation()
            : charsetAndCollation;
    }

    public AkCollator getCollator() {
        if (type.usesCollator()) {
            CharsetAndCollation cac = getCharsetAndCollation();
            if (cac != null) {
                return cac.getCollator();
            }
        }
        return null;
    }

    public void setInitialAutoIncrementValue(Long initialAutoIncrementValue)
    {
        this.initialAutoIncrementValue = initialAutoIncrementValue;
        columnar.markColumnsStale();
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
                   Type type,
                   Boolean nullable,
                   Long typeParameter1,
                   Long typeParameter2,
                   Long initialAutoIncValue,
                   CharsetAndCollation charsetAndCollation,
                   Long maxStorageSize,
                   Integer prefixSize)
    {
        this.columnar = columnar;
        this.columnName = columnName;
        this.position = position;
        this.type = type;
        this.nullable = nullable;
        this.typeParameter1 = typeParameter1;
        this.typeParameter2 = typeParameter2;
        this.initialAutoIncrementValue = initialAutoIncValue;
        this.charsetAndCollation = charsetAndCollation;
        this.maxStorageSize = maxStorageSize;
        this.prefixSize = prefixSize;
        fillInDefaultParams();
    }

    private void fillInDefaultParams() {
        Long[] defaults = Types.defaultParams().get(getType());
        if(defaults != null) {
            if(this.typeParameter1 == null) {
                this.typeParameter1 = defaults[0];
            }
            if(this.typeParameter2 == null) {
                this.typeParameter2 = defaults[1];
            }
        }
    }

    private long paramCheck(final Number param)
    {
        if (param == null || param.longValue() < 0) {
            throw new IllegalStateException(this + " needs a positive column width parameter");
        }
        return param.longValue();
    }

    private int prefixSize(final long byteCount)
    {
        return
            byteCount < 256 ? 1 :
            byteCount < 65536 ? 2 :
            byteCount < 16777216 ? 3 : 4;
    }

    private TInstance tInstance(boolean force) {
        final TInstance old = tInstanceRef.get();
        if (old != null && !force)
            return old;
        final TInstance tinst = generateTInstance(getCharsetAndCollation(), type, typeParameter1, typeParameter2, nullable);
        tInstanceRef.set(tinst); // TODO ignores race conditions, because they "shouldn't" happen but do. Don't know why
//        if (!tInstanceRef.compareAndSet(old, tinst))
//            assert false : "CAS failed; Column is not thread-safe, so mutating it from multiple threads is bad!";
        return tinst;
    }

    // Visitable

    /** {@link Visitor#visit(Column)} this instance. */
    @Override
    public void visit(Visitor visitor) {
        visitor.visit(this);
    }

    public static TInstance generateTInstance(CharsetAndCollation charsetAndCollation, Type type, Long typeParameter1,
                                               Long typeParameter2, boolean nullable) {
        final TInstance tinst;

        switch (Types.asEnum(type)) {
        case T_ENUM:
        case T_BIT:
        case T_GEOMETRY:
        case T_GEOMETRYCOLLECTION:
        case T_POINT:
        case T_POLYGON:
        case T_SET:
        case T_LINESTRING:
        case T_MULTILINESTRING:
        case T_MULTIPOLYGON:
        case T_MULTIPOINT:
            throw new UnsupportedOperationException("unsupported type: " + type);

        case T_BLOB:
            tinst = MBinary.BLOB.instance(nullable);
            break;
        case T_BIGINT:
            tinst = MNumeric.BIGINT.instance(nullable);
            break;
        case T_U_BIGINT:
            tinst = MNumeric.BIGINT_UNSIGNED.instance(nullable);
            break;
        case T_BINARY:
            tinst = MBinary.BINARY.instance(typeParameter1.intValue(), nullable);
            break;
        case T_CHAR:
            tinst = charString(charsetAndCollation, typeParameter1, MString.CHAR, nullable);
            break;
        case T_DATE:
            tinst = MDatetimes.DATE.instance(nullable);
            break;
        case T_DATETIME:
            tinst = MDatetimes.DATETIME.instance(nullable);
            break;
        case T_DECIMAL:
            tinst = MNumeric.DECIMAL.instance(typeParameter1.intValue(), typeParameter2.intValue(), nullable);
            break;
        case T_U_DECIMAL:
            tinst = MNumeric.DECIMAL_UNSIGNED.instance(typeParameter1.intValue(), typeParameter2.intValue(), nullable);
            break;
        case T_DOUBLE:
            tinst = MApproximateNumber.DOUBLE.instance(nullable);
            break;
        case T_U_DOUBLE:
            tinst = MApproximateNumber.DOUBLE_UNSIGNED.instance(nullable);
            break;
        case T_FLOAT:
            tinst = MApproximateNumber.FLOAT.instance(nullable);
            break;
        case T_U_FLOAT:
            tinst = MApproximateNumber.FLOAT_UNSIGNED.instance(nullable);
            break;
        case T_INT:
            tinst = MNumeric.INT.instance(nullable);
            break;
        case T_U_INT:
            tinst = MNumeric.INT_UNSIGNED.instance(nullable);
            break;
        case T_LONGBLOB:
            tinst = MBinary.LONGBLOB.instance(nullable);
            break;
        case T_LONGTEXT:
            tinst = textString(charsetAndCollation, MString.LONGTEXT, nullable);
            break;
        case T_MEDIUMBLOB:
            tinst = MBinary.MEDIUMBLOB.instance(nullable);
            break;
        case T_MEDIUMINT:
            tinst = MNumeric.MEDIUMINT.instance(nullable);
            break;
        case T_U_MEDIUMINT:
            tinst = MNumeric.MEDIUMINT_UNSIGNED.instance(nullable);
            break;
        case T_MEDIUMTEXT:
            tinst = textString(charsetAndCollation, MString.MEDIUMTEXT, nullable);
            break;
        case T_SMALLINT:
            tinst = MNumeric.SMALLINT.instance(nullable);
            break;
        case T_U_SMALLINT:
            tinst = MNumeric.SMALLINT_UNSIGNED.instance(nullable);
            break;
        case T_TEXT:
            tinst = textString(charsetAndCollation, MString.TEXT, nullable);
            break;
        case T_TIME:
            tinst = MDatetimes.TIME.instance(nullable);
            break;
        case T_TIMESTAMP:
            tinst = MDatetimes.TIMESTAMP.instance(nullable);
            break;
        case T_TINYBLOB:
            tinst = MBinary.TINYBLOB.instance(nullable);
            break;
        case T_TINYINT:
            tinst = MNumeric.TINYINT.instance(nullable);
            break;
        case T_U_TINYINT:
            tinst = MNumeric.TINYINT_UNSIGNED.instance(nullable);
            break;
        case T_TINYTEXT:
            tinst = textString(charsetAndCollation, MString.TINYTEXT, nullable);
            break;
        case T_VARBINARY:
            tinst = MBinary.VARBINARY.instance(typeParameter1.intValue(), nullable);
            break;
        case T_VARCHAR:
            tinst = charString(charsetAndCollation, typeParameter1, MString.VARCHAR, nullable);
            break;
        case T_YEAR:
            tinst = MDatetimes.YEAR.instance(nullable);
            break;
        case T_BOOLEAN:
            tinst = AkBool.INSTANCE.instance(nullable);
            break;
        default:
            throw new UnsupportedOperationException("unknown type: " + type);
        }

        assert tinst != null : type;
        return tinst;
    }

    private static TInstance charString(CharsetAndCollation charsetAndCollation, Long typeParameter1, TClass tClass,
                                        boolean nullable)
    {
        if (charsetAndCollation == null)
            return tClass.instance(typeParameter1.intValue(), nullable);
        StringFactory.Charset charset = StringFactory.Charset.of(charsetAndCollation.charset());
        String collationName = charsetAndCollation.collation();
        int collatorId = AkCollatorFactory.getAkCollator(collationName).getCollationId();
        return tClass.instance(typeParameter1.intValue(), charset.ordinal(), collatorId, nullable);
    }

    private static TInstance textString(CharsetAndCollation charsetAndCollation, TClass tClass, boolean nullable) {
        if (charsetAndCollation == null)
            return tClass.instance(nullable);
        StringFactory.Charset charset = StringFactory.Charset.of(charsetAndCollation.charset());
        return tClass.instance(0, charset.ordinal(), nullable); // TODO collation
    }

    // State

    public static final String AKIBAN_PK_NAME = "__akiban_pk";

    private final String columnName;
    private final Type type;
    private final Columnar columnar;
    private final Integer position;
    private UUID uuid;
    private Boolean nullable;
    private Long typeParameter1;
    private Long typeParameter2;
    private Long initialAutoIncrementValue;
    private CharsetAndCollation charsetAndCollation;
    private final AtomicReference<TInstance> tInstanceRef = new AtomicReference<>();

    // TODO: Should be final, but the multi-part construction of a valid Column needs to be cleaned up
    private Long maxStorageSize;
    private Integer prefixSize;

    private FieldDef fieldDef;
    private Boolean defaultIdentity;
    private Sequence identityGenerator;
    private String defaultValue;
    private String defaultFunction;
}
