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

package com.akiban.ais.model;

import com.akiban.ais.model.validation.AISInvariants;
import com.akiban.server.collation.AkCollatorFactory;
import com.akiban.server.rowdata.FieldDef;
import com.akiban.server.types3.TClass;
import com.akiban.server.types3.TInstance;
import com.akiban.server.types3.common.types.StringFactory;
import com.akiban.server.types3.mcompat.mtypes.MBinary;
import com.akiban.server.types3.mcompat.mtypes.MDatetimes;
import com.akiban.server.types3.mcompat.mtypes.MApproximateNumber;
import com.akiban.server.types3.mcompat.mtypes.MNumeric;
import com.akiban.server.types3.mcompat.mtypes.MString;

import java.util.concurrent.atomic.AtomicReference;

import com.akiban.server.collation.AkCollator;

public class Column implements ColumnContainer
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
        return out;
    }

    public TInstance tInstance() {
        return new TInstance(tInstance(false));
    }

    @Override
    public Column getColumn() {
        return this;
    }

    @Override
    public String toString()
    {
        return table.getName().getTableName() + "." + columnName;
        /*** Too verbose for my taste. Restore this if you really need it
        StringBuilder typeDescription = new StringBuilder();
        typeDescription.append(type.name());
        if (type.nTypeParameters() > 0) {
            typeDescription.append("(");
            typeDescription.append(typeParameter1);
            if (type.nTypeParameters() > 1) {
                typeDescription.append(", ");
                typeDescription.append(typeParameter2);
            }
            typeDescription.append(")");
        }
        String s;
        if (groupColumn == null && userColumn == null) {
            s = "Column(" + columnName + ": " + typeDescription + ")";
        } else if (groupColumn != null) {
            s = "Column(" + columnName + ": " + typeDescription + " -> "
                + groupColumn.getTable().getName().getTableName() + "."
                + groupColumn.getName() + ")";
        } else {
            s = "Column(" + columnName + ": " + typeDescription + " -> "
                + userColumn.getTable().getName().getTableName() + "."
                + userColumn.getName() + ")";
        }
        return s;
         ***/
    }

    public void finishCreating() {
        fillInDefaultParams();
    }

    public void setNullable(Boolean nullable)
    {
        this.nullable = nullable;
    }
    
    public void setAutoIncrement(Boolean autoIncrement)
    {
        this.initialAutoIncrementValue = autoIncrement ? 1L /* mysql default */ : null;
    }

    public final void setDefaultIdentity(Boolean defaultIdentity) {
        this.defaultIdentity = defaultIdentity;
    }

    public final void setIdentityGenerator(Sequence identityGenerator) {
        this.identityGenerator = identityGenerator;
    }

    public void setTypeParameter1(Long typeParameter1)
    {
        if(typeParameter1 != null) {
            this.typeParameter1 = typeParameter1;
        }
    }

    public void setTypeParameter2(Long typeParameter2)
    {
        if (typeParameter2 != null) {
            this.typeParameter2 = typeParameter2;
        }
    }

    public void setCharsetAndCollation(CharsetAndCollation charsetAndCollation)
    {
        this.charsetAndCollation = charsetAndCollation;
    }

    public void setCharset(String charset)
    {
        if (charset != null) {
            this.charsetAndCollation = CharsetAndCollation.intern(charset, getCharsetAndCollation().collation());
        }
    }

    public void setCollation(String collation)
    {
        if (collation != null) {
            this.charsetAndCollation = CharsetAndCollation.intern(getCharsetAndCollation().charset(), collation);
        }
    }

    public String getDescription()
    {
        StringBuilder buffer = new StringBuilder();
        buffer.append(table.getName().getSchemaName());
        buffer.append(".");
        buffer.append(table.getName().getTableName());
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
        return table;
    }

    public Table getTable()
    {
        return (Table) table;
    }

    public UserTable getUserTable()
    {
        return (UserTable) getTable();
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
     * This is a three state boolean: 
     * True: column created with GENERATED BY DEFAULT AS IDENTITY
     * False: Column created with GENERATED ALWAYS AS IDENTITY
     * Null: Not generated by identity column
     * 
     * NOTE: It is possible for the GetInitialAutoIncrement to be
     * not null and this to be null, as MySQL generated tables use
     *  auto-increment value, where as the Akiban SQL use the 
     * identity generators. 
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

    public Boolean isAkibanPKColumn()
    {
        return columnName.equals(AKIBAN_PK_NAME);
    }

    public void setDefaultValue(String defaultValue) {
        this.defaultValue = defaultValue;
    }

    public String getDefaultValue() {
        return defaultValue;
    }

    /**
     * Compute the maximum character width.  This is used to determine how many bytes
     * will be reserved to encode the length in bytes of a VARCHAR or other text field.
     * TODO: We need to implement a character set table to embody knowledge of many
     * different character sets.  This is simply a stub to get us past the UTF8
     * problem.
     * @return
     */
    private int maxCharacterWidth() {
        // See bug687205
        if (charsetAndCollation != null && "utf8".equalsIgnoreCase(charsetAndCollation.charset())) {
            return 3;
        } else {
            return 1;
        }
    }

    private int averageCharacterWidth() {
        return 1;
    }

    /**
     * @return This column's CharsetAndCollation if it has one, otherwise the owning Columnar's
     */
    public CharsetAndCollation getCharsetAndCollation()
    {
        return
            charsetAndCollation == null
            ? table.getCharsetAndCollation()
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

    // Note: made public for AISBuilder -- peter.  TODO remove this comment.
    public void setInitialAutoIncrementValue(Long initialAutoIncrementValue)
    {
        this.initialAutoIncrementValue = initialAutoIncrementValue;
    }

    public void setFieldDef(FieldDef fieldDef)
    {
        this.fieldDef = fieldDef;
    }

    public FieldDef getFieldDef()
    {
        return fieldDef;
    }

    private Column(Columnar table,
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
        this.table = table;
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
            return tClass.instance(nullable);
        StringFactory.Charset charset = StringFactory.Charset.of(charsetAndCollation.charset());
        String collationName = charsetAndCollation.collation();
        int collatorId = AkCollatorFactory.getAkCollator(collationName).getCollationId();
        return tClass.instance(typeParameter1.intValue(), charset.ordinal(), collatorId, nullable);
    }

    private static TInstance textString(CharsetAndCollation charsetAndCollation, TClass tClass, boolean nullable) {
        if (charsetAndCollation == null)
            return tClass.instance(nullable);
        StringFactory.Charset charset = StringFactory.Charset.of(charsetAndCollation.charset());
        return tClass.instance(charset.ordinal(), -1, nullable); // TODO collation
    }

    // State

    public static final String AKIBAN_PK_NAME = "__akiban_pk";

    private final String columnName;
    private final Type type;
    private final Columnar table;
    private final Integer position;
    private Boolean nullable;
    private Long typeParameter1;
    private Long typeParameter2;
    private Long initialAutoIncrementValue;
    private CharsetAndCollation charsetAndCollation;
    private final AtomicReference<TInstance> tInstanceRef = new AtomicReference<TInstance>();

    // TODO: Should be final, but the multi-part construction of a valid Column needs to be cleaned up
    private Long maxStorageSize;
    private Integer prefixSize;

    private FieldDef fieldDef;
    private Boolean defaultIdentity;
    private Sequence identityGenerator;
    private String defaultValue;
}
