/* <GENERIC-HEADER - BEGIN>
 *
 * $(COMPANY) $(COPYRIGHT)
 *
 * Created on: Nov, 20, 2009
 * Created by: Thomas Hazel
 *
 * </GENERIC-HEADER - END> */

package com.akiban.ais.model;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

public class Column implements Serializable, ModelNames
{
    public static Column create(AkibaInformationSchema ais, Map<String, Object> map)
    {
        Column column = null;
        String schemaName = (String) map.get(column_schemaName);
        String tableName = (String) map.get(column_tableName);
        Table table = ais.getTable(schemaName, tableName);
        if (table != null) {
            String typename = (String) map.get(column_typename);
            String columnName = (String) map.get(column_columnName);
            Integer position = (Integer) map.get(column_position);
            Boolean nullable = (Boolean) map.get(column_nullable);
            Type type = ais.getType(typename);
            column = new Column(table, columnName, position, type);
            column.setNullable(nullable);
            Integer nParameters = type.nTypeParameters();
            
            if (nParameters >= 1) {
                Long param = (Long) map.get(column_typeParam1);
                if (param != null)
                    column.setTypeParameter1(param);

                if (nParameters >= 2) {
                    param = (Long) map.get(column_typeParam2);
                    if (param != null)
                        column.setTypeParameter2(param);
                }
            }

            column.setInitialAutoIncrementValue((Long) map.get(column_initialAutoIncrementValue));
            /* Not clear how to do this in a GWT-safe way
            if (map.get(column_maxStorageSize) != null &&
                column.getMaxStorageSize().longValue() != (Long) map.get(column_maxStorageSize)) {
                throw new InternalError(column.toString());
            }
            if (map.get(column_prefixSize) != null &&
                column.getPrefixSize().intValue() != (Integer) map.get(column_prefixSize)) {
                throw new InternalError(column.toString());
            }
            */
            column.setCharsetAndCollation(CharsetAndCollation.intern((String) map.get(column_charset),
                                                                     (String) map.get(column_collation)));
        }
        return column;
    }

    public static Column create(Table table, String columnName, Integer position, Type type)
    {
        return new Column(table, columnName, position, type);
    }

    /**
     * @deprecated Use create(Table, String, Integer, Type) instead.
     * @param table
     * @param columnName
     * @param position
     * @param nullable
     * @param type
     * @param autoIncrement
     * @return
     */
    public static Column create(Table table,
                                String columnName,
                                Integer position,
                                Boolean nullable,
                                Type type,
                                Boolean autoIncrement)
    {
        Column column = new Column(table, columnName, position, type);
        column.setNullable(nullable);
        column.setAutoIncrement(autoIncrement);
        return column;
    }

    /**
     * @deprecated Use create(Table, String, Integer, Type) instead. 
     * @param table
     * @param columnName
     * @param position
     * @param nullable
     * @param type
     * @param autoIncrement
     * @param typeParam1
     * @return
     */
    public static Column create(Table table,
                                String columnName,
                                Integer position,
                                Boolean nullable,
                                Type type,
                                Boolean autoIncrement,
                                Long typeParam1)
    {
        Column column = new Column(table, columnName, position, type);
        column.setNullable(nullable);
        column.setAutoIncrement(autoIncrement);
        column.setTypeParameter1(typeParam1);
        return column;
    }

    /**
     * @deprecated Use create(Table, String, Integer, Type) instead. 
     * @param table
     * @param columnName
     * @param position
     * @param nullable
     * @param type
     * @param autoIncrement
     * @param typeParam1
     * @param typeParam2
     * @return
     */
    public static Column create(Table table,
                                String columnName,
                                Integer position,
                                Boolean nullable,
                                Type type,
                                Boolean autoIncrement,
                                Long typeParam1,
                                Long typeParam2)
    {
        Column column = new Column(table, columnName, position, type);
        column.setNullable(nullable);
        column.setAutoIncrement(autoIncrement);
        column.setTypeParameter1(typeParam1);
        column.setTypeParameter2(typeParam2);
        return column;
    }

    public Map<String, Object> map()
    {
        Map<String, Object> map = new HashMap<String, Object>();
        String groupSchemaName = null;
        String groupTableName = null;
        String groupColumnName = null;
        if (groupColumn != null) {
            groupSchemaName = groupColumn.getTable().getName().getSchemaName();
            groupTableName = groupColumn.getTable().getName().getTableName();
            groupColumnName = groupColumn.getName();
        }
        map.put(column_schemaName, getTable().getName().getSchemaName());
        map.put(column_tableName, getTable().getName().getTableName());
        map.put(column_columnName, columnName);
        map.put(column_position, position);
        map.put(column_typename, type.name());
        map.put(column_typeParam1, type.nTypeParameters() >= 1 ? typeParameter1 : null);
        map.put(column_typeParam2, type.nTypeParameters() >= 2 ? typeParameter2 : null);
        map.put(column_nullable, nullable);
        map.put(column_maxStorageSize, getMaxStorageSize());
        map.put(column_prefixSize, getPrefixSize());
        map.put(column_initialAutoIncrementValue, initialAutoIncrementValue);
        map.put(column_groupSchemaName, groupSchemaName);
        map.put(column_groupTableName, groupTableName);
        map.put(column_groupColumnName, groupColumnName);
        map.put(column_charset, getCharsetAndCollation().charset());
        map.put(column_collation, getCharsetAndCollation().collation());
        return map;
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

    private Column()
    {
        // XXX: GWT requires empty constructor
    }

    public void setNullable(Boolean nullable)
    {
        this.nullable = nullable;
    }
    
    public void setAutoIncrement(Boolean autoIncrement)
    {
        this.initialAutoIncrementValue = autoIncrement ? 1L /* mysql default */ : null;
    }
    
    public void setTypeParameter1(Long typeParameter1)
    {
        this.typeParameter1 = typeParameter1;
    }

    public void setTypeParameter2(Long typeParameter2)
    {
        this.typeParameter2 = typeParameter2;
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

    public void setGroupColumn(Column column)
    {
        assert column == null || groupColumn == null : groupColumn;
        groupColumn = column;
    }

    public void setUserColumn(Column column)
    {
        assert userColumn == null
                : "this may happen because you have two tables with the same column name, but different schemas: "
                + userColumn;
        userColumn = column;
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
        switch (type.nTypeParameters()) {
            case 0:
                break;
            case 1:
                // XXX: GWT issue - was using String.format
                String str1 = "(" + typeParameter1 + ")";
                columnType.append(str1);
                break;
            case 2:
                // XXX: GWT issue - was using String.format
                String str2 = "(" + typeParameter1 + ", " + typeParameter2 + ")";
                columnType.append(str2);
                break;
        }
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

    public Column getGroupColumn()
    {
        return groupColumn;
    }

    public Column getUserColumn()
    {
        return userColumn;
    }

    public Table getTable()
    {
        return table;
    }

    public UserTable getUserTable()
    {
        return (UserTable) getTable();
    }

    public Long getInitialAutoIncrementValue()
    {
        return initialAutoIncrementValue;
    }

    public Long getMaxStorageSize()
    {
        long maxStorageSize;
        if (type.equals(Types.VARCHAR) || type.equals(Types.CHAR)) {
            long maxCharacters = paramCheck(typeParameter1);
            final long charWidthMultiplier = characterWidth();
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

            final int precision = typeParameter1.intValue();
            final int scale = typeParameter2.intValue();

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

    public Integer getPrefixSize()
    {
        int prefixSize;
        if (type.equals(Types.VARCHAR) || type.equals(Types.CHAR)) {
            final long maxCharacters = paramCheck(typeParameter1);
            final long charWidthMultiplier = characterWidth();
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

    /**
     * Compute the maximum character width.  This is used to determine how many bytes
     * will be reserved to encode the length in bytes of a VARCHAR or other text field.
     * TODO: We need to implement a character set table to embody knowledge of many
     * different character sets.  This is simply a stub to get us past the UTF8
     * problem.
     * @return
     */
    private int characterWidth() {
//
//  See bug 337
        if (charsetAndCollation != null && "utf8".equalsIgnoreCase(charsetAndCollation.charset())) {
            return 3;
        } else {
            return 1;
        }
    }
    
    public CharsetAndCollation getCharsetAndCollation()
    {
        return
            charsetAndCollation == null
            ? table.getCharsetAndCollation()
            : charsetAndCollation;
    }

    // Note: made public for AISBuilder -- peter.  TODO remove this comment.
    public void setInitialAutoIncrementValue(Long initialAutoIncrementValue)
    {
        this.initialAutoIncrementValue = initialAutoIncrementValue;
    }

    private Column(Table table,
                   String columnName,
                   Integer position,
                   Type type)
    {
        this.table = table;
        this.columnName = columnName;
        this.position = position;
        this.type = type;
        this.table.addColumn(this);
        
        if(type.equals(Types.DECIMAL) || type.equals(Types.U_DECIMAL))
        {
            setTypeParameter1(10L);
            setTypeParameter2(0L);
        }
    }

    private long paramCheck(final Number param)
    {
        if (param == null || param.longValue() < 1) {
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

    // State

    public static final String AKIBAN_PK_NAME = "__akiban_pk";

    private String columnName;
    private Type type;
    private Boolean nullable;
    private Table table;
    private Integer position;
    private Long typeParameter1;
    private Long typeParameter2;
    private Column groupColumn; // Non-null iff this is a user table column
    private Column userColumn; // Non-null iff this is a group table column
    private Long initialAutoIncrementValue;
    private CharsetAndCollation charsetAndCollation;
}
