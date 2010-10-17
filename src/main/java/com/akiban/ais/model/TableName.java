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

public class TableName implements Serializable, Comparable<TableName>
{
    private String schemaName;
    private String tableName;

    @SuppressWarnings("unused")
    private TableName()
    {
        // GWT requires empty constructor
    }

    public TableName(String schemaName, String tableName)
    {
        this.schemaName = schemaName;
        this.tableName = tableName;
    }

    public TableName(String schemaTable)
    {
        // this parses the schema & table from the MySQL version of this data
        // which is stored at ./<schema>/<table>, The . is a path operator which
        // may be different if the user specified different locations.

        int tablePos = schemaTable.indexOf('/');
        tableName = tableName.substring(tablePos + 1);
        int schemaPos = schemaTable.indexOf('/', tablePos - 1);
        int schemaLen = schemaTable.length() - schemaPos - tableName.length() - 1;
        schemaName = schemaTable.substring(schemaPos + 1, schemaLen - 1);
    }

    public static TableName create(String schemaName, String tableName)
    {
        return new TableName(schemaName, tableName);
    }

    public String getSchemaName()
    {
        return schemaName;
    }

    public String getTableName()
    {
        return tableName;
    }

    public String getDescription()
    {
        StringBuilder buffer = new StringBuilder();
        buffer.append(getSchemaName());
        buffer.append(".");
        buffer.append(getTableName());
        return buffer.toString();
    }

    @Override
    public String toString()
    {
        return getDescription();
    }

    @Override
    public boolean equals(Object obj)
    {
        if (! (obj instanceof TableName)) {
            return false;
        }
        TableName o = (TableName) obj;

        return getSchemaName().equals(o.getSchemaName()) &&
               getTableName().equals(o.getTableName());
    }

    @Override
    public int hashCode()
    {
        return getSchemaName().hashCode() +
               getTableName().hashCode();
    }

    @Override
    public int compareTo(TableName that)
    {
        int c = this.getSchemaName().compareTo(that.getSchemaName());
        if (c == 0) {
            c = this.getTableName().compareTo(that.getTableName());
        }
        return c;
    }


    /**
     * Writes this table's escaped name to the builder. If the table's schema matches (case sensitive) the given
     * unlessSchema that portion of the name will <em>not</em> be included in the returned String. Otherwise, it will
     * be. If unlessSchema is null, this will always print the fully qualified name.
     * @param builder the builder to write the escaped name to
     * @param unlessSchema if matches this TableName's schema, print only the table's escaped name (not fully qualified)
     * @return the builder you passed in
     */
    public StringBuilder escape(StringBuilder builder, String unlessSchema) {
        if (unlessSchema == null || (!schemaName.equals(unlessSchema))) {
            if (needsEscaping(schemaName)) {
                escape(schemaName, builder);
            }
            else {
                builder.append(schemaName);
            }
            builder.append('.');
        }

        if (needsEscaping(tableName)) {
            escape(tableName, builder);
        }
        else {
            builder.append(tableName);
        }

        return builder;
    }

    /**
     * Shorthand for passing <tt>null</tt> as the second argument in {@link #escape(StringBuilder,String)}.
     * @param builder builder to write to
     * @return the builder you pass in
     */
    public StringBuilder escape(StringBuilder builder) {
        return escape(builder, null);
    }

    /**
     * Prints this table's escaped, fully qualified name.
     * @return the escaped, fully qualified name
     */
    public String escaped() {
        return escaped(null);
    }

    /**
     * Returns this table's escaped name. If the table's schema matches (case sensitive) the given unlessSchema,
     * that portion of the name will <em>not</em> be included in the returned String. Otherwise, it will be. If
     * unlessSchema is null, this will always print the fully qualified name.
     * @param unlessSchema if matches this TableName's schema, print only the table's escaped name (not fully qualified)
     * @return the escaped name
     */
    public String escaped(String unlessSchema) {
        return escape(new StringBuilder(), unlessSchema).toString();
    }

    public static boolean needsEscaping(String name) {
        char[] chars = new char[ name.length() ];
        name.getChars(0, name.length(), chars, 0);
        for (char c : chars) {
            if (! (Character.isLetterOrDigit(c) || c == '_')) {
                return true;
            }
        }
        return false;
    }

    public static StringBuilder escape(String name, StringBuilder out) {
        out.append('`').append(name.replace("`", "``")).append('`');
        return out;
    }
}
