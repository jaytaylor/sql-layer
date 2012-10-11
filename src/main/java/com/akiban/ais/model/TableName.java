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

public class TableName implements Comparable<TableName>
{
    public final static String INFORMATION_SCHEMA = "information_schema";
    public final static String SQLJ_SCHEMA = "sqlj";
    public final static String SYS_SCHEMA = "sys";

    private final String schemaName;
    private final String tableName;

    public TableName(String schemaName, String tableName)
    {
        this.schemaName = schemaName;
        this.tableName = tableName;
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

        return equals(o.getSchemaName(), o.getTableName());
    }
    
    public boolean equals(String schema, String table)
    {
        return getSchemaName().equals(schema) &&
                getTableName().equals(table);
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
    private StringBuilder escape(StringBuilder builder, String unlessSchema) {
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

    private static boolean needsEscaping(String name) {
        char[] chars = new char[ name.length() ];
        name.getChars(0, name.length(), chars, 0);
        for (char c : chars) {
            if (! (Character.isLetterOrDigit(c) || c == '_')) {
                return true;
            }
        }
        return false;
    }

    private static StringBuilder escape(String name, StringBuilder out) {
        out.append('`').append(name.replace("`", "``")).append('`');
        return out;
    }
}
