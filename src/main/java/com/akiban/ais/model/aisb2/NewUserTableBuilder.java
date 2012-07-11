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

package com.akiban.ais.model.aisb2;

public interface NewUserTableBuilder extends NewAISBuilder {
    /**
     * Joins this table to another one, using the default schema
     * @param table the table to join to
     * @return a builder that will create the new join
     */
    NewAkibanJoinBuilder joinTo(String table);

    /**
     * Joins this table to another one.
     * @param schema the schema of the table to join to
     * @param table the table name of the table to join to
     * @return a builder that will create the new join
     */
    NewAkibanJoinBuilder joinTo(String schema, String table);

    /**
     * Joins this table to another one.
     * @param schema the schema of the table to join to
     * @param table the table name of the table to join to
     * @param fkName the name of the Akiban FK, <em>without</em> the {@code __akiban} prefix.
     * @return a builder that will create the new join
     */
    NewAkibanJoinBuilder joinTo(String schema, String table, String fkName);

    /**
     * Adds a non-nullable Long column
     * @param name the column's name
     * @return this
     */
    NewUserTableBuilder colLong(String name);

    /**
     * Adds an optionally nullable Long column
     * @param name the column's name
     * @param nullable whether the column is nullable
     * @return this
     */
    NewUserTableBuilder colLong(String name, boolean nullable);

    /**
     * Adds a non-nullable auto-incrementing column
     * @param name the column's name
     * @return this
     */
    NewUserTableBuilder autoIncLong(String name, int initialValue);

    /**
     * Adds a non-nullable varchar with UTF-8 encoding
     * @param name the column's name
     * @param length the varchar's max length
     * @return this
     */
    NewUserTableBuilder colString(String name, int length);

    /**
     * Adds an optionally nullable varchar with UTF-8 encoding
     * @param name the column's name
     * @param length the varchar's max length
     * @param nullable whether the column is nullable
     * @return this
     */
    NewUserTableBuilder colString(String name, int length, boolean nullable);

    /**
     * Adds an optionally nullable varchar with a specified encoding
     * @param name the column's name
     * @param length the varchar's max length
     * @param nullable whether the column is nullable
     * @param charset the column's encoding
     * @return this
     */
    NewUserTableBuilder colString(String name, int length, boolean nullable, String charset);

    NewUserTableBuilder colDouble(String name);
    NewUserTableBuilder colDouble(String name, boolean nullable);
    
    NewUserTableBuilder colTimestamp(String name);
    NewUserTableBuilder colTimestamp(String name, boolean nullable);

    NewUserTableBuilder colBigInt(String name);
    NewUserTableBuilder colBigInt(String name, boolean nullable);

    NewUserTableBuilder colVarBinary(String name, int length);
    NewUserTableBuilder colVarBinary(String name, int length, boolean nullable);
    
    NewUserTableBuilder colText(String name);
    NewUserTableBuilder colText(String name, boolean nullable);

    /**
     * Adds a PK
     * @param columns the columns that are in the PK
     * @return this
     */
    NewUserTableBuilder pk(String... columns);

    /**
     * Adds a unique key
     * @param indexName the key's name
     * @param columns the columns in the key
     * @return this
     */
    NewUserTableBuilder uniqueKey(String indexName, String... columns);

    /**
     * Adds a non-unique key
     * @param indexName the key's name
     * @param columns the columns in the key
     * @return this
     */
    NewUserTableBuilder key(String indexName, String... columns);
}
