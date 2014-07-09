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

package com.foundationdb.ais.model.aisb2;

import com.foundationdb.ais.model.TableName;

public interface NewTableBuilder extends NewAISBuilder {
    /**
     * Joins this table to another one, using the default schema
     * @param table the table to join to
     * @return a builder that will create the new join
     */
    NewAkibanJoinBuilder joinTo(String table);

    /**
     * Joins this table to another one.
     * @param tableName the name of the table to join to
     * @return a builder that will create the new join
     */
    NewAkibanJoinBuilder joinTo(TableName tableName);

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
     * Adds a non-nullable Int column
     * @param name the column's name
     * @return this
     */
    NewTableBuilder colInt(String name);

    /**
     * Adds an optionally nullable Int column
     * @param name the column's name
     * @param nullable whether the column is nullable
     * @return this
     */
    NewTableBuilder colInt(String name, boolean nullable);

    /**
     * Adds a non-nullable, sequence backed, auto-incrementing BY DEFAULT identity column
     * @param name the column's name
     * @param initialValue the START WITH value
     * @return this
     */
    NewTableBuilder autoIncInt(String name, int initialValue);

    /**
     * Adds a non-nullable, sequence backed, auto-incrementing identity column
     * @param name the column's name
     * @param initialValue the START WITH value
     * @param always ALWAYS if <code>true</code>, otherwise DEFAULT
     * @return this
     */
    NewTableBuilder autoIncInt(String name, int initialValue, boolean always);

    /**
     * Adds an optionally nullable boolean column
     * @param name the column's name
     * @param nullable whether the column is nullable
     * @return this
     */
    NewTableBuilder colBoolean(String name, boolean nullable);

    /**
     * Adds a non-nullable varchar with UTF-8 encoding
     * @param name the column's name
     * @param length the varchar's max length
     * @return this
     */
    NewTableBuilder colString(String name, int length);

    /**
     * Adds an optionally nullable varchar with UTF-8 encoding
     * @param name the column's name
     * @param length the varchar's max length
     * @param nullable whether the column is nullable
     * @return this
     */
    NewTableBuilder colString(String name, int length, boolean nullable);

    /**
     * Adds an optionally nullable varchar with a specified encoding
     * @param name the column's name
     * @param length the varchar's max length
     * @param nullable whether the column is nullable
     * @param charset the column's encoding
     * @return this
     */
    NewTableBuilder colString(String name, int length, boolean nullable, String charset);

    NewTableBuilder colDouble(String name);
    NewTableBuilder colDouble(String name, boolean nullable);
    
    NewTableBuilder colBigInt(String name);
    NewTableBuilder colBigInt(String name, boolean nullable);

    NewTableBuilder colVarBinary(String name, int length);
    NewTableBuilder colVarBinary(String name, int length, boolean nullable);
    
    NewTableBuilder colText(String name);
    NewTableBuilder colText(String name, boolean nullable);

    /*
    NewTableBuilder colTimestamp(String name);
    NewTableBuilder colTimestamp(String name, boolean nullable);
    */

    NewTableBuilder colSystemTimestamp(String name);
    NewTableBuilder colSystemTimestamp(String name, boolean nullable);

    /**
     * Adds a PK
     * @param columns the columns that are in the PK
     * @return this
     */
    NewTableBuilder pk(String... columns);

    /**
     * Adds a unique key
     * @param indexName the key's name
     * @param columns the columns in the key
     * @return this
     */
    NewTableBuilder uniqueKey(String indexName, String... columns);

    NewTableBuilder uniqueConstraint(String constraintName, String indexName, String... columns);

    /**
     * Adds a non-unique key
     * @param indexName the key's name
     * @param columns the columns in the key
     * @return this
     */
    NewTableBuilder key(String indexName, String... columns);
}
