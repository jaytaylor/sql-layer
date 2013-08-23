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

public interface NewUserTableBuilder extends NewAISBuilder {
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
     * Adds a non-nullable, sequence backed, auto-incrementing BY DEFAULT identity column
     * @param name the column's name
     * @param initialValue the START WITH value
     * @return this
     */
    NewUserTableBuilder autoIncLong(String name, int initialValue);

    /**
     * Adds a non-nullable, sequence backed, auto-incrementing identity column
     * @param name the column's name
     * @param initialValue the START WITH value
     * @param always ALWAYS if <code>true</code>, otherwise DEFAULT
     * @return this
     */
    NewUserTableBuilder autoIncLong(String name, int initialValue, boolean always);

    /**
     * Adds an optionally nullable boolean column
     * @param name the column's name
     * @param nullable whether the column is nullable
     * @return this
     */
    NewUserTableBuilder colBoolean(String name, boolean nullable);

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
    
    NewUserTableBuilder colDateTime (String name);
    NewUserTableBuilder colDateTime (String name, boolean nullable);

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
