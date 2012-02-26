/**
 * Copyright (C) 2011 Akiban Technologies Inc.
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License, version 3,
 * as published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see http://www.gnu.org/licenses.
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
    NewUserTableBuilder autoIncLong(String name);

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

    NewUserTableBuilder colBinary(String name, int length);
    NewUserTableBuilder colBinary(String name, int length, boolean nullable);

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
