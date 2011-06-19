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

public interface NewAISGroupIndexBuilder extends NewAISProvider {

    /**
     * Invokes {@link #on(String, String, String)} with the default schema
     * @param table the table name
     * @param column  the schema name
     * @return this builder instance
     */
    NewAISGroupIndexBuilder on(String table, String column);

    /**
     * Builds the first column of a group index. You must must invoke this between invoking
     * {@link #groupIndex(String)} and one of the {@link #and(String, String, String)} overloads.
     * This method sets the group for the upcoming group index; all subsequent calls to
     * {@link #and(String, String, String)} must reference tables also in this group.
     * @param schema the table's schema
     * @param table the table's name
     * @param column the column name
     * @return this builder instance
     */
    NewAISGroupIndexBuilder on(String schema, String table, String column);

    /**
     * Invokes {@link #and(String, String, String)} with the default schema
     * @param table the table's name
     * @param column the column name
     * @return this builder instance
     */
    NewAISGroupIndexBuilder and(String table, String column);

    /**
     * Adds a column to a group index started by {@link #on(String, String, String)}.
     * @param schema the schema
     * @param table the table's name
     * @param column the column name
     * @return this builder instance
     */
    NewAISGroupIndexBuilder and(String schema, String table, String column);
}
