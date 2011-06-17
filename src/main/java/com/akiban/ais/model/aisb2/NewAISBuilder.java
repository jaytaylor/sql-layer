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

public interface NewAISBuilder extends NewAISProvider {
    /**
     * Sets the default schema
     * @param schema the new default schema name; like SQL's {@code USING}.
     * @return {@code this}
     */
    NewAISBuilder defaultSchema(String schema);

    /**
     * Starts creating a new table using the default schema.
     * @param table the table's name
     * @return the new table's builder
     */
    NewUserTableBuilder userTable(String table);

    /**
     * Starts creating a new table using the given schema
     * @param schema the new table's schema
     * @param table the new table's table name
     * @return the new table's builder
     */
    NewUserTableBuilder userTable(String schema, String table);
}
