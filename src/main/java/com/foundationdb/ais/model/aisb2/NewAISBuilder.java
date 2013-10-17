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
    NewTableBuilder table(String table);

    /**
     * Starts creating a new table using the given schema
     * @param schema the new table's schema
     * @param table the new table's table name
     * @return the new table's builder
     */
    NewTableBuilder table(String schema, String table);

    NewTableBuilder table(TableName tableName);
    
    /**
     * Returns the NewTableBuilder for the table being built
     * @return
     */
    NewTableBuilder getTable();
    NewTableBuilder getTable(TableName table);
   
    /**
     * create a new sequence
     */
    NewAISBuilder sequence (String name);
    NewAISBuilder sequence (String name, long start, long increment, boolean isCycle);

    /**
     * create a new view 
     * @param view name of view
     * @return
     */
    NewViewBuilder view(String view);

    NewViewBuilder view(String schema, String view);

    NewViewBuilder view(TableName viewName);

    /**
     * create a new procedure 
     * @param procedure name of procedure
     * @return
     */
    NewRoutineBuilder procedure(String procedure);

    NewRoutineBuilder procedure(String schema, String procedure);

    NewRoutineBuilder procedure(TableName procedureName);

    /**
     * create a new SQL/J jar 
     * @param jarName name of jar file
     * @return
     */
    NewSQLJJarBuilder sqljJar(String jarName);

    NewSQLJJarBuilder sqljJar(String schema, String jarName);

    NewSQLJJarBuilder sqljJar(TableName name);
}
