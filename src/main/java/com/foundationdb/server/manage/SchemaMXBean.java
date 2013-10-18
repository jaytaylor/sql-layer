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

package com.foundationdb.server.manage;

import java.util.List;
import java.util.Map;

import com.foundationdb.ais.model.TableName;
import com.foundationdb.server.service.session.Session;

@SuppressWarnings("unused")
public interface SchemaMXBean {
    static final String SCHEMA_BEAN_NAME = "com.foundationdb:type=Schema";

    /**
     * Gets the schema's generation ID. Each revision of the schema has a unique
     * ID.
     * 
     * @return the current schema revision's ID
     */
    int getSchemaGeneration() throws Exception;

    void forceSchemaGenerationUpdate() throws Exception;

    /**
     * Gets the list of known tables, with each table's DDL.
     * 
     * @return a map of table names to DDL
     * @throws Exception
     *             if there was a problem in retrieving the information
     */
    Map<TableName, String> getTables() throws Exception;

    boolean isProtectedTable(int rowDefId);

    boolean isProtectedTable(String schema, String table);

    /**
     * Creates a new table.
     * 
     * @param schemaName
     *            the schema name
     * @param DDL
     *            the DDL, which must start with "create table". The table's
     *            name specified in this DDL may be fully or partially qualified
     *            (that is, it may or may not contain the schemaName).
     * @return the new table's ID
     * @throws Exception
     *             if there was a problem in creating the table
     */
    void createTable(final Session session, String schemaName, String DDL) throws Exception;

    /**
     * Drops a table by name.
     * 
     * @param schema
     *            the schema name
     * @param tableName
     *            the table's name
     * @return I have no idea what this returns
     * @throws Exception
     *             if the table wasn't found
     */
    void dropTable(final Session session, String schema, String tableName) throws Exception;

//    void dropAllTables(final Session session) throws Exception;

    /**
     * Drops a schema and all of its tables.
     * 
     * @param schemaName
     *            the schema name
     * @return I have no idea what this returns // TODO
     * @throws Exception
     *             if there was a problem in dropping the schema
     */
    void dropSchema(final Session session, String schemaName) throws Exception;

    /**
     * Gets the current schema's grouping description.
     * 
     * @return a String of the same format as a static grouping string
     * @throws Exception
     *             if there was a problem in getting the information
     */
    public String getGrouping() throws Exception;

    /**
     * Gets a list of DDL statements to be executed on the head to create the
     * group tables and user tables.
     * 
     * @return the list of DDLs
     * @throws Exception
     *             if there was a problem in getting the info
     */
    public List<String> getDDLs() throws Exception;

    /**
     * Change the stored DDL statement for a table that already exists. Does not change the tableid. 
     * 
     * @throws Exception
     */
    public void changeTableDDL(String schemaName, String tableName, String DDL) throws Exception;
}
