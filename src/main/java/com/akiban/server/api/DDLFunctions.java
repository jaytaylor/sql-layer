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

package com.akiban.server.api;

import java.util.Collection;
import java.util.List;

import com.akiban.ais.model.AkibanInformationSchema;
import com.akiban.ais.model.Index;
import com.akiban.ais.model.Table;
import com.akiban.ais.model.TableName;
import com.akiban.ais.model.UserTable;
import com.akiban.server.error.InvalidOperationException;
import com.akiban.server.error.NoSuchTableException;
import com.akiban.server.error.NoSuchTableIdException;
import com.akiban.server.error.ParseException;
import com.akiban.server.error.RowDefNotFoundException;
import com.akiban.server.rowdata.RowDef;
import com.akiban.server.service.session.Session;

public interface DDLFunctions {
    /**
     * Creates a table in a given schema with the given ddl.
     * @param session the Session to run under.
     * @param schema may be null; if it is, and the schema must be provided in the DDL text
     * @param ddlText the DDL text: <tt>CREATE TABLE....</tt>
     * @throws ParseException if the given schema is <tt>null</tt> and no schema is provided in the DDL;
     *  or if there is some other parse error
     * exists
     */
    void createTable(Session session, String schema, String ddlText);
    /**
     * 
     * @param session the session to run the Create under
     * @param table - new user table to add to the existing system
     */
    void createTable (Session session, UserTable table);

    /**
     * Rename an existing table.
     * @param session Session
     * @param currentName Current name of the table
     * @param newName Desired name of the table
     */
    void renameTable(Session session, TableName currentName, TableName newName);

    /**
     * Drops a table if it exists.
     * @param tableName the table to drop
     * @throws NullPointerException if tableName is null
     */
    void dropTable(Session session, TableName tableName);
    /**
     * Drops a table if it exists, and possibly its children.
     * @param schemaName the schema to drop
     * @throws NullPointerException if schemaName is null
     */
    void dropSchema(Session session, String schemaName);

     /**
     * Drops all tables associated with the group
     * @param groupName the group to drop
     * @throws NullPointerException if groupName is null
     */
    void dropGroup(Session session, String groupName);
    /**
     * Gets the AIS from the Store.
     * @return returns the store's AIS.
     */
    AkibanInformationSchema getAIS(Session session);

    /**
     * Resolves the given table ID to its table's name.
     * @param session the session
     * @param tableId the table to look up
     * @return the table's name
     * @throws NullPointerException if the tableId is null
     */
    TableName getTableName(Session session, int tableId);

    /**
     * Resolves the given table name to its table's id.
     * @param session the session
     * @param tableName the table to look up
     * @return the table's id
     * @throws NullPointerException if the tableName is null
     * @throws NoSuchTableException if the tableName can not be found in the session list
     */
    int getTableId(Session session, TableName tableName) throws NoSuchTableException;

    /**
     * Resolves the given table to its Table
     * @param session the session
     * @param tableId the table to look up
     * @return the Table
     * @throws NoSuchTableIdException if the id can not be found in the session list
     */
    public Table getTable(Session session, int tableId) throws NoSuchTableIdException;

    /**
     * Resolves the given table to its Table
     * @param session the session
     * @param tableName the table to look up
     * @return the Table
     * @throws NoSuchTableException if the tableName can not be found in the session list
     */
    public Table getTable(Session session, TableName tableName) throws NoSuchTableException;
    /**
     * Resolves the given table to its UserTable
     * @param session the session
     * @param tableName the table to look up
     * @return the Table
     * @throws NoSuchTableException if the tableName can not be found in the session list
     */
    public UserTable getUserTable(Session session, TableName tableName) throws NoSuchTableException;

    /**
     * Resolves the given table ID to its RowDef
     * @param tableId the table to look up
     * @return the rowdef
     * @throws RowDefNotFoundException if the tableID has no associated RowDef. 
     */
    RowDef getRowDef(int tableId) throws RowDefNotFoundException;

    /**
     * Retrieves the "CREATE" DDLs for all Akiban tables, including tables in the <tt>akiban_information_schema</tt>
     * schema. The DDLs will be arranged such that it should be safe to call them in order, but they will not contain
     * any DROP commands; it is up to the caller to drop all conflicting tables. Schemas will be created with
     * <tt>IF NOT EXISTS</tt>, so the caller does not need to drop conflicting schemas.
     * @return the list of CREATE SCHEMA and CREATE TABLE statements that correspond to known tables
     */
    List<String> getDDLs(Session session);

    /**
     * Get the generation of the current AIS. This value is only guaranteed to change
     * when the AIS goes.
     * @see #getTimestamp
     */
    int getGeneration();

    /**
     * Get the timestamp of the current AIS. This value increases for each new AIS.
     */
    long getTimestamp();

    /**
     * Forces an increment to the chunkserver's AIS generation ID. This can be useful for debugging.
     * @throws InvalidOperationException if an exception occurred
     */
    void forceGenerationUpdate();
    
    /**
     * Create new indexes on existing table(s). Both Table and Group indexes are supported. Primary
     * keys can not be created through this interface. Specified index IDs will not be used as they
     * are recalculated later. Blocks until the actual index data has been created.
     *
     * @param indexesToAdd a list of indexes to add to the existing AIS
     * @throws InvalidOperationException
     */
    void createIndexes(Session session, Collection<? extends Index> indexesToAdd);

    /**
     * Drop indexes on an existing table.
     * @param tableName the table containing the indexes to drop
     * @param indexesToDrop list of indexes to drop
     * @throws InvalidOperationException
     */
    void dropTableIndexes(Session session, TableName tableName, Collection<String> indexesToDrop);

    /**
     * Drop indexes on an existing group.
     * @param indexesToDrop
     * @throws InvalidOperationException
     */
    void dropGroupIndexes(Session session, String groupName, Collection<String> indexesToDrop);

    /**
     * Update statistics for the given table.
     * @param tableName the table whose statistics should be updated.
     * @param indexesToUpdate specific indexes to update. If <code>null</code>, all indexes are analyzed.
     * @throws InvalidOperationException
     */
    void updateTableStatistics(Session session, TableName tableName, Collection<String> indexesToUpdate);
}
