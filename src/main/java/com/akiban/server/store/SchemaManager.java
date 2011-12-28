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

package com.akiban.server.store;

import java.util.Collection;
import java.util.List;
import java.util.SortedMap;

import com.akiban.ais.model.AkibanInformationSchema;
import com.akiban.ais.model.Index;
import com.akiban.ais.model.TableName;
import com.akiban.ais.model.UserTable;
import com.akiban.server.service.session.Session;

public interface SchemaManager {

    /**
     * Create a new table in the SchemaManager. This currently parses a given DDL statement,
     * validates it, and then adds the table to the existing AIS. Successful completion of
     * this method results in a new timestamp and schema generation, see
     * {@link #getUpdateTimestamp()} and {@link #getSchemaGeneration()} respectively.
     * @param session Session to operate under
     * @param defaultSchemaName Default schema name to use if statement does not contain one.
     * @param statement A valid DDL statement of the form 'create table t(...)'
     * @throws Exception If the statement is invalid, the table contains unsupported parts (e.g. data type), or
     * there is an internal error.
     * @return The name of the table that was created.
     * @throws Exception 
     */
    TableName createTableDefinition(Session session, String defaultSchemaName, String statement);
    
    TableName createTableDefinition(Session session, UserTable newTable);

    /**
     * Rename an existing table.
     * @param session Session
     * @param currentName Current name of table
     * @param newName Desired name of table
     * @throws Exception 
     * @throws Exception For any error
     */
    void renameTable(Session session, TableName currentName, TableName newName);

    /**
     * Modifying the existing schema definitions by adding indexes. Both Table and Group indexes are
     * supported through this interface. If indexes is empty, this method does nothing.
     *
     * @param session Session to operate under.
     * @param indexes List of index definitions to add.
     * @throws Exception If the request is invalid (e.g. duplicate index name, malformed Index) or there
     * was an internal error.
     * @return List of newly created indexes.
     * @throws Exception 
     */
    Collection<Index> createIndexes(Session session, Collection<? extends Index> indexes);

    /**
     * Modifying the existing schema definitions by adding indexes. Both Table and Group indexes are
     * supported through this interface.
     * @param session Session to operate under.
     * @param indexes List of indexes to drop.
     * @throws Exception 
     * @throws Exception If there was an internal error.
     */
    void dropIndexes(Session session, Collection<Index> indexes);

    /**
     * Delete the definition of the table with the given name. This method does nothing if
     * the table does not exist.
     * @param session The session to operate under.
     * @param schemaName The name of the schema the table is in.
     * @param tableName The name of the table.
     * @throws Exception 
     * @throws Exception If the definition cannot be deleted (e.g. table is referenced) or an internal error.
     */
    void deleteTableDefinition(Session session, String schemaName, String tableName);

    /**
     * Generate a TableDefinition, which includes a canonical 'create table' statement,
     * schema name, table name, and table ID, for the given table.
     * @param session Session to operate under.
     * @param tableName The name of the requested table.
     * @return Filled in TableDefinition.
     */
    TableDefinition getTableDefinition(Session session, TableName tableName);

    /**
     * Generate a 'create table' DDL statement for each table in the given schema.
     * @param session Session to operate under.
     * @param schemaName Schema to to query.
     * @return Map, keyed by table name, of all TableDefinitions.
     * @throws Exception 
     * @throws Exception For an internal error.
     */
    SortedMap<String, TableDefinition> getTableDefinitions(Session session, String schemaName);

    /**
     * Returns the current and authoritative AIS, containing all metadata about
     * all known for the running system.
     * @param session Session to operate under.
     * @return The current AIS.
     */
    AkibanInformationSchema getAis(Session session);

    /**
     * Generate DDL statements for every schema, user, and, optionally, group tables.
     * The format of the 'create schema' contains if not exists and will occur before
     * any table in that schema. No other garauntees are given about ordering.
     *
     * @param session The Session to operate under.
     * @param withGroupTables If true, include 'create table' statements for every GroupTable.
     * @return List of every create statement request.
     * @throws Exception For any internal error.
     */
    List<String> schemaStrings(Session session, boolean withGroupTables);

    /**
     * Return the last timestamp for the last successful change through the SchemaManager.
     * This value changes for any create, delete, or alter method executed.
     * @return The timestamp at which a change last occurred.
     */
    long getUpdateTimestamp();

    /**
     * Force a new timestamp as returned by {@link #getUpdateTimestamp()}. Primarily
     * intended for testing and diagnostics.
     */
    void forceNewTimestamp();

    /**
     * Get the value associated with the current state of the SchemaManager. This
     * changes under every condition that {@link #getUpdateTimestamp()} does but
     * implies no other ordering other than being distinct from the last.
     * @return The current schema generation value.
     */
    int getSchemaGeneration();


}
