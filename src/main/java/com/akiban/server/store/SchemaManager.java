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
import com.akiban.server.service.session.Session;

/**
 * A SchemaManager implementation updates, maintains and supplied schema (DDL)
 * records. The fundamental structure is map from tableId (an integer that
 * uniquely identifies a version of a table definition) to a record that
 * contains the schema name, table name and canonical version of the create
 * table statement.
 *
 * To support schema evolution, every version of a table definition has a new,
 * unique tableId. Thus creating a new table definition under the same name as
 * an existing one will allocate a new, unique tableId and retain the old
 * definition. This way data stored under the previous definition can be
 * interpreted and transformed into a row of the new format.
 *
 * @author peter
 *
 */
public interface SchemaManager {

    /**
     * Create table definition. This method attempts to parse and
     * validate the supplied CREATE TABLE statement. If valid, this method
     * allocates a new tableId and stores a canonical form of the statement with
     * that tableId.
     *
     * If useOldId is false, creating a table with a pre-existing schema.name will
     * be rejected with an exception. If useOldId is true, the current definition
     * will be replaced with no compatibility checking.
     *
     * @param session Session to operation under
     * @param schemaName Default schema name to use if statement does not contain one
     * @param statement A valid DDL statement (e.g. create table t(...))
     * @throws Exception
     */
    TableName createTableDefinition(Session session, String schemaName, String statement) throws Exception;

    void alterTableAddIndexes(Session session, TableName tableName, Collection<Index> indexes) throws Exception;
    void alterTableDropIndexes(Session session, TableName tableName, Collection<String> indexNames) throws Exception;

    /**
     * Delete all table definitions associated with the specified schema and
     * table names.
     * 
     * @param schemaName
     * @param tableName
     * @throws Exception
     */
    void deleteTableDefinition(Session session, String schemaName, String tableName) throws Exception;

    /**
     * Get the most recently updated version of the table definition for a
     * specified schema and table name. If there is no such table, return null.
     * 
     * @param schemaName
     * @param tableName
     * @return
     * @throws Exception
     */
    TableDefinition getTableDefinition(Session session, String schemaName,
            String tableName) throws Exception;

    /**
     * Returns SortedMap sorted by table name of all the table definitions
     * within one schema.
     * 
     * @param schemaName
     * @return
     * @throws Exception
     */
    SortedMap<String, TableDefinition> getTableDefinitions(Session session,
            String schemaName) throws Exception;

    /**
     * Constructs (if necessary) and returns an AkibanInformationSchema object
     * derived from the current set of tables defined in all schemas. In
     * addition, the returned AIS contains table definitions for the
     * akiban_information_schema tables themselves.
     * 
     * @return
     * @throws Exception
     */
    AkibanInformationSchema getAis(Session session);

    /**
     * Generates a list containing DDL statements for every schema, user
     * table, and, optionally, group tables in the database.
     *
     * @param session
     * The Session to use;
     * @param withGroupTables
     * If <code>true</code> this method will define synthetic group
     * tables to accompany the user table actually defined in the
     * schema database.
     */
    List<String> schemaStrings(Session session, boolean withGroupTables)
            throws Exception;

    /**
     * Return the system-wide Timestamp (as known from Persistit) for the last
     * update successfully committed to the schema database.
     * 
     * @return The timestamp at which a change last occurred
     */
    long getUpdateTimestamp();

    /**
     * Update the current timestamp to a value newer than the most recent
     * update. This causes the MySQL head to require a refresh even though there
     * has been no actual schema change. This method is intended for testing and
     * internal diagnostics.
     */
    void forceNewTimestamp();

    /**
     * "Temporary" method that returns a unique integer value derived from the
     * update timestamp. This supports the current network protocol in which the
     * schemaGeneration is an int. The value returned by this method changes
     * with every schema change, but the values do not necessarily always
     * increase.
     * 
     * @return generation
     */
    int getSchemaGeneration();

}
