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

package com.akiban.cserver.store;

import java.util.List;
import java.util.SortedMap;

import com.akiban.ais.model.AkibanInformationSchema;
import com.akiban.cserver.service.session.Session;
import com.persistit.exception.PersistitException;

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
     * Create or update a table definition. This method attempts to parse and
     * validate the supplied CREATE TABLE statement. If valid, this method
     * allocates a new tableId and stores a canonical form of the statement with
     * that tableId. Given a valid statement, this method succeeds even if there
     * already is a table of the same name defined for the specified schema. The
     * new statement is given a new, unique tableId but the old definition is
     * left intact to allow interpreting previously stored rows.
     * 
     * TODO: this method will verify that if this is an updated table
     * definition, that the update is feasible.
     * 
     * @param schemaName
     * @param tableName
     * @param statement
     * @param useOldId
     * @throws Exception
     */
    void createTableDefinition(Session session, String schemaName,
            String statement, boolean useOldId) throws Exception;
    
    /**
     * Delete the table definition for the specified tableId. This method does
     * not disturb other table definition versions having the same table and
     * schema names.
     * 
     * @param tableId
     * @throws Exception
     */
    void deleteTableDefinition(Session session, final int tableId)
            throws Exception;

    /**
     * Delete all table definitions associated with the specified schema and
     * table names.
     * 
     * @param schemaName
     * @param tableName
     * @throws Exception
     */
    void deleteTableDefinition(Session session, String schemaName,
            String tableName) throws Exception;

    /**
     * Delete all versions of all table definitions for the specified schema
     * name.
     * 
     * @param schemaName
     * @throws Exception
     */
    void deleteSchemaDefinition(Session session, String schemaName)
            throws Exception;

    /**
     * Delete all schema data.
     * 
     * @throws Exception
     */
    void deleteAllDefinitions(Session session) throws Exception;

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
     * Returns a List of all the table definitions for a table identified by
     * schema and table name. If there are none, returns an empty list.
     * 
     * @param schemaName
     * @param tableName
     * @return
     * @throws Exception
     */
    List<TableDefinition> getTableDefinitionHistory(Session session,
            String schemaName, String tableName) throws Exception;

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
     * Construct and return as a single String the entire set of create table
     * statements to be executed on the MySQL head in response to a
     * SchemaRequest. In addition, the returned string contains definitions of
     * the group tables implied by the table definitions.
     * 
     * @return
     * @throws Exception
     */
    String schemaString(Session session, boolean withGroupTables)
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

    void loadTableStatusRecords(final Session session) throws PersistitException;
    
    void removeStaleTableStatusRecords(final Session session) throws Exception;
    
    void saveTableStatusRecords(final Session session) throws PersistitException;
}
