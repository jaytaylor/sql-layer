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

package com.foundationdb.server.store;

import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import com.foundationdb.ais.AISCloner;
import com.foundationdb.ais.model.AkibanInformationSchema;
import com.foundationdb.ais.model.Index;
import com.foundationdb.ais.model.Routine;
import com.foundationdb.ais.model.Sequence;
import com.foundationdb.ais.model.SQLJJar;
import com.foundationdb.ais.model.Table;
import com.foundationdb.ais.model.TableName;
import com.foundationdb.ais.model.View;
import com.foundationdb.ais.util.ChangedTableDescription;
import com.foundationdb.qp.memoryadapter.MemoryTableFactory;
import com.foundationdb.server.error.NoSuchSchemaException;
import com.foundationdb.server.service.security.SecurityService;
import com.foundationdb.server.service.session.Session;
import com.foundationdb.server.store.TableChanges.ChangeSet;
import com.foundationdb.server.store.format.StorageFormatRegistry;
import com.foundationdb.server.types.common.types.TypesTranslator;
import com.foundationdb.server.types.service.TypesRegistry;
import com.persistit.Key;

public interface SchemaManager {
    /** Flags indicating behavior regarding contained objects in DROP calls **/
    static enum DropBehavior {
        /** Reject if there are contained objects **/
        RESTRICT,

        /** Allow and also drop contained objects **/
        CASCADE
    }

    interface OnlineChangeState {
        AkibanInformationSchema getAIS();
        Collection<ChangeSet> getChangeSets();
    }


    /**
     * <p>
     * Create a new table in the {@link TableName#INFORMATION_SCHEMA}
     * schema. This table will be be populated and accessed through the normal
     * {@link Store} methods.
     * </p>
     * <p>
     * As this table contains rows that will go to disk, a caller specified
     * version will also be stored to facilitate upgrades. If this table
     * already exists (i.e. created on a previous start-up), the version must
     * match or an exception will be thrown. Upgrade and/or conversion must be
     * handled by the caller.
     * </p>
     *
     * @param newTable New table to create.
     * @param version Version of the table being created.
     *
     * @return Name of the table that was created.
     */
    TableName registerStoredInformationSchemaTable(Table newTable, int version);

    /**
     * Create a new table in the {@link TableName#INFORMATION_SCHEMA}
     * schema. This table will be be populated on demand and accessed through
     * the given {@link MemoryTableFactory}.
     *
     * @param newTable New table to create.
     * @param factory Factory to service this table.
     *
     * @return Name of the table that was created.
     */
    TableName registerMemoryInformationSchemaTable(Table newTable, MemoryTableFactory factory);

    /**
     * Delete the definition of a table in the {@link TableName#INFORMATION_SCHEMA}
     * schema. The table must exist and be a memory table.
     *
     * @param tableName Table to delete.
     */
    void unRegisterMemoryInformationSchemaTable(TableName tableName);

    /** Mark the {@code hKey} has already been handled with respect to online DDL. */
    void addOnlineHandledHKey(Session session, int tableID, Key hKey);

    /**
     * Get an iterator that returns all hKeys added from {@link #addOnlineHandledHKey} in order.
     * Start from {@code hKey} if not null, otherwise the beginning.
     */
    Iterator<byte[]> getOnlineHandledHKeyIterator(Session session, int tableID, Key hKey);

    /** {@code true} if {@code tableID} is undergoing an online change in *another* session. */
    boolean isOnlineActive(Session session, int tableID);

    /** Get all AIS and ChangeSets for active online sessions. */
    Collection<OnlineChangeState> getOnlineChangeStates(Session session);

    /**
     * Mark {@code session} as performing online DDL so future SchemaManager calls do not not modify the primary schema.
     * Must be paired with {@link #finishOnline(Session)} or {@link #discardOnline(Session)}.
     */
    void startOnline(Session session);

    /** Mark the online session associated with {@code tableID} with an error. */
    void setOnlineDMLError(Session session, int tableID, String message);

    /** Return the error message for a concurrent DML violation or {@code null} if none has occurred. */
    String getOnlineDMLError(Session session);

    /** Get current AIS for an in-progress online DDL. */
    AkibanInformationSchema getOnlineAIS(Session session);

    /** Add change information for an in-progress online DDL. */
    void addOnlineChangeSet(Session session, ChangeSet changeSet);

    /** Get current changes for an in-progress online DDL. */
    Collection<ChangeSet> getOnlineChangeSets(Session session);

    /** Move definition changes since the last {@link #startOnline(Session)} call to the primary schema. */
    void finishOnline(Session session);

    /** Discard definition changes since the last {@link #startOnline(Session)} call. */
    void discardOnline(Session session);

    /**
     * Create a new table in the SchemaManager.
     * @param session Session to operate under
     * @param newTable New table to add
     * @return The name of the table that was created.
     */
    TableName createTableDefinition(Session session, Table newTable);

    /**
     * Rename an existing table.
     * @param session Session
     * @param currentName Current name of table
     * @param newName Desired name of table
     */
    void renameTable(Session session, TableName currentName, TableName newName);

    /** Add table or group indexes to existing table(s). Empty or null <code>indexes</code> not allowed. **/
    void createIndexes(Session session, Collection<? extends Index> indexesToCreate, boolean keepStorage);

    /**
     * Modifying the existing schema definitions by adding indexes. Both Table and Group indexes are
     * supported through this interface.
     * @param session Session to operate under.
     * @param indexes List of indexes to drop.
     */
    void dropIndexes(Session session, Collection<? extends Index> indexes);

    /**
     * Delete the definition of the table with the given name. Throws
     * NoSuchTableException if the table does not exist.
     * @param session The session to operate under.
     * @param schemaName The name of the schema the table is in.
     * @param tableName The name of the table.
     * @param dropBehavior How to handle child tables.
     */
    void dropTableDefinition(Session session, String schemaName, String tableName, DropBehavior dropBehavior);

    /**
     * Delete the definition for an entire schema.
     * @param session The session to operate under.
     * @param schemaName The name of the schema to drop.
     * @param sequencesToDrop
     * @param routinesToDrop
     * @param jarsToDrop
     * @throws NoSuchSchemaException if the schema does not exist
     */
    void dropSchema(Session session, String schemaName, Set<TableName> sequencesToDrop,
                    Set<TableName> routinesToDrop, Set<TableName> jarsToDrop);

    /** Drops all non-system schemas from the ais. **/
    void dropNonSystemSchemas(Session session);

    /** Change definitions of existing tables. */
    void alterTableDefinitions(Session session, Collection<ChangedTableDescription> alteredTables);

    /** ALTER the definition of the given Sequence */
    void alterSequence(Session session, TableName sequenceName, Sequence newDefinition);

    /**
     * Return the AIS for {@code session}, which will include any <i>non-online</i> changes for the current transaction.
     * Also see {@link #getOnlineAIS(Session)}.
     */
    AkibanInformationSchema getAis(Session session);

    /** Add the given view to the current AIS. */
    void createView(Session session, View view);

    /** Drop the given view from the current AIS. */
    void dropView(Session session, TableName viewName);
    
    /** Add the Sequence to the current AIS */
    void createSequence(Session session, Sequence sequence);
    
    /** Drop the given sequence from the current AIS. */
    void dropSequence(Session session, Sequence sequence);

    /** Add the Routine to the current AIS */
    void createRoutine(Session session, Routine routine, boolean replaceExisting);
    
    /** Drop the given routine from the current AIS. */
    void dropRoutine(Session session, TableName routineName);

    /** Add an SQL/J jar to the current AIS. */
    void createSQLJJar(Session session, SQLJJar sqljJar);
    
    /** Update an SQL/J jar in the current AIS. */
    void replaceSQLJJar(Session session, SQLJJar sqljJar);
    
    /** Drop the given SQL/J jar from the current AIS. */
    void dropSQLJJar(Session session, TableName jarName);

    /** Add the Routine to live AIS */
    void registerSystemRoutine(Routine routine);
    
    /** Drop a system routine from the live AIS. */
    void unRegisterSystemRoutine(TableName routineName);

    /** Add the SQL/J jar to live AIS */
    void registerSystemSQLJJar(SQLJJar sqljJar);
    
    /** Drop a system SQL/J jar from the live AIS. */
    void unRegisterSystemSQLJJar(TableName jarName);

    /** Get all known/allocated tree names */
    Set<String> getTreeNames(Session session);

    /** Get oldest AIS generation still in memory */
    long getOldestActiveAISGeneration();

    /** Get AIS generations still in memory */
    Set<Long> getActiveAISGenerations();

    /** Return {@code true} if {@code tableID} has changed *concurrent* to this session's transaction. */
    boolean hasTableChanged(Session session, int tableID);

    /** Link up to security service. */
    void setSecurityService(SecurityService securityService);

    /** The types registry. */
    TypesRegistry getTypesRegistry();

    /** The types translator. */
    TypesTranslator getTypesTranslator();

    /** The store-specific format registry. */
    StorageFormatRegistry getStorageFormatRegistry();

    /** An <code>AISCloner</code> for merging. */
    AISCloner getAISCloner();
}
