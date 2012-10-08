/**
 * END USER LICENSE AGREEMENT (“EULA”)
 *
 * READ THIS AGREEMENT CAREFULLY (date: 9/13/2011):
 * http://www.akiban.com/licensing/20110913
 *
 * BY INSTALLING OR USING ALL OR ANY PORTION OF THE SOFTWARE, YOU ARE ACCEPTING
 * ALL OF THE TERMS AND CONDITIONS OF THIS AGREEMENT. YOU AGREE THAT THIS
 * AGREEMENT IS ENFORCEABLE LIKE ANY WRITTEN AGREEMENT SIGNED BY YOU.
 *
 * IF YOU HAVE PAID A LICENSE FEE FOR USE OF THE SOFTWARE AND DO NOT AGREE TO
 * THESE TERMS, YOU MAY RETURN THE SOFTWARE FOR A FULL REFUND PROVIDED YOU (A) DO
 * NOT USE THE SOFTWARE AND (B) RETURN THE SOFTWARE WITHIN THIRTY (30) DAYS OF
 * YOUR INITIAL PURCHASE.
 *
 * IF YOU WISH TO USE THE SOFTWARE AS AN EMPLOYEE, CONTRACTOR, OR AGENT OF A
 * CORPORATION, PARTNERSHIP OR SIMILAR ENTITY, THEN YOU MUST BE AUTHORIZED TO SIGN
 * FOR AND BIND THE ENTITY IN ORDER TO ACCEPT THE TERMS OF THIS AGREEMENT. THE
 * LICENSES GRANTED UNDER THIS AGREEMENT ARE EXPRESSLY CONDITIONED UPON ACCEPTANCE
 * BY SUCH AUTHORIZED PERSONNEL.
 *
 * IF YOU HAVE ENTERED INTO A SEPARATE WRITTEN LICENSE AGREEMENT WITH AKIBAN FOR
 * USE OF THE SOFTWARE, THE TERMS AND CONDITIONS OF SUCH OTHER AGREEMENT SHALL
 * PREVAIL OVER ANY CONFLICTING TERMS OR CONDITIONS IN THIS AGREEMENT.
 */

package com.akiban.server.store;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;

import com.akiban.ais.model.AkibanInformationSchema;
import com.akiban.ais.model.Index;
import com.akiban.ais.model.Sequence;
import com.akiban.ais.model.TableName;
import com.akiban.ais.model.UserTable;
import com.akiban.ais.model.View;
import com.akiban.ais.util.ChangedTableDescription;
import com.akiban.qp.memoryadapter.MemoryTableFactory;
import com.akiban.server.service.session.Session;

public interface SchemaManager {
    /** Flags indicating behavior regarding contained objects in DROP calls **/
    static enum DropBehavior {
        /** Reject if there are contained objects **/
        RESTRICT,

        /** Allow and also drop contained objects **/
        CASCADE
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
    TableName registerStoredInformationSchemaTable(UserTable newTable, int version);

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
    TableName registerMemoryInformationSchemaTable(UserTable newTable, MemoryTableFactory factory);

    /**
     * Delete the definition of a table in the {@link TableName#INFORMATION_SCHEMA}
     * schema. The table must exist and be a memory table.
     *
     * @param tableName Table to delete.
     */
    void unRegisterMemoryInformationSchemaTable(TableName tableName);

    /**
     * Create a new table in the SchemaManager. Successful completion of this
     * method results in a new timestamp and schema generation, see
     * {@link #getUpdateTimestamp()} and {@link #getSchemaGeneration()} respectively.
     * @param session Session to operate under
     * @param newTable New table to add
     * @return The name of the table that was created.
     */
    TableName createTableDefinition(Session session, UserTable newTable);

    /**
     * Rename an existing table.
     * @param session Session
     * @param currentName Current name of table
     * @param newName Desired name of table
     */
    void renameTable(Session session, TableName currentName, TableName newName);

    /**
     * Modifying the existing schema definitions by adding indexes. Both Table and Group indexes are
     * supported through this interface. If indexes is empty, this method does nothing.
     *
     * @param session Session to operate under.
     * @param indexes List of index definitions to add.
     * @return List of newly created indexes.
     */
    Collection<Index> createIndexes(Session session, Collection<? extends Index> indexes);

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
     * Change an existing table definition to be new value specified.
     * @param session Session to operate under.
     * @param alteredTables Description of tables being altered.
     */
    void alterTableDefinitions(Session session, Collection<ChangedTableDescription> alteredTables);

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
     * any table in that schema. No other guarantees are given about ordering.
     *
     * @param session The Session to operate under.
     * @param withGroupTables If true, include 'create table' statements for tables in the I_S.
     * @param withGroupTables If true, include 'create table' statements for every GroupTable.
     * @return List of every create statement request.
     */
    List<String> schemaStrings(Session session, boolean withISTables, boolean withGroupTables);

    /**
     * Return the last timestamp for the last successful change through the SchemaManager.
     * This value changes for any create, delete, or alter method executed.
     * @return The timestamp at which a change last occurred.
     */
    long getUpdateTimestamp();

    /**
     * Get the value associated with the current state of the SchemaManager. This
     * changes under every condition that {@link #getUpdateTimestamp()} does but
     * implies no other ordering other than being distinct from the last.
     * @return The current schema generation value.
     */
    int getSchemaGeneration();

    /** Add the given view to the current AIS. */
    void createView(Session session, View view);

    /** Drop the given view from the current AIS. */
    void dropView(Session session, TableName viewName);
    
    /** Add the Sequence to the current AIS */
    void createSequence(Session session, Sequence sequence);
    
    /** Drop the given sequence from the current AIS. */
    void dropSequence(Session session, Sequence sequence);

    // TODO: PSSM should handle this itself...
    void rollbackAIS(Session session, AkibanInformationSchema replacementAIS,
                     Map<TableName, Integer> savedOrdinals, Collection<String> schemaNames);
}
