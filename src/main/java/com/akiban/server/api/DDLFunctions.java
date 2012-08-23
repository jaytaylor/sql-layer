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

package com.akiban.server.api;

import java.util.Collection;
import java.util.List;

import com.akiban.ais.model.AkibanInformationSchema;
import com.akiban.ais.model.Index;
import com.akiban.ais.model.Sequence;
import com.akiban.ais.model.Table;
import com.akiban.ais.model.TableName;
import com.akiban.ais.model.UserTable;
import com.akiban.ais.model.View;
import com.akiban.ais.util.TableChange;
import com.akiban.qp.operator.QueryContext;
import com.akiban.server.error.NoSuchTableException;
import com.akiban.server.error.NoSuchTableIdException;
import com.akiban.server.error.RowDefNotFoundException;
import com.akiban.server.rowdata.RowDef;
import com.akiban.server.service.dxl.IndexCheckSummary;
import com.akiban.server.service.session.Session;

import static com.akiban.ais.util.TableChangeValidator.ChangeLevel;

public interface DDLFunctions {
    /**
     * Create a new table.
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
     * <p>
     *     Alter an existing table's definition.
     * </p>
     * <p>
     *     Contract for contents of the <code>newDefinition</code> table:
     *     <ul>
     *         <li>All column changes must be in the <code>columnChanges</code> list</li>
     *         <li>All index changes must be in the <code>indexChanges</code> list</li>
     *         <li>The <code>parentJoin</code> must be the final desired state</li>
     *     </ul>
     *     Contract for contents of <code>newDefinition</code>'s AIS:
     *     <ul>
     *         <li>The current group must be entirely represented</li>
     *         <li>Existing <code>childJoin</code>s need not be accurate</li>
     *         <li>Any Group-level change will be automatically propagated (e.g. group splits, index updates, etc)</li>
     *     </ul>
     * </p>
     * @param tableName the table to alter
     * @param newDefinition the new definition of the table
     * @param columnChanges list of all column changes
     * @param indexChanges list of all index changes
     */
    ChangeLevel alterTable(Session session, TableName tableName, UserTable newDefinition,
                           List<TableChange> columnChanges, List<TableChange> indexChanges, QueryContext context);

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
     * Retrieves the "CREATE" DDLs for all Akiban tables, including tables in the <tt>information_schema</tt>
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
     * Create new indexes on existing table(s). Both Table and Group indexes are supported. Primary
     * keys can not be created through this interface. Specified index IDs will not be used as they
     * are recalculated later. Blocks until the actual index data has been created.
     *
     * @param indexesToAdd a list of indexes to add to the existing AIS
     */
    void createIndexes(Session session, Collection<? extends Index> indexesToAdd);

    /**
     * Drop indexes on an existing table.
     * @param tableName the table containing the indexes to drop
     * @param indexesToDrop list of indexes to drop
     */
    void dropTableIndexes(Session session, TableName tableName, Collection<String> indexesToDrop);

    /**
     * Drop indexes on an existing group.
     * @param indexesToDrop Indexes to drop
     */
    void dropGroupIndexes(Session session, String groupName, Collection<String> indexesToDrop);

    /**
     * Update statistics for the given table.
     * @param tableName the table whose statistics should be updated.
     * @param indexesToUpdate specific indexes to update. If <code>null</code>, all indexes are analyzed.
     */
    void updateTableStatistics(Session session, TableName tableName, Collection<String> indexesToUpdate);

    IndexCheckSummary checkAndFixIndexes(Session session, String schemaRegex, String tableRegex);

    /**
     * Create a view.
     * @param session the session to run the Create under
     * @param view - new view to add to the existing system
     */
    void createView(Session session, View view);

    /**
     * Drops a view if it exists.
     * @param viewName the name of the view to drop
     */
    void dropView(Session session, TableName viewName);
    
    /**
     * Create a sequence, an independent number generator.
     * @param sequence Sequence to create.
     */
    void createSequence(Session session, Sequence sequence);
    
    /**
     * Drop a user created sequence.
     * @param sequenceName Sequence to drop
     */
    void dropSequence(Session session, TableName sequenceName);
}
