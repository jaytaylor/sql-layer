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

package com.akiban.server.api.ddl;

import com.akiban.ais.model.AkibanInformationSchema;
import com.akiban.ais.model.Index;
import com.akiban.ais.model.Sequence;
import com.akiban.ais.model.Table;
import com.akiban.ais.model.TableName;
import com.akiban.ais.model.UserTable;
import com.akiban.ais.model.View;
import com.akiban.server.api.AlterTableChange;
import com.akiban.server.api.DDLFunctions;
import com.akiban.server.error.NoSuchTableException;
import com.akiban.server.error.NoSuchTableIdException;
import com.akiban.server.error.RowDefNotFoundException;
import com.akiban.server.rowdata.RowDef;
import com.akiban.server.service.dxl.IndexCheckSummary;
import com.akiban.server.service.session.Session;

import java.util.Collection;
import java.util.List;

/**
 * Simple implementation that throws UnsupportedOperation for all methods.
 */
public class DDLFunctionsMockBase implements DDLFunctions {
    @Override
    public void createTable(Session session, UserTable table) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void renameTable(Session session, TableName currentName, TableName newName) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void dropTable(Session session, TableName tableName) {
        throw new UnsupportedOperationException();
    }
    
    @Override
    public void alterTable(Session session, TableName tableName, UserTable newDefinition,
                           List<AlterTableChange> columnChanges, List<AlterTableChange> indexChanges) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void dropSchema(Session session, String schemaName) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void dropGroup(Session session, String groupName) {
        throw new UnsupportedOperationException();
    }

    @Override
    public AkibanInformationSchema getAIS(Session session) {
        throw new UnsupportedOperationException();
    }

    @Override
    public TableName getTableName(Session session, int tableId) {
        throw new UnsupportedOperationException();
    }

    @Override
    public int getTableId(Session session, TableName tableName) throws NoSuchTableException {
        throw new UnsupportedOperationException();
    }

    @Override
    public Table getTable(Session session, int tableId) throws NoSuchTableIdException {
        throw new UnsupportedOperationException();
    }

    @Override
    public Table getTable(Session session, TableName tableName) throws NoSuchTableException {
        throw new UnsupportedOperationException();
    }

    @Override
    public UserTable getUserTable(Session session, TableName tableName) throws NoSuchTableException {
        throw new UnsupportedOperationException();
    }

    @Override
    public RowDef getRowDef(int tableId) throws RowDefNotFoundException {
        throw new UnsupportedOperationException();
    }

    @Override
    public List<String> getDDLs(Session session) {
        throw new UnsupportedOperationException();
    }

    @Override
    public int getGeneration() {
        throw new UnsupportedOperationException();
    }

    @Override
    public long getTimestamp() {
        throw new UnsupportedOperationException();
    }

    @Override
    public void createIndexes(Session session, Collection<? extends Index> indexesToAdd) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void dropTableIndexes(Session session, TableName tableName, Collection<String> indexesToDrop) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void dropGroupIndexes(Session session, String groupName, Collection<String> indexesToDrop) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void updateTableStatistics(Session session, TableName tableName, Collection<String> indexesToUpdate) {
        throw new UnsupportedOperationException();
    }

    @Override
    public IndexCheckSummary checkAndFixIndexes(Session session, String schemaRegex, String tableRegex) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void createView(Session session, View view) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void dropView(Session session, TableName viewName) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void createSequence(Session session, Sequence sequence) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void dropSequence(Session session, TableName sequenceName) {
        throw new UnsupportedOperationException();
    }
}
