/**
 * Copyright (C) 2009-2013 Akiban Technologies, Inc.
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

package com.akiban.server.api.ddl;

import com.akiban.ais.model.AkibanInformationSchema;
import com.akiban.ais.model.Index;
import com.akiban.ais.model.Routine;
import com.akiban.ais.model.Sequence;
import com.akiban.ais.model.SQLJJar;
import com.akiban.ais.model.Table;
import com.akiban.ais.model.TableName;
import com.akiban.ais.model.UserTable;
import com.akiban.ais.model.View;
import com.akiban.ais.util.TableChange;
import com.akiban.qp.operator.QueryContext;
import com.akiban.server.api.DDLFunctions;
import com.akiban.server.error.NoSuchTableException;
import com.akiban.server.error.NoSuchTableIdException;
import com.akiban.server.error.RowDefNotFoundException;
import com.akiban.server.rowdata.RowDef;
import com.akiban.server.service.dxl.IndexCheckSummary;
import com.akiban.server.service.session.Session;

import java.util.Collection;
import java.util.List;

import static com.akiban.ais.util.TableChangeValidator.ChangeLevel;

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
    public ChangeLevel alterTable(Session session, TableName tableName, UserTable newDefinition,
                           List<TableChange> columnChanges, List<TableChange> indexChanges, QueryContext context) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void dropSchema(Session session, String schemaName) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void dropGroup(Session session, TableName groupName) {
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
    public RowDef getRowDef(Session session, int tableId) throws RowDefNotFoundException {
        throw new UnsupportedOperationException();
    }

    @Override
    public int getGenerationAsInt(Session session) {
        throw new UnsupportedOperationException();
    }

    @Override
    public long getGeneration(Session session) {
        throw new UnsupportedOperationException();
    }

    @Override
    public long getOldestActiveGeneration() {
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
    public void dropGroupIndexes(Session session, TableName groupName, Collection<String> indexesToDrop) {
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

    @Override
    public void createRoutine(Session session, Routine routine) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void dropRoutine(Session session, TableName procedureName) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void createSQLJJar(Session session, SQLJJar sqljJar) {
        throw new UnsupportedOperationException();
    }
    
    @Override
    public void replaceSQLJJar(Session session, SQLJJar sqljJar) {
        throw new UnsupportedOperationException();
    }
    
    @Override
    public void dropSQLJJar(Session session, TableName jarName) {
        throw new UnsupportedOperationException();
    }
}
