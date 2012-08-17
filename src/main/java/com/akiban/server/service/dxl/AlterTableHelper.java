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

package com.akiban.server.service.dxl;

import com.akiban.ais.AISCloner;
import com.akiban.ais.model.AkibanInformationSchema;
import com.akiban.ais.model.Column;
import com.akiban.ais.model.Columnar;
import com.akiban.ais.model.Group;
import com.akiban.ais.model.GroupIndex;
import com.akiban.ais.model.Index;
import com.akiban.ais.model.IndexColumn;
import com.akiban.ais.model.IndexName;
import com.akiban.ais.model.Table;
import com.akiban.ais.model.TableName;
import com.akiban.ais.model.UserTable;
import com.akiban.ais.protobuf.ProtobufWriter;
import com.akiban.ais.util.TableChange;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class AlterTableHelper {
    final List<TableChange> columnChanges;
    final List<TableChange> indexChanges;

    public AlterTableHelper(List<TableChange> columnChanges, List<TableChange> indexChanges) {
        checkChangeTypes(columnChanges);
        checkChangeTypes(indexChanges);
        this.columnChanges = columnChanges;
        this.indexChanges = indexChanges;
    }

    private static void checkChangeTypes(List<TableChange> changes) {
        for(TableChange change : changes) {
            switch(change.getChangeType()) {
                case ADD:
                case DROP:
                case MODIFY:
                break;
                default:
                    throw new IllegalStateException("Unknown change type: " + change);
            }
        }
    }

    public Integer findOldPosition(UserTable oldTable, Column newColumn) {
        String newName = newColumn.getName();
        for(TableChange change : columnChanges) {
            if(newName.equals(change.getNewName())) {
                switch(change.getChangeType()) {
                    case ADD:
                        return null;
                    case MODIFY:
                        Column oldColumn = oldTable.getColumn(change.getOldName());
                        assert oldColumn != null : newColumn;
                        return oldColumn.getPosition();
                    case DROP:
                        throw new IllegalStateException("Dropped new column? " + newName);
                }
            }
        }
        Column oldColumn = oldTable.getColumn(newName);
        if((oldColumn == null) && newColumn.isAkibanPKColumn()) {
            return null;
        }
        // Not in change list, must be an original column
        assert oldColumn != null : newColumn;
        return oldColumn.getPosition();
    }

    public void findAffectedOldIndexes(UserTable table, List<Index> toTruncate, List<Index> toDrop) {
        for(TableChange change : indexChanges) {
            switch(change.getChangeType()) {
                case MODIFY:
                    toTruncate.add(table.getIndex(change.getOldName()));
                break;
                case DROP:
                    toDrop.add(table.getIndex(change.getOldName()));
                break;
            }
        }
    }

    public List<Index> findAffectedNewIndexes(UserTable table) {
        List<Index> indexes = new ArrayList<Index>();
        for(TableChange change : indexChanges) {
            switch(change.getChangeType()) {
                case ADD:
                case MODIFY:
                    indexes.add(table.getIndex(change.getNewName()));
                break;
            }
        }
        return indexes;
    }

    public void recreateAffectedGroupIndexes(UserTable origTable, final UserTable newTable,
                                             List<Index> indexesToBuild, List<Index> indexesToDrop,
                                             Map<IndexName, List<Column>> affectedGroupIndexes) {
        AkibanInformationSchema tempAIS = AISCloner.clone(newTable.getAIS(), new ProtobufWriter.TableSelector() {
            @Override
            public boolean isSelected(Columnar columnar) {
                return columnar.isTable() && (newTable.getGroup() == ((Table) columnar).getGroup());
            }
        });

        Group origGroup = origTable.getGroup();
        Group tempGroup = tempAIS.getGroup(newTable.getGroup().getName());
        for(Map.Entry<IndexName, List<Column>> entry : affectedGroupIndexes.entrySet()) {
            GroupIndex origIndex = origGroup.getIndex(entry.getKey().getName());
            List<Column> columns = entry.getValue();
            if(columns.isEmpty()) {
                indexesToDrop.add(origIndex);
            } else {
                GroupIndex tempIndex = GroupIndex.create(tempAIS, tempGroup, origIndex);
                for(int i = 0; i < columns.size(); ++i) {
                    Column column = columns.get(i);
                    UserTable tempTable = tempAIS.getUserTable(column.getTable().getName());
                    Column tempColumn = tempTable.getColumn(column.getName());
                    IndexColumn.create(tempIndex,  tempColumn, i, true, null);
                }
                indexesToBuild.add(tempIndex);
            }
        }
    }
}
