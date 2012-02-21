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

package com.akiban.ais.io;

import java.util.Collection;

import com.akiban.ais.metamodel.MMGroup;
import com.akiban.ais.metamodel.MMType;
import com.akiban.ais.metamodel.MetaModel;
import com.akiban.ais.model.AkibanInformationSchema;
import com.akiban.ais.model.Column;
import com.akiban.ais.model.Group;
import com.akiban.ais.model.GroupTable;
import com.akiban.ais.model.Index;
import com.akiban.ais.model.IndexColumn;
import com.akiban.ais.model.Join;
import com.akiban.ais.model.JoinColumn;
import com.akiban.ais.model.Target;
import com.akiban.ais.model.Type;
import com.akiban.ais.model.UserTable;
import com.akiban.util.Command;

public class Writer
{
    public Writer(Target target) {
        this.target = target;
    }

    protected int getVersion() {
        return MetaModel.only().getModelVersion();
    }

    protected Collection<Type> getTypes(AkibanInformationSchema ais) {
        return ais.getTypes();
    }

    protected Collection<Group> getGroups(AkibanInformationSchema ais) {
        return ais.getGroups().values();
    }

    protected Collection<GroupTable> getGroupTables(AkibanInformationSchema ais) {
        return ais.getGroupTables().values();
    }

    protected Collection<UserTable> getUserTables(AkibanInformationSchema ais) {
        return ais.getUserTables().values();
    }

    protected Collection<Join> getJoins(AkibanInformationSchema ais) {
        return ais.getJoins().values();
    }
    
    private void saveVersion(int version) {
        target.writeVersion(version);
    }

    private void saveTypes(Collection<Type> types) {
        target.writeCount(types.size());
        for (Type type : types) {
            target.writeType(MMType.map(type));
        }
    }

    private void saveGroups(Collection<Group> groups) {
        target.writeCount(groups.size());
        for (Group group : groups) {
            target.writeGroup(MMGroup.map(group));
            nIndexes += group.getIndexes().size();
        }
    }

    private void saveTables(Collection<GroupTable> groupTables, Collection<UserTable> userTables) {
        target.writeCount(groupTables.size() + userTables.size());
        for (GroupTable groupTable : groupTables) {
            target.writeTable(groupTable.map());
            assert groupTable.getRoot() != null : groupTable;
            nColumns += groupTable.getColumns().size();
            nIndexes += groupTable.getIndexes().size();
        }
        for (UserTable userTable : userTables) {
            target.writeTable(userTable.map());
            nColumns += userTable.getColumnsIncludingInternal().size();
            nIndexes += userTable.getIndexesIncludingInternal().size();
        }
    }

    private void saveColumns(Collection<GroupTable> groupTables, Collection<UserTable> userTables) {
        target.writeCount(nColumns);
        for (GroupTable groupTable : groupTables) {
            for (Column column : groupTable.getColumns()) {
                target.writeColumn(column.map());
            }
        }
        for (UserTable userTable : userTables) {
            for (Column column : userTable.getColumnsIncludingInternal()) {
                target.writeColumn(column.map());
            }
        }
    }

    private void saveJoins(Collection<Join> joins)  {
        target.writeCount(joins.size());
        for (Join join : joins) {
            target.writeJoin(join.map());
            nJoinColumns += join.getJoinColumns().size();
        }
    }

    private void saveJoinColumns(Collection<Join> joins) {
        target.writeCount(nJoinColumns);
        for (Join join : joins) {
            for (JoinColumn joinColumn : join.getJoinColumns()) {
                target.writeJoinColumn(joinColumn.map());
            }
        }
    }

    private void saveIndexes(Collection<Group> groups, Collection<GroupTable> groupTables,
                             Collection<UserTable> userTables) {
        target.writeCount(nIndexes);
        for (Group group : groups) {
            for (Index index : group.getIndexes()) {
                target.writeIndex(index.map());
                nIndexColumns += index.getColumns().size();
            }
        }
        for (UserTable userTable : userTables) {
            for (Index index : userTable.getIndexesIncludingInternal()) {
                target.writeIndex(index.map());
                nIndexColumns += index.getColumns().size();
            }
        }
        for (GroupTable groupTable : groupTables) {
            for (Index index : groupTable.getIndexes()) {
                target.writeIndex(index.map());
                nIndexColumns += index.getColumns().size();
            }
        }
    }

    private void saveIndexColumns(Collection<Group> groups, Collection<GroupTable> groupTables,
                                  Collection<UserTable> userTables) {
        target.writeCount(nIndexColumns);
        for (Group group : groups) {
            for (Index index : group.getIndexes()) {
                for (IndexColumn indexColumn : index.getColumns()) {
                    target.writeIndexColumn(indexColumn.map());
                }
            }
        }
        for (UserTable userTable : userTables) {
            for (Index index : userTable.getIndexesIncludingInternal()) {
                for (IndexColumn indexColumn : index.getColumns()) {
                    target.writeIndexColumn(indexColumn.map());
                }
            }
        }
        for (GroupTable groupTable : groupTables) {
            for (Index index : groupTable.getIndexes()) {
                for (IndexColumn indexColumn : index.getColumns()) {
                    target.writeIndexColumn(indexColumn.map());
                }
            }
        }
    }

    public final void save(AkibanInformationSchema ais) {
        final int version = getVersion();
        final Collection<Type> types = getTypes(ais);
        final Collection<Group> groups = getGroups(ais);
        final Collection<GroupTable> groupTables = getGroupTables(ais);
        final Collection<UserTable> userTables = getUserTables(ais);
        final Collection<Join> joins = getJoins(ais);
        target.deleteAll();
        saveVersion(version);
        saveTypes(types);
        saveGroups(groups);
        saveTables(groupTables, userTables);
        saveColumns(groupTables, userTables);
        saveJoins(joins);
        saveJoinColumns(joins);
        saveIndexes(groups, groupTables, userTables);
        saveIndexColumns(groups, groupTables, userTables);
        target.close();
    }

    private final Target target;
    private int nColumns = 0;
    private int nJoinColumns = 0;
    private int nIndexes = 0;
    private int nIndexColumns = 0;
}
