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

package com.akiban.ais.metamodel.io;

import java.util.Collection;

import com.akiban.ais.metamodel.MMColumn;
import com.akiban.ais.metamodel.MMGroup;
import com.akiban.ais.metamodel.MMIndex;
import com.akiban.ais.metamodel.MMIndexColumn;
import com.akiban.ais.metamodel.MMJoin;
import com.akiban.ais.metamodel.MMJoinColumn;
import com.akiban.ais.metamodel.MMTable;
import com.akiban.ais.metamodel.MMType;
import com.akiban.ais.metamodel.MetaModel;
import com.akiban.ais.metamodel.Target;
import com.akiban.ais.model.AkibanInformationSchema;
import com.akiban.ais.model.Column;
import com.akiban.ais.model.Group;
import com.akiban.ais.model.GroupTable;
import com.akiban.ais.model.Index;
import com.akiban.ais.model.IndexColumn;
import com.akiban.ais.model.Join;
import com.akiban.ais.model.JoinColumn;
import com.akiban.ais.model.Type;
import com.akiban.ais.model.UserTable;

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
            target.writeTable(MMTable.map(groupTable));
            assert groupTable.getRoot() != null : groupTable;
            nColumns += groupTable.getColumns().size();
            nIndexes += groupTable.getIndexes().size();
        }
        for (UserTable userTable : userTables) {
            target.writeTable(MMTable.map(userTable));
            nColumns += userTable.getColumnsIncludingInternal().size();
            nIndexes += userTable.getIndexesIncludingInternal().size();
        }
    }

    private void saveColumns(Collection<GroupTable> groupTables, Collection<UserTable> userTables) {
        target.writeCount(nColumns);
        for (GroupTable groupTable : groupTables) {
            for (Column column : groupTable.getColumns()) {
                target.writeColumn(MMColumn.map(column));
            }
        }
        for (UserTable userTable : userTables) {
            for (Column column : userTable.getColumnsIncludingInternal()) {
                target.writeColumn(MMColumn.map(column));
            }
        }
    }

    private void saveJoins(Collection<Join> joins)  {
        target.writeCount(joins.size());
        for (Join join : joins) {
            target.writeJoin(MMJoin.map(join));
            nJoinColumns += join.getJoinColumns().size();
        }
    }

    private void saveJoinColumns(Collection<Join> joins) {
        target.writeCount(nJoinColumns);
        for (Join join : joins) {
            for (JoinColumn joinColumn : join.getJoinColumns()) {
                target.writeJoinColumn(MMJoinColumn.map(joinColumn));
            }
        }
    }

    private void saveIndexes(Collection<Group> groups, Collection<GroupTable> groupTables,
                             Collection<UserTable> userTables) {
        target.writeCount(nIndexes);
        for (Group group : groups) {
            for (Index index : group.getIndexes()) {
                target.writeIndex(MMIndex.map(index));
                nIndexColumns += index.getKeyColumns().size();
            }
        }
        for (UserTable userTable : userTables) {
            for (Index index : userTable.getIndexesIncludingInternal()) {
                target.writeIndex(MMIndex.map(index));
                nIndexColumns += index.getKeyColumns().size();
            }
        }
        for (GroupTable groupTable : groupTables) {
            for (Index index : groupTable.getIndexes()) {
                target.writeIndex(MMIndex.map(index));
                nIndexColumns += index.getKeyColumns().size();
            }
        }
    }

    private void saveIndexColumns(Collection<Group> groups, Collection<GroupTable> groupTables,
                                  Collection<UserTable> userTables) {
        target.writeCount(nIndexColumns);
        for (Group group : groups) {
            for (Index index : group.getIndexes()) {
                for (IndexColumn indexColumn : index.getKeyColumns()) {
                    target.writeIndexColumn(MMIndexColumn.map(indexColumn));
                }
            }
        }
        for (UserTable userTable : userTables) {
            for (Index index : userTable.getIndexesIncludingInternal()) {
                for (IndexColumn indexColumn : index.getKeyColumns()) {
                    target.writeIndexColumn(MMIndexColumn.map(indexColumn));
                }
            }
        }
        for (GroupTable groupTable : groupTables) {
            for (Index index : groupTable.getIndexes()) {
                for (IndexColumn indexColumn : index.getKeyColumns()) {
                    target.writeIndexColumn(MMIndexColumn.map(indexColumn));
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
