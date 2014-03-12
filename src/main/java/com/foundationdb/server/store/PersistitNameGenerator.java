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

import com.foundationdb.ais.model.AkibanInformationSchema;
import com.foundationdb.ais.model.DefaultNameGenerator;
import com.foundationdb.ais.model.FullTextIndex;
import com.foundationdb.ais.model.Group;
import com.foundationdb.ais.model.GroupIndex;
import com.foundationdb.ais.model.HasStorage;
import com.foundationdb.ais.model.Index;
import com.foundationdb.ais.model.Sequence;
import com.foundationdb.ais.model.Table;
import com.foundationdb.ais.model.TableIndex;
import com.foundationdb.ais.model.TableName;
import com.foundationdb.server.store.format.PersistitStorageDescription;

import java.util.HashSet;
import java.util.Set;
import java.util.TreeSet;

public class PersistitNameGenerator extends DefaultNameGenerator
{
    static final int MAX_TREE_NAME_LENGTH = 256;
    private static final String TREE_NAME_SEPARATOR = ".";

    protected final Set<String> treeNames;


    public PersistitNameGenerator(AkibanInformationSchema ais) {
        super();
        treeNames = new HashSet<>();
        mergeAIS(ais);
    }

    @Override
    public synchronized void mergeAIS(AkibanInformationSchema ais) {
        super.mergeAIS(ais);
        treeNames.addAll(collectTreeNames(ais));
    }

    @Override
    public synchronized Set<String> getStorageNames() {
        return new TreeSet<>(treeNames);
    }
    
    //
    // PersistitNameGenerator
    //

    public synchronized String generateIndexTreeName(Index index) {
        // schema.table.index
        final TableName tableName;
        switch(index.getIndexType()) {
            case TABLE:
                tableName = ((TableIndex)index).getTable().getName();
            break;
            case GROUP:
                Table root = ((GroupIndex)index).getGroup().getRoot();
                if(root == null) {
                    throw new IllegalArgumentException("Grouping incomplete (no root)");
                }
                tableName = root.getName();
            break;
            case FULL_TEXT:
                tableName = ((FullTextIndex)index).getIndexedTable().getName();
            break;
            default:
                throw new IllegalArgumentException("Unknown type: " + index.getIndexType());
        }
        return generateIndexTreeName(tableName.getSchemaName(), tableName.getTableName(), index.getIndexName().getName());
    }

    public synchronized String generateGroupTreeName(String schemaName, String groupName) {
        // schema.group_name
        String proposed = escapeForTreeName(schemaName) + TREE_NAME_SEPARATOR +
            escapeForTreeName(groupName);
        return makeUnique(treeNames, proposed, MAX_TREE_NAME_LENGTH);
    }

    public synchronized String generateSequenceTreeName(Sequence sequence) {
        TableName tableName = sequence.getSequenceName();
        return generateSequenceTreeName(tableName.getSchemaName(), tableName.getTableName());
    }

    public synchronized void generatedTreeName(String treeName) {
        if(!treeNames.add(treeName)) {
            throw new IllegalArgumentException("Tree name already present: " + treeName);
        }
    }

    public synchronized void removeTreeName(String treeName) {
        treeNames.remove(treeName);
    }

    //
    // Internal
    //

    protected synchronized String generateIndexTreeName(String schemaName, String tableName, String indexName) {
        String proposed = escapeForTreeName(schemaName) + TREE_NAME_SEPARATOR +
            escapeForTreeName(tableName) + TREE_NAME_SEPARATOR +
            escapeForTreeName(indexName);
        return makeUnique(treeNames, proposed, MAX_TREE_NAME_LENGTH);
    }

    protected synchronized String generateSequenceTreeName(String schemaName, String sequenceName) {
        String proposed = escapeForTreeName(schemaName) + TREE_NAME_SEPARATOR +
                escapeForTreeName(sequenceName);
        return makeUnique(treeNames, proposed, MAX_TREE_NAME_LENGTH);
    }


    //
    // Static
    //

    public static String getTreeName(HasStorage object) {
        if (object.getStorageDescription() instanceof PersistitStorageDescription) {
            return ((PersistitStorageDescription)object.getStorageDescription()) .getTreeName();
        }
        return null;
    }

    public static String escapeForTreeName(String name) {
        return name.replace(TREE_NAME_SEPARATOR, "\\" + TREE_NAME_SEPARATOR);
    }

    public static Set<String> collectTreeNames(AkibanInformationSchema ais) {
        Set<String> treeNames = new HashSet<>();
        String treeName;
        for(Group group : ais.getGroups().values()) {
            treeName = getTreeName(group);
            if (treeName != null) {
                treeNames.add(treeName);
            }
            for(Index index : group.getIndexes()) {
                treeName = getTreeName(index);
                if (treeName != null) {
                    treeNames.add(treeName);
                }
            }
        }
        for(Table table : ais.getTables().values()) {
            for(Index index : table.getIndexesIncludingInternal()) {
                treeName = getTreeName(index);
                if (treeName != null) {
                    treeNames.add(treeName);
                }
            }
        }
        for (Sequence sequence : ais.getSequences().values()){
            treeName = getTreeName(sequence);
            if (treeName != null) {
                treeNames.add(treeName);
            }
        }
        return treeNames;
    }
}
