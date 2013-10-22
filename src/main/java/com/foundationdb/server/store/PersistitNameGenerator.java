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
import com.foundationdb.ais.model.Group;
import com.foundationdb.ais.model.HasStorage;
import com.foundationdb.ais.model.Index;
import com.foundationdb.ais.model.Sequence;
import com.foundationdb.ais.model.Table;
import com.foundationdb.server.store.format.PersistitStorageDescription;

import java.util.HashSet;
import java.util.Set;

public class PersistitNameGenerator extends DefaultNameGenerator
{
    public PersistitNameGenerator(AkibanInformationSchema ais) {
        super();
        mergeAIS(ais);
    }

    @Override
    public void mergeAIS(AkibanInformationSchema ais) {
        super.mergeAIS(ais);
        treeNames.addAll(collectTreeNames(ais));
    }
    
    public static String getTreeName(HasStorage object) {
        if (object.getStorageDescription() instanceof PersistitStorageDescription) {
            return ((PersistitStorageDescription)object.getStorageDescription())
                .getTreeName();
        }
        return null;
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
