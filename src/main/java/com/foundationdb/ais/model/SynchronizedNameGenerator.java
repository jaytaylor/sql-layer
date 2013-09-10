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

package com.foundationdb.ais.model;

import java.util.List;
import java.util.Set;

public class SynchronizedNameGenerator implements NameGenerator {
    private final Object LOCK = new Object();
    private final NameGenerator realNamer;


    public static SynchronizedNameGenerator wrap(NameGenerator wrapped) {
        return new SynchronizedNameGenerator(wrapped);
    }

    private SynchronizedNameGenerator(NameGenerator realNamer) {
        this.realNamer = realNamer;
    }


    @Override
    public TableName generateIdentitySequenceName(TableName table) {
        synchronized(LOCK) {
            return realNamer.generateIdentitySequenceName(table);
        }
    }

    @Override
    public String generateJoinName(TableName parentTable, TableName childTable, List<JoinColumn> joinIndex) {
        synchronized(LOCK) {
            return realNamer.generateJoinName(parentTable, childTable, joinIndex);
        }
    }

    @Override
    public String generateJoinName(TableName parentTable, TableName childTable, List<String> pkColNames, List<String> fkColNames) {
        synchronized(LOCK) {
            return realNamer.generateJoinName(parentTable, childTable, pkColNames, fkColNames);
        }
    }

    @Override
    public int generateTableID(TableName name) {
        synchronized(LOCK) {
            return realNamer.generateTableID(name);
        }
    }

    @Override
    public int generateIndexID(int rootTableID) {
        synchronized(LOCK) {
            return realNamer.generateIndexID(rootTableID);
        }
    }

    @Override
    public String generateGroupTreeName(String schemaName, String groupName) {
        synchronized(LOCK) {
            return realNamer.generateGroupTreeName(schemaName, groupName);
        }
    }

    @Override
    public String generateIndexTreeName(Index index) {
        synchronized(LOCK) {
            return realNamer.generateIndexTreeName(index);
        }
    }

    @Override
    public String generateSequenceTreeName(Sequence sequence) {
        synchronized(LOCK) {
            return realNamer.generateSequenceTreeName(sequence);
        }
    }

    @Override
    public void generatedTreeName(String treeName) {
        synchronized(LOCK) {
            realNamer.generatedTreeName(treeName);
        }
    }

    @Override
    public void mergeAIS(AkibanInformationSchema ais) {
        synchronized(LOCK) {
            realNamer.mergeAIS(ais);
        }
    }

    @Override
    public void removeTableID(int tableID) {
        synchronized(LOCK) {
            realNamer.removeTableID(tableID);
        }
    }

    @Override
    public void removeTreeName(String treeName) {
        synchronized(LOCK) {
            realNamer.removeTreeName(treeName);
        }
    }

    @Override
    public Set<String> getTreeNames() {
        synchronized(LOCK) {
            return realNamer.getTreeNames();
        }
    }
}
