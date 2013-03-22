
package com.akiban.ais.model;

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
