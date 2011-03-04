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

package com.akiban.server.mttests.mthapi.base.sais;

import com.akiban.util.ArgumentValidation;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;

public final class SaisBuilder {
    public class FKBuilder {
        private final String parent;
        private final String child;
        private final Map<String,String> fks;

        private FKBuilder(String parent, String child) {
            ArgumentValidation.notNull("parent table", parent);
            ArgumentValidation.notNull("child table", child);
            confirmTable(parent);
            confirmTable(child);
            this.parent = parent;
            this.child = child;
            fks = new HashMap<String, String>();
        }

        public FKBuilder col(String parentColumn, String childColumn) {
            confirmColumn(parent, parentColumn);
            confirmColumn(child, childColumn);
            fks.put(parentColumn, childColumn);
            return this;
        }

        private void confirmTable(String table) {
            if (!tablesToFields.containsKey(table)) {
                throw new NoSuchElementException("unknown table: " + table);
            }
        }

        private void confirmColumn(String table, String column) {
            Set<String> columns = tablesToFields.get(table);
            if (!columns.contains(column)) {
                throw new NoSuchElementException("table " + table + " doesn't contain " + column);
            }
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            FKBuilder fkBuilder = (FKBuilder) o;

            return child.equals(fkBuilder.child) && parent.equals(fkBuilder.parent);
        }

        @Override
        public int hashCode() {
            int result = parent.hashCode();
            result = 31 * result + child.hashCode();
            return result;
        }

        @Override
        public String toString() {
            return String.format("FKS[%s to %s: %s]", parent, child, fks);
        }
    }

    public class QuickJoiner {
        private final String child;

        QuickJoiner(String child) {
            this.child = child;
        }

        public FKBuilder joinTo(String parent) {
            return join(parent, child);
        }
    }

    Map<String,Set<String>> tablesToFields = new HashMap<String, Set<String>>();
    Map<String,Set<FKBuilder>> fkBuilders = new HashMap<String,Set<FKBuilder>>();
    Set<String> roots = new HashSet<String>();

    public QuickJoiner table(String name, String... fields) {
        ArgumentValidation.notNull("table name", name);
        if(tablesToFields.containsKey(name)) {
            throw new IllegalArgumentException(name + " already defined");
        }
        List<String> fieldsList = Arrays.asList(fields);
        if (fieldsList.contains(null)) {
            throw new IllegalArgumentException("column names can't be null: " + fieldsList);
        }
        Set<String> fieldsSet = Collections.unmodifiableSet(new HashSet<String>(fieldsList));
        if (fieldsSet.size() != fieldsList.size()) {
            throw new IllegalArgumentException("some duplicated columns: " + fieldsList);
        }
        roots.add(name);
        tablesToFields.put(name, fieldsSet);

        return new QuickJoiner(name);
    }

    private FKBuilder join(String parent, String child) {
        if (parent.equals(child)) {
            throw new IllegalArgumentException("can't join table to itself: " + parent);
        }
        FKBuilder builder = new FKBuilder(parent, child);
        Set<FKBuilder> builders = fkBuilders.get(parent);
        if (builders == null) {
            builders = new HashSet<FKBuilder>();
            fkBuilders.put(parent, builders);
        }
        if (!builders.add(builder)) {
            throw new IllegalArgumentException("join already exists: " + fkBuilders);
        }
        roots.remove(child);
        return builder;
    }

    public Set<SaisTable> getRootTables() {
        Set<String> remainingTables = new HashSet<String>(tablesToFields.keySet());
        if (remainingTables.isEmpty()) {
            throw new IllegalStateException("no tables defined");
        }

        if (roots.isEmpty()) {
            throw new IllegalStateException("no root tables defined");
        }

        Set<SaisTable> ret = new HashSet<SaisTable>();
        for (String root : roots) {
            remainingTables.remove(root);
            SaisTable rootSais = recursivelyBuild(root, remainingTables);
            ret.add(rootSais);
        }

        return ret;
    }

    private SaisTable recursivelyBuild(String table, Set<String> remainingTables) {
        Set<FKBuilder> tableFKBuilders = fkBuilders.get(table);
        Set<String> fields = tablesToFields.get(table);
        if (tableFKBuilders == null) {
            return new SaisTable(table, fields, Collections.<SaisFK>emptySet());
        }

        Set<SaisFK> saisFKs = new HashSet<SaisFK>();
        for (FKBuilder fkBuilder : tableFKBuilders) {
            assert table.equals(fkBuilder.parent);
            String childName = fkBuilder.child;
            remainingTables.remove(childName);
            SaisTable child = recursivelyBuild(childName, remainingTables);
            saisFKs.add( new SaisFK(child, fkBuilder.fks) );
        }
        return new SaisTable(table, fields, saisFKs);
    }

    public SaisTable getSoleRootTable() {
        Set<SaisTable> tables = getRootTables();
        if (tables.size() != 1) {
            throw new IllegalStateException(String.format("%d tables defined", tables.size()));
        }
        return tables.iterator().next();
    }
}
