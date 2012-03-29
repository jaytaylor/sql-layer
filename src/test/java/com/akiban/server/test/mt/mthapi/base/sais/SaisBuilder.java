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

package com.akiban.server.test.mt.mthapi.base.sais;

import com.akiban.util.ArgumentValidation;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;

public final class SaisBuilder {
    public class FKBuilder {
        private final String parent;
        private final String child;
        private final List<FKPair> fks;

        private FKBuilder(String parent, String child) {
            ArgumentValidation.notNull("parent table", parent);
            ArgumentValidation.notNull("child table", child);
            confirmTable(parent);
            confirmTable(child);
            this.parent = parent;
            this.child = child;
            fks = new ArrayList<FKPair>();
        }

        public FKBuilder col(String parentColumn, String childColumn) {
            confirmColumn(parent, parentColumn);
            confirmColumn(child, childColumn);
            fks.add(new FKPair(parentColumn, childColumn));
            return this;
        }

        private void confirmTable(String table) {
            if (!tablesToFields.containsKey(table)) {
                throw new NoSuchElementException("unknown table: " + table);
            }
        }

        private void confirmColumn(String table, String column) {
            List<String> columns = tablesToFields.get(table);
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

        public SaisBuilder backToBuilder() {
            return SaisBuilder.this;
        }
    }

    public class TableBuilder {
        private final String child;

        TableBuilder(String child) {
            this.child = child;
        }

        public FKBuilder joinTo(String parent) {
            return join(parent, child);
        }

        public TableBuilder pk(String... pks) {
            ArgumentValidation.isGTE("pks can't be empty", pks.length, 1);
            List<String> old = tablesToPKs.put(child, Arrays.asList(pks));
            if (old != null) {
                tablesToPKs.put(child, old);
                throw new IllegalStateException("table alrady had a PK");
            }
            return this;
        }

        public TableBuilder key(String column) {
            return key(column, column);
        }

        public TableBuilder key(String name, String... cols) {
            // TODO: no-op for now!
            return this;
        }

        public SaisBuilder backToBuilder() {
            return SaisBuilder.this;
        }
    }

    Map<String,List<String>> tablesToFields = new HashMap<String, List<String>>();
    Map<String,List<String>> tablesToPKs = new HashMap<String, List<String>>();
    Map<String,Set<FKBuilder>> fkBuilders = new HashMap<String,Set<FKBuilder>>();
    Set<String> roots = new HashSet<String>();

    public TableBuilder table(String name, String... fields) {
        ArgumentValidation.notNull("table name", name);
        if(tablesToFields.containsKey(name)) {
            throw new IllegalArgumentException(name + " already defined");
        }
        List<String> fieldsList = Arrays.asList(fields);
        if (fieldsList.contains(null)) {
            throw new IllegalArgumentException("column names can't be null: " + fieldsList);
        }
        roots.add(name);
        tablesToFields.put(name, fieldsList);

        return new TableBuilder(name);
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

    public Set<SaisTable> getAllTables() {
        Set<SaisTable> tmp = new LinkedHashSet<SaisTable>();
        for(SaisTable root : getRootTables()) {
            recursivelyAddAll(root, tmp);
        }
        return tmp;
    }

    private static void recursivelyAddAll(SaisTable table, Set<SaisTable> out) {
        out.add(table);
        for (SaisFK fk : table.getChildren()) {
            recursivelyAddAll(fk.getChild(), out);
        }
    }

    private SaisTable recursivelyBuild(String table, Set<String> remainingTables) {
        Set<FKBuilder> tableFKBuilders = fkBuilders.get(table);
        List<String> fields = tablesToFields.get(table);
        List<String> pk = tablesToPKs.get(table);
        if (tableFKBuilders == null) {
            return new SaisTable(table, fields, pk, Collections.<SaisFK>emptySet());
        }

        Set<SaisFK> saisFKs = new HashSet<SaisFK>();
        for (FKBuilder fkBuilder : tableFKBuilders) {
            assert table.equals(fkBuilder.parent);
            String childName = fkBuilder.child;
            remainingTables.remove(childName);
            SaisTable child = recursivelyBuild(childName, remainingTables);
            saisFKs.add( new SaisFK(child, fkBuilder.fks) );
        }
        SaisTable saisTable = new SaisTable(table, fields, pk, saisFKs);
        for (SaisFK childFK : saisFKs) {
            childFK.setParent(saisTable);
            childFK.getChild().setParentFK(childFK);
        }
        return saisTable;
    }

    public SaisTable getSoleRootTable() {
        Set<SaisTable> tables = getRootTables();
        if (tables.size() != 1) {
            throw new IllegalStateException(String.format("%d tables defined", tables.size()));
        }
        return tables.iterator().next();
    }
}
