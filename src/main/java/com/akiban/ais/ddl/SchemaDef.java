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

package com.akiban.ais.ddl;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeMap;

import org.antlr.runtime.ANTLRFileStream;
import org.antlr.runtime.ANTLRInputStream;
import org.antlr.runtime.ANTLRStringStream;
import org.antlr.runtime.CharStream;
import org.antlr.runtime.CommonTokenStream;
import org.antlr.runtime.Parser;

import org.antlr.runtime.RecognitionException;

import com.akiban.ais.model.Column;
import com.akiban.server.error.SchemaDefParseException;

/**
 * Structures used to hold the results of parsing DDL statements. DDLSource.g
 * includes productions that modify a SchemaDef in a peculiar and particular
 * order. Other clients should only read values from this class.
 *
 * @author peter
 */
public class SchemaDef {

    public final static String CREATE_TABLE = "create table ";
    public final static String IF_NOT_EXISTS = "if not exists ";

    public enum IndexQualifier {
        FOREIGN_KEY, UNIQUE
    }

    private String masterSchemaName;

    private final Map<CName, UserTableDef> userTableMap = new TreeMap<CName, UserTableDef>();

    private final Map<CName, SortedSet<CName>> groupMap = new TreeMap<CName, SortedSet<CName>>();

    private UserTableDef currentTable;

    private String currentConstraintName;

    private IndexDef currentIndex;

    private ColumnDef currentColumn;

    private IndexColumnDef currentIndexColumn;

    private final List<IndexDefHandle> provisionalIndexes = new ArrayList<IndexDefHandle>();

    public static class SchemaDefException extends RuntimeException {
        private static final long serialVersionUID = 1L;

        public SchemaDefException(String message) {
            super(message);
        }
    }

    public void setMasterSchemaName(final String schemaName) {
        this.masterSchemaName = schemaName;
    }

    public String getMasterSchemaName() {
        return masterSchemaName;
    }

    public Map<CName, UserTableDef> getUserTableMap() {
        return userTableMap;
    }

    public Map<CName, SortedSet<CName>> getGroupMap() {
        return groupMap;
    }

    public UserTableDef getCurrentTable() {
        return currentTable;
    }

    void addTable(final CName tableName) {
        // If ANTLR detects a problem, it'll try to push forward as much as it
        // can. If this happens while there's
        // a provisional index pending, we need to clear it.
        provisionalIndexes.clear();
        // This method now supports incremental replacement of existing
        // user table definitions; therefore we attempt to remove any
        // prior definition.
        userTableMap.remove(tableName);
        currentTable = getUserTableDef(tableName);
        currentColumn = null;
    }

    void addLikeTable(final CName destName, final CName sourceName) {
        addTable(destName);
        currentTable.likeName = sourceName;
    }

    void addColumn(final String columnName, final String typeName, final String param1, final String param2) {
        final int uposition = currentColumn == null ? 0 : currentColumn.uposition + 1;
        currentColumn = new ColumnDef(columnName);
        currentColumn.typeName = typeName;
        currentColumn.typeParam1 = param1;
        currentColumn.typeParam2 = param2;
        currentColumn.uposition = uposition;
        convertColumnAlias();
        currentTable.columns.add(currentColumn);
    }

    void addColumnComment(final String comment) {
        currentColumn.comment = comment;
    }

    private enum ColumnOption {
        PRIMARY, UNIQUE, KEY
    }

    private ColumnOption prevColumnOption;
    private ColumnOption currColumnOption;

    void startColumnOption() {
    }

    void endColumnOption() {
        // The following will examine the grammar at different points, assuming options of:
        //      Foo [UNIQUE [KEY] | [PRIMARY] KEY] Bar
        // where Foo and Bar are some column option keyword other than UNIQUE/PRIMARY/KEY

        // At the end of this method, we'll set the next iteration's prevColumnOption. To help ensure that
        // we do this explicitly in all code paths, we'll do it via a local "final" var.
        final ColumnOption nextPrevious;

        if (currColumnOption == null) { // current option is Bar
            if (prevColumnOption == null) {
                nextPrevious = null; // Foo Bar -- nothing to do
            }
            else { // if prev is Foo, nothing to do
                switch (prevColumnOption) {
                    case UNIQUE: // Foo UNIQUE Bar -- unique key
                        inlineUniqueKey();
                        break;
                    case KEY: // Foo KEY Bar -- primary key
                        inlineColumnPK();
                        break;
                    default: // Foo PRIMARY Bar -- invalid
                        throw new SchemaDefException(prevColumnOption.toString());
                }
                nextPrevious = null;
            }
        }
        else {
            switch (currColumnOption) {
                case KEY:
                    if (prevColumnOption == ColumnOption.UNIQUE) {
                        inlineUniqueKey(); // UNIQUE KEY
                    }
                    else {
                        inlineColumnPK(); // Foo KEY or PRIMARY KEY
                    }
                    nextPrevious = null;
                    break;
                case PRIMARY:
                    nextPrevious = ColumnOption.PRIMARY;
                    break;
                case UNIQUE:
                    nextPrevious = ColumnOption.UNIQUE;
                    break;
                default:
                    throw new SchemaDefException("Unknown option: " + currColumnOption);
            }
            if(prevColumnOption == ColumnOption.PRIMARY && currColumnOption != ColumnOption.KEY) {
                throw new SchemaDefException("PRIMARY must be followed by KEY");
            }
        }
        prevColumnOption = nextPrevious;
        currColumnOption = null;
    }

    void seeKEY() {
        currColumnOption = ColumnOption.KEY;
    }

    void seePRIMARY() {
        currColumnOption = ColumnOption.PRIMARY;
    }

    void seeUNIQUE() {
        currColumnOption = ColumnOption.UNIQUE;
    }

    void inlineColumnPK() {
        if (currentTable.primaryKey == null) {
            startPrimaryKey();
            addIndexColumn(currentColumn.getName());
        } else {
            final String existingColName = currentTable.primaryKey.columns.get(0).getColumnName();
            if(!existingColName.equals(currentColumn.getName())) {
                throw new SchemaDefException("only one column may be marked as [PRIMARY] KEY");
            }
        }
    }

    void inlineUniqueKey() {
        IndexDef indexDef = addIndex(currentColumn.name);
        indexDef.qualifiers.add(IndexQualifier.UNIQUE);
        addIndexColumn(currentColumn.name);
    }

    void startPrimaryKey() {
        if (currentTable.primaryKey != null) {
            throw new SchemaDefException("too many primary keys");
        }
        currentIndex = currentTable.primaryKey = new IndexDef("PRIMARY");
    }

    void setEngine(final String engine) {
        currentTable.engine = engine;
    }

    void setConstraintName(final String name) {
        assert currentConstraintName == null : currentConstraintName;
        currentConstraintName = name;
    }

    void finishConstraint(IndexQualifier type) {
        if (type.equals(IndexQualifier.FOREIGN_KEY)) {
            assert !currentIndex.columns.isEmpty() : currentIndex;
            assert !currentIndex.references.isEmpty() : currentIndex;
            assert !currentIndex.references.get(currentIndex.references.size()-1).columns.isEmpty() : currentIndex;
        }
        currentIndex = null;
        currentConstraintName = null;
    }

    IndexDef addIndex(final String name) {
        currentIndex = new IndexDef(name);
        IndexDefHandle handle = new IndexDefHandle(currentIndex);

        if (name == null) {
            provisionalIndexes.add(handle);
        } else {
            currentTable.indexHandles.add(handle);
        }
        return currentIndex;
    }

    IndexDef addIndex(final String name, IndexQualifier qualifier) {
        String actualName = name;
        if (currentConstraintName != null) {
            if (IndexQualifier.FOREIGN_KEY.equals(qualifier) ||
               (IndexQualifier.UNIQUE.equals(qualifier) && name == null)) {
                actualName = currentConstraintName;
            }
        }
        addIndex(actualName);
        currentIndex.qualifiers.add(qualifier);
        addIndexQualifier(qualifier);
        return currentIndex;
    }

    /**
     * Collapse all indexes into the minimal set possible (with the additional
     * requirement that if a grouping key existed in the DDL, it will also exist
     * in the AIS -- even if that means multiple compatible keys):
     *   1) Combine foreign key into non-foreign key
     *   2) Combine any foreign key that is subset of another foreign key
     *   3) Combine any unnamed key into a named key
     *   4) Generate names for any unnamed key that is left
     */
    void resolveAllIndexes() {
        Map<List<IndexColumnDef>, IndexDef> columnsToIndexes = new HashMap<List<IndexColumnDef>, IndexDef>();
        
        // We have to add in two passes: first, adding only non-FK, and then adding only FKs
        List<IndexDefHandle> fkIndexHandles = new ArrayList<IndexDefHandle>();
        for (IndexDefHandle handle : currentTable.indexHandles) {
            if (handle.real.isForeignKey()) {
                fkIndexHandles.add(handle);
                continue;
            }
            List<IndexColumnDef> columns = handle.real.columns;
            if (!columnsToIndexes.containsKey(columns)) {
                columnsToIndexes.put(columns, handle.real);
            }
        }

        // Want the foreign keys indexes with the most number of columns to sort first
        // so that minimal number of new indexes are generated
        final Comparator<IndexDefHandle> fkComparator = new Comparator<IndexDefHandle>() {
            @Override
            public int compare(IndexDefHandle o1, IndexDefHandle o2) {
                return o2.real.columns.size() - o1.real.columns.size();
            }
        };

        Collections.sort(fkIndexHandles, fkComparator);
        for (IndexDefHandle handle : fkIndexHandles) {
            List<IndexColumnDef> columns = handle.real.columns;
            IndexDef equivalent = findEquivalentIndex(columnsToIndexes, handle.real);
            if (equivalent != null) {
                // For two compatible foreign keys, the name second in the declaration takes
                // takes precedence if they are equally sized (and equivalent, of course)
                if (equivalent.isForeignKey()
                    && (handle.real.columns.size() == equivalent.columns.size())) {
                    equivalent.name = handle.real.name;
                }
                equivalent.addIndexAttributes(handle.real);
                currentTable.indexHandles.remove(handle);
            }
            else if (!columnsToIndexes.containsKey(columns)) {
                columnsToIndexes.put(columns, handle.real);
            }
        }

        // Again, two passes: non-fk and then fk
        fkIndexHandles.clear();
        IndexNameGenerator indexNameGenerator = new IndexNameGenerator(currentTable.indexes);
        for (IndexDefHandle handle : provisionalIndexes) {
            if (handle.real.isForeignKey()) {
                fkIndexHandles.add(handle);
                continue;
            }
            final IndexDef real = handle.real;
            final IndexDef equivalent = findEquivalentIndex(columnsToIndexes, real);
            if (equivalent == null || equivalent.hasAkibanJoin()) {
                real.name = indexNameGenerator.generateName(real);
                currentTable.indexHandles.add(handle);
                columnsToIndexes.put(real.columns, real);
            } else {
                if (equivalent.isForeignKey() && !real.isForeignKey()) {
                    equivalent.name = indexNameGenerator.generateName(real);
                }
                equivalent.addIndexAttributes(real);
            }
        }
        provisionalIndexes.clear();

        Collections.sort(fkIndexHandles, fkComparator);
        for (IndexDefHandle handle : fkIndexHandles) {
            final IndexDef real = handle.real;
            final IndexDef equivalent = findEquivalentIndex(columnsToIndexes, real);
            if (equivalent == null || equivalent.hasAkibanJoin()) {
                real.name = indexNameGenerator.generateName(real);
                currentTable.indexHandles.add(handle);
                columnsToIndexes.put(real.columns, real);
            } else {
                equivalent.addIndexAttributes(real);
            }
        }

        // Finally, resolve all handles to their real selves
        Map<String,IndexDef> seenDefs = new HashMap<String,IndexDef>();
        for (IndexDefHandle handle : currentTable.indexHandles) {
            IndexDef index = handle.real;
            IndexDef oldIndex = seenDefs.get(index.name);
            if (oldIndex == null) {
                seenDefs.put(index.name, index);
                currentTable.indexes.add(index);
            }
            else {
                oldIndex.addIndexAttributes(index);
            }
        }
    }

    private static IndexDef findEquivalentIndex(Map<List<IndexColumnDef>, IndexDef> columnsToIndexes,
                                                IndexDef index) {
        if(index.hasAkibanJoin()) {
            return null;
        }
        List<IndexColumnDef> columns = index.columns;
        IndexDef exact = columnsToIndexes.get(columns);
        if (exact != null) {
            return exact;
        }
        if (index.isForeignKey()) {
            for (Map.Entry<List<IndexColumnDef>, IndexDef> entry : columnsToIndexes.entrySet()) {
                List<IndexColumnDef> testList = entry.getKey();
                if (columnListsAreSubset(testList, columns)) {
                    return entry.getValue();
                }
            }
        }
        return null;
    }

    private static boolean columnListsAreSubset(List<IndexColumnDef> larger, List<IndexColumnDef> smaller) {
        return larger.size() >= smaller.size() && larger.subList(0, smaller.size()).equals(smaller);
    }

    void addIndexQualifier(final IndexQualifier qualifier) {
        assert qualifier != null;
        currentIndex.qualifiers.add(qualifier);
        assert currentIndex.constraints.isEmpty() : currentIndex.constraints;
        if (currentConstraintName != null) {
            currentIndex.constraints.add(currentConstraintName);
        }
    }

    void addIndexColumn(final String columnName) {
        currentIndexColumn = new IndexColumnDef(columnName);
        currentIndex.columns.add(currentIndexColumn);
    }

    void addIndexReference(final CName referenceTableName) {
        currentIndex.addReferenceDef(currentConstraintName, referenceTableName);
    }

    void addIndexReferenceColumn(final String columnName) {
        currentIndex.addReferenceColumn(columnName);
    }

    void setIndexedLength(String indexedLength) {
        currentIndexColumn.indexedLength = indexedLength == null ? null
                : Integer.parseInt(indexedLength);
    }

    void setIndexColumnDesc() {
        currentIndexColumn.descending = true;
    }

    void use(final String name) {
        setMasterSchemaName(name);
    }

    void nullable(final boolean nullable) {
        currentColumn.nullable = nullable;
    }

    void autoIncrement() {
        if (currentTable.autoIncrementColumn != null) {
            throw new SchemaDefException("AUTO_INCREMENT already defined: "
                    + currentTable.autoIncrementColumn);
        }
        currentTable.autoIncrementColumn = currentColumn;
        currentColumn.autoincrement = 0L;
    }

    void autoIncrementInitialValue(final String value) {
        if (currentTable.autoIncrementColumn != null) {
            currentTable.autoIncrementColumn.setAutoIncrement(value);
        }
    }

    void addCharsetValue(final String charset) {
        if (currentColumn != null) {
            currentColumn.charset = charset;
        } else {
            currentTable.charset = charset;
            for (final ColumnDef column : currentTable.columns) {
                if (column.charset == null) {
                    column.charset = charset;
                }
            }
        }
    }

    void addCollateValue(final String collate) {
        if (currentColumn != null) {
            currentColumn.collate = collate;
        } else {
            currentTable.collate = collate;
            for (final ColumnDef column : currentTable.columns) {
                if (column.collate == null) {
                    column.collate = collate;
                }
            }
        }
    }

    void otherConstraint(final String constraint) {
        currentColumn.constraints.add(constraint);
    }

    void finishTable() {
        if (currentTable.primaryKey == null) {
            // Add our own primary key
            addColumn(Column.AKIBAN_PK_NAME, "BIGINT", null, null);
            currentColumn.nullable = false;
            startPrimaryKey();
            addIndexColumn(Column.AKIBAN_PK_NAME);
        }
        currentColumn = null;
    }

    void serialDefaultValue() {
        currentColumn.nullable = false;
        autoIncrement();
        startColumnOption();
        seeUNIQUE();
        endColumnOption();
    }

    /**
     * Updates the currentColumn to canonical types if it is an alias (SERIAL, BLOB(100), etc)
     */
    void convertColumnAlias() {
        String typeName = currentColumn.typeName;
        String param1 = currentColumn.typeParam1;
        String param2 = currentColumn.typeParam2;
        // MySQL: BIT(0) => BIT(1)
        if(typeName.equals("BIT") && param1 != null && param1.equals("0")) {
            param1 = "1";
        }
        // MySQL: BLOB/TEXT(L): L=0 => blob, L<2^8 => tiny, L<2^16 => blob, L<2^24 => medium, L<2^32 => large
        else if((typeName.equals("BLOB") || typeName.equals("TEXT")) && param1 != null) {
            final long len = Long.parseLong(param1);
            if(len >= 1L<<24) {
                typeName = "LONG" + typeName;
            }
            else if(len >= 1L<<16) {
                typeName = "MEDIUM" + typeName;
            }
            else if(len > 0 && len < 1L<<8) {
                typeName = "TINY" + typeName;
            }
            param1 = null;
        }
        else if(typeName.equals("NCHAR")) {
            typeName = "CHAR";
            currentColumn.charset = "utf8";
        }
        else if(typeName.equals("NVARCHAR")) {
            typeName = "VARCHAR";
            currentColumn.charset = "utf8";
        }
        // SQL: FLOAT(P): 0<=P<=24 => FLOAT,  25<=P<=53 => DOUBLE
        else if(typeName.equals("FLOAT") && param1 != null && param2 == null) {
            if(Long.parseLong(param1) > 24L) {
                typeName = "DOUBLE";
            }
            param1 = null;
        }
        // MySQL: SERIAL => BIGINT UNSIGNED NOT NULL AUTO_INCREMENT UNIQUE
        else if(typeName.equals("SERIAL")){
            typeName = "BIGINT UNSIGNED";
            serialDefaultValue();
        }
        currentColumn.typeName = typeName;
        currentColumn.typeParam1 = param1;
        currentColumn.typeParam2 = param2;
    }

    private UserTableDef getUserTableDef(final CName tableName) {
        UserTableDef def = userTableMap.get(tableName);
        if (def == null) {
            def = new UserTableDef(tableName);
            userTableMap.put(tableName, def);
        }
        return def;
    }

    public static class ColumnDef {
        String name;
        String typeName;
        String typeParam1;
        String typeParam2;
        boolean nullable = true;
        Long autoincrement = null;
        List<String> constraints = new ArrayList<String>();
        int uposition;
        String comment;
        String charset;
        String collate;

        ColumnDef(final String name) {
            this.name = name;
        }

        ColumnDef(final String name, String typeName, String param1,
                String param2, boolean nullable, String autoIncrement,
                String[] constraints) {
            this.name = name;
            this.typeName = typeName;
            this.typeParam1 = param1;
            this.typeParam2 = param2;
            this.nullable = nullable;
            this.constraints.addAll(Arrays.asList(constraints));
            setAutoIncrement(autoIncrement);
        }

        private void setAutoIncrement(String autoIncrement) {
            if (autoIncrement == null) {
                this.autoincrement = null;
                return;
            }
            try {
                // See bug696169, AUTO_INCREMENT=N should set initial value to N-1
                this.autoincrement = Long.parseLong(autoIncrement) - 1L;
            } catch (NumberFormatException e) {
                throw new SchemaDefException(
                        "Not a valid AUTO_INCREMENT value: " + autoIncrement);
            }
        }

        public String getName() {
            return name;
        }

        public String getType() {
            return typeName;
        }

        public Long defaultAutoIncrement() {
            return autoincrement;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            ColumnDef columnDef = (ColumnDef) o;
            if (nullable != columnDef.nullable) {
                return false;
            }
            if (autoincrement != null ? !autoincrement
                    .equals(columnDef.autoincrement)
                    : columnDef.autoincrement != null) {
                return false;
            }
            if (comment != null ? !comment.equals(columnDef.comment)
                    : columnDef.comment != null) {
                return false;
            }
            if (constraints != null ? !constraints
                    .equals(columnDef.constraints)
                    : columnDef.constraints != null) {
                return false;
            }
            if (name != null ? !name.equals(columnDef.name)
                    : columnDef.name != null) {
                return false;
            }
            if (typeName != null ? !typeName.equals(columnDef.typeName)
                    : columnDef.typeName != null) {
                return false;
            }
            if (typeParam1 != null ? !typeParam1.equals(columnDef.typeParam1)
                    : columnDef.typeParam1 != null) {
                return false;
            }
            if (typeParam2 != null ? !typeParam2.equals(columnDef.typeParam2)
                    : columnDef.typeParam2 != null) {
                return false;
            }
            return true;
        }

        @Override
        public int hashCode() {
            int result = name != null ? name.hashCode() : 0;
            result = 31 * result + (typeName != null ? typeName.hashCode() : 0);
            result = 31 * result
                    + (typeParam1 != null ? typeParam1.hashCode() : 0);
            result = 31 * result
                    + (typeParam2 != null ? typeParam2.hashCode() : 0);
            result = 31 * result + (nullable ? 1 : 0);
            result = 31 * result
                    + (autoincrement != null ? autoincrement.hashCode() : 0);
            result = 31 * result
                    + (constraints != null ? constraints.hashCode() : 0);
            result = 31 * result + (comment != null ? comment.hashCode() : 0);
            return result;
        }

        @Override
        public String toString() {
            return "ColumnDef[" + name + ' ' + typeName + '(' + typeParam1
                    + ',' + typeParam2 + ") nullable=" + nullable + " autoinc="
                    + autoincrement + " constraints" + constraints;
        }

        public String getCharset() {
            return charset;
        }
    }

    private static class IndexDefHandle {

        public IndexDefHandle(IndexDef real) {
            this.real = real;
        }

        IndexDef real;
    }

    public static class UserTableDef {
        public static String AKIBANDB_ENGINE_NAME = "akibandb";

        CName groupName;
        CName name;
        CName likeName;
        IndexDef primaryKey;
        List<ColumnDef> columns = new ArrayList<ColumnDef>();
        List<IndexDef> indexes = new ArrayList<IndexDef>();
        CName parentName;
        String engine = AKIBANDB_ENGINE_NAME;
        String charset;
        String collate;
        private final List<IndexDefHandle> indexHandles = new ArrayList<IndexDefHandle>();
        private ColumnDef autoIncrementColumn = null;

        UserTableDef(final CName name) {
            this.name = name;
        }

        public CName getCName() {
            return name;
        }

        public CName getLikeCName() {
            return likeName;
        }

        public boolean isLikeTableDef() {
            return likeName != null;
        }

        public List<ColumnDef> getColumns() {
            return columns;
        }

        public ColumnDef getColumn(String name) {
            for(ColumnDef def : columns) {
                if(def.name.equals(name)) {
                    return def;
                }
            }
            return null;
        }

        public List<String> getColumnNames() {
            List<String> ret = new ArrayList<String>(columns.size());
            for (ColumnDef col : columns) {
                assert !ret.contains(col.getName()) : col + " already in "
                        + ret;
                ret.add(col.getName());
            }
            return ret;
        }

        public ColumnDef getAutoIncrementColumn() {
            return autoIncrementColumn;
        }

        public IndexDef getPrimaryKey() {
            return primaryKey;
        }

        public List<ReferenceDef> getAkibanJoinRefs() {
            List<ReferenceDef> joinRefs = new ArrayList<ReferenceDef>();
            for (final IndexDef indexDef : indexes) {
                joinRefs.addAll(indexDef.getAkibanJoinRefs());
            }
            return joinRefs;
        }

        public boolean isAkibanTable() {
            return engine.equalsIgnoreCase(AKIBANDB_ENGINE_NAME);
        }

        public String getCharset() {
            return charset;
        }

        public List<IndexDef> getIndexes() {
            return indexes;
        }
    }

    private static final class IndexNameGenerator {
        private final Set<String> indexNames;

        public IndexNameGenerator(Collection<IndexDef> knownIndexes) {
            indexNames = new HashSet<String>();
            for (IndexDef knownIndex : knownIndexes) {
                boolean added = indexNames.add(knownIndex.name);
                assert added : knownIndex.name;
            }
        }

        public String generateName(IndexDef forIndex) {
            // Brute strength. For the low number of collisions we expect, this is simpler, and probably as fast,
            // as maintaining a Map<String,Integer> which to generate names, which we'd have to double-check are
            // actually unique.
            String baseName = forIndex.columns.get(0).columnName;
            String name = baseName;
            for(int suffixNum=2; indexNames.contains(name); ++suffixNum) {
                name = String.format("%s_%d", baseName, suffixNum);
            }
            indexNames.add(name);
            return name;
        }
    }

    public static class ReferenceDef {
        String name;
        CName table;
        IndexDef index;
        List<String> columns = new ArrayList<String>();

        ReferenceDef(String name, CName table, IndexDef index) {
            this.name = name;
            this.table = table;
            this.index = index;
        }

        public boolean isAkibanJoin() {
            return (name != null) && (name.startsWith("__akiban"));
        }

        public String getSchemaName() {
            return table.getSchema();
        }

        public String getTableName() {
            return table.getName();
        }

        public IndexDef getIndex() {
            return index;
        }

        public List<String> getColumns() {
            return columns;
        }

        public void addColumn(String columnName) {
            columns.add(columnName);
        }

        @Override
        public String toString() {
            return name + "_" + table + "(" + columns + ")";
        }

        @Override
        public boolean equals(Object o) {
            if(this == o) {
                return true;
            }
            if(o == null || getClass() != o.getClass()) {
                return false;
            }
            ReferenceDef that = (ReferenceDef) o;
            if(columns != null ? !columns.equals(that.columns) : that.columns != null) {
                return false;
            }
            if(name != null ? !name.equals(that.name) : that.name != null) {
                return false;
            }
            if(table != null ? !table.equals(that.table) : that.table != null) {
                return false;
            }
            return true;
        }

        @Override
        public int hashCode() {
            int result = name != null ? name.hashCode() : 0;
            result = 31 * result + (table != null ? table.hashCode() : 0);
            result = 31 * result + (columns != null ? columns.hashCode() : 0);
            return result;
        }
    }
    
    public static class IndexDef {
        String name;
        Set<IndexQualifier> qualifiers = EnumSet.noneOf(IndexQualifier.class);
        List<IndexColumnDef> columns = new ArrayList<IndexColumnDef>();
        List<ReferenceDef> references = new ArrayList<ReferenceDef>();
        List<String> constraints = new ArrayList<String>();
        String comment;

        IndexDef(final String name) {
            this.name = name;
        }

        public List<IndexColumnDef> getColumns() {
            return columns;
        }

        public List<String> getColumnNames() {
            List<String> ret = new ArrayList<String>(columns.size());
            for (IndexColumnDef col : columns) {
                ret.add(col.columnName);
            }
            return ret;
        }

        @Override
        public String toString() {
            return "IndexDef[name=" + name + "; columns=" + columns
                    + "; qualifiers=" + qualifiers + "; references="
                    + references + "; constraints=" + constraints + ']';
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            IndexDef indexDef = (IndexDef) o;
            if (columns != null ? !columns.equals(indexDef.columns)
                    : indexDef.columns != null) {
                return false;
            }
            if (comment != null ? !comment.equals(indexDef.comment)
                    : indexDef.comment != null) {
                return false;
            }
            if (constraints != null ? !constraints.equals(indexDef.constraints)
                    : indexDef.constraints != null) {
                return false;
            }
            if (name != null ? !name.equals(indexDef.name)
                    : indexDef.name != null) {
                return false;
            }
            if (qualifiers != null ? !qualifiers.equals(indexDef.qualifiers)
                    : indexDef.qualifiers != null) {
                return false;
            }
            if (references != null ? !references.equals(indexDef.references)
                    : indexDef.references != null) {
                return false;
            }
            return true;
        }

        @Override
        public int hashCode() {
            int result = name != null ? name.hashCode() : 0;
            result = 31 * result
                    + (qualifiers != null ? qualifiers.hashCode() : 0);
            result = 31 * result + (columns != null ? columns.hashCode() : 0);
            result = 31 * result + (references != null ? references.hashCode() : 0);
            result = 31 * result
                    + (constraints != null ? constraints.hashCode() : 0);
            result = 31 * result + (comment != null ? comment.hashCode() : 0);
            return result;
        }

        void addIndexAttributes(IndexDef otherIndex) {
            if(!columnListsAreSubset(columns, otherIndex.columns)) {
                throw new SchemaDefException(String.format(
                        "duplicate index: %s and %s, columns %s and %s",
                        name, otherIndex.name,columns, otherIndex.columns));
            }
            if (comment == null) {
                comment = otherIndex.comment;
            }
            qualifiers.addAll(otherIndex.qualifiers);
            constraints.addAll(otherIndex.constraints);
            references.addAll(otherIndex.references);
        }

        public List<ReferenceDef> getAkibanJoinRefs() {
            List<ReferenceDef> refs = new ArrayList<ReferenceDef>();
            for (ReferenceDef ref : references) {
                if (ref.isAkibanJoin()) {
                    refs.add(ref);
                }
            }
            return refs;
        }

        public boolean hasAkibanJoin() {
            return isForeignKey() && !getAkibanJoinRefs().isEmpty();
        }

        public void addReferenceDef(String name, CName table) {
            references.add(new ReferenceDef(name, table, this));
        }

        public void addReferenceColumn(String columnName) {
            assert !references.isEmpty() : "Index has no existing table reference";
            int lastIndex = references.size() - 1;
            references.get(lastIndex).addColumn(columnName);
        }

        public boolean isForeignKey() {
            return qualifiers.contains(IndexQualifier.FOREIGN_KEY);
        }

        public String getName() {
            return name;
        }
    }

    public static class IndexColumnDef {
        String columnName;
        Integer indexedLength = null;
        boolean descending = false;

        IndexColumnDef(String columnName) {
            this.columnName = columnName;
        }

        public String getColumnName() {
            return columnName;
        }
        
        /**
         * Returns true if o is an IndexColumnDef that describes the same
         * column.
         * 
         * @param o
         *            the other object
         * @return whether the other object is an IndexColumn with the same
         *         column name
         */
        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            IndexColumnDef that = (IndexColumnDef) o;
            return !(columnName != null ? !columnName.equals(that.columnName)
                    : that.columnName != null);
        }

        @Override
        public int hashCode() {
            return columnName != null ? columnName.hashCode() : 0;
        }

        @Override
        public String toString() {
            String ret = columnName;
            if (indexedLength != null) {
                ret += '(' + indexedLength + ')';
            }
            if (descending) {
                ret += " DESC";
            }
            return ret;
        }
    }

    /**
     * Qualified compound name. Represents schema.table, schema.group
     * 
     */
    public static class CName implements Comparable<CName> {
        private final String schema;
        private final String name;
        private final boolean schemaWasDerived;

        CName(final String schema, final String name) {
            this.schema = schema;
            this.name = name;
            this.schemaWasDerived = false;
        }

        CName(final SchemaDef schemaDef, final String schema, final String name) {
            String schemaName = schema;
            if (schemaName == null && schemaDef != null) {
                if (schemaDef.currentTable != null) {
                    schemaName = schemaDef.currentTable.name.schema;
                }
                if (schemaName == null) {
                    schemaName = schemaDef.getMasterSchemaName();
                }
            }
            this.schema = schemaName;
            this.name = name;
            this.schemaWasDerived = (schema == null);
        }

        @Override
        public int compareTo(CName c) {
            int result = getSchema().compareTo(c.getSchema());
            if (result == 0) {
                result = getName().compareTo(c.getName());
            }
            return result;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            CName cName = (CName) o;
            if (!name.equals(cName.name)) {
                return false;
            }
            if (schema != null ? !schema.equals(cName.schema)
                    : cName.schema != null) {
                return false;
            }
            return true;
        }

        @Override
        public int hashCode() {
            int result = schema != null ? schema.hashCode() : 0;
            result = 31 * result + name.hashCode();
            return result;
        }

        @Override
        public String toString() {
            return getSchema() + "." + getName();
        }

        public String getSchema() {
            return schema;
        }

        public String getName() {
            return name;
        }

        public boolean getSchemaWasDerived() {
            return schemaWasDerived;
        }
    }

    private static int lowerCaseLA(char[] data, int n, int i, int p) {
        if (i == 0) {
            return 0; // undefined
        }
        if (i < 0) {
            i++; // e.g., translate LA(-1) to use offset 0
            if ((p + i - 1) < 0) {
                // Invalid. No char before first char.
                return CharStream.EOF;
            }
        }
        if ((p + i - 1) >= n) {
            return CharStream.EOF;
        }
        return Character.toLowerCase(data[p + i - 1]);
    }

    /**
     * Case Insensitive ANTLRStringStream
     */
    public static class SDStringStream extends ANTLRStringStream {
        public SDStringStream(final String string) {
            super(string);
        }

        @Override
        public int LA(int i) {
            return lowerCaseLA(data, n, i, p);
        }
    }

    /**
     * Case Insensitive ANTLRFileStream
     */
    public static class SDFileStream extends ANTLRFileStream {
        public SDFileStream(final String fileName) throws IOException {
            super(fileName);
        }

        @Override
        public int LA(int i) {
            return lowerCaseLA(data, n, i, p);
        }
    }

    /**
     * Case Insensitive ANTLRInputStream
     */
    public static class SDInputStream extends ANTLRInputStream {
        public SDInputStream(InputStream input) throws IOException {
            super(input);
        }

        @Override
        public int LA(int i) {
            return lowerCaseLA(data, n, i, p);
        }
    }

    /**
     * Incrementally adds a new table to the SchemaDef.
     * 
     * @param createTableStatement
     * @return
     * @throws Exception
     */
    public UserTableDef parseCreateTable(final String createTableStatement) throws SchemaDefParseException, SchemaDefException {
        DDLSourceLexer lex = new DDLSourceLexer(new SDStringStream(
                createTableStatement));
        CommonTokenStream tokens = new CommonTokenStream(lex);
        final DDLSourceParser tsparser = new DDLSourceParser(tokens);
        try {
            tsparser.table(this);
            if (tsparser.getNumberOfSyntaxErrors() > 0) {
                throw new SchemaDefParseException ("", "Syntax Error", createTableStatement);
            }
        } catch (RecognitionException e) {
            throw new SchemaDefParseException ("",  tsparser.getErrorHeader(e), createTableStatement);
        }
        return getCurrentTable();
    }

    public static SchemaDef parseSchema(final String schema) throws SchemaDefParseException  {
        return parseSchemaFromANTLRStream(new SDStringStream(schema));
    }

    public static SchemaDef parseSchemaFromFile(final String fileName) throws IOException, SchemaDefParseException {
        return parseSchemaFromANTLRStream(new SDFileStream(fileName));
    }

    public static SchemaDef parseSchemaFromStream(InputStream stream)  throws SchemaDefParseException, IOException {
        return parseSchemaFromANTLRStream(new SDInputStream(stream));
    }

    private static SchemaDef parseSchemaFromANTLRStream(ANTLRStringStream stream)  {
        DDLSourceLexer lex = new DDLSourceLexer(stream);
        CommonTokenStream tokens = new CommonTokenStream(lex);
        final DDLSourceParser tsparser = new DDLSourceParser(tokens);
        final SchemaDef schemaDef = new SchemaDef();
        try {
            tsparser.schema(schemaDef);
        } catch (RecognitionException e) {
            throw new SchemaDefParseException ("", tsparser.getErrorHeader(e), tsparser.getErrorMessage(e, tsparser.getTokenNames()));
        }
        return schemaDef;
    }

    /**
     * Transform a given statement into a minimal known state:
     * <ul>
     *     <li>Remove preceding and trailing whitespace</li>
     *     <li>Ensure that it starts with, exactly, 'create table '</li>
     *     <li>Ensure that it does not contain 'if not exists'</li>
     *     <li>Ensure that it ends with ';'</li>
     * </ul>
     * @param s Statement to 'canonicalize'
     * @return The statement modified to appear as described.
     */
    public static String canonicalStatement(final String s) {
        String minimal = s.trim();
        minimal = strip(minimal, CREATE_TABLE);
        minimal = strip(minimal, IF_NOT_EXISTS).trim();
        minimal = CREATE_TABLE + minimal;
        final int len = minimal.length();
        if(len > 0 && minimal.charAt(len-1) != ';') {
            return minimal + ";";
        }
        return minimal;
    }

    private static String strip(final String s1, final String s2) {
        final int sLen = s2.length();
        if (s1.length() >= sLen && s1.substring(0, sLen).equalsIgnoreCase(s2)) {
            return s1.substring(sLen);
        }
        return s1;
    }
}
