package com.akiban.ais.ddl;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeMap;

/**
 * Structures used to hold the results of DDLSource parse. DDLSource.g includes
 * productions that modify a SchemaDef in a peculiar and particular order. Other
 * clients should only read values from this class.
 * 
 * @author peter
 */
public class SchemaDef {

    public static class SchemaDefException extends RuntimeException {
        public SchemaDefException(String message) {
            super(message);
        }
    }


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
        // If ANTLR detects a problem, it'll try to push forward as much as it can. If this happens while there's
        // a provisional index pending, we need to clear it.
        provisionalIndexes.clear();
        currentTable = getUserTableDef(tableName);
    }

    void addColumn(final String columnName, final String typeName,
            final String param1, final String param2) {
        currentColumn = new ColumnDef(columnName);
        currentColumn.typeName = typeName;
        currentColumn.typeParam1 = param1;
        currentColumn.typeParam2 = param2;
        currentTable.columns.add(currentColumn);
    }

    void addColumnComment(final String comment) {
        currentColumn.comment = comment;
    }


    void inlineColumnPK() {
        checkPkEmpty();
        addPrimaryKeyColumn(currentColumn.getName());
    }

    void inlineKey() {
        addIndex(currentColumn.name);
        addIndexColumn(currentColumn.name);
    }

    void inlineUniqueKey() {
        IndexDef indexDef = addIndex(currentColumn.name);
        indexDef.qualifiers.add(IndexQualifier.UNIQUE);
        addIndexColumn(currentColumn.name);
    }

    void startPrimaryKey() {
        checkPkEmpty();
    }

    private void checkPkEmpty() {
        if (!currentTable.primaryKey.isEmpty()) {
            throw new SchemaDefException("too many primary keys");
        }
    }

    void addPrimaryKeyColumn(final String columnName) {
        currentTable.primaryKey.add(columnName);
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
            assert ! currentIndex.columns.isEmpty() : currentIndex;
            assert ! currentIndex.referenceColumns.isEmpty() : currentIndex;
            assert null != currentIndex.referenceTable : currentIndex;
        }
        currentIndex = null;
        currentConstraintName = null;
    }

    IndexDef addIndex(final String name) {
        currentIndex = new IndexDef(name);
        IndexDefHandle handle = new IndexDefHandle(currentIndex);

        if (name == null) {
            provisionalIndexes.add(handle);
        }
        else {
            currentTable.indexHandles.add(handle);
        }
        return currentIndex;
    }

    static boolean isAkiban(final IndexDef indexDef) {
        for (String constraint : indexDef.constraints) {
            if (constraint.startsWith("__akiban")) {
                return true;
            }
        }
        return false;
    }

    void resolveProvisionalIndexes() {
        // We'll go over each of our provisional indexes, each of which has a null name.
        // If the current table doesn't already have an equivalent index, we'll just give this index a name
        // and add it to the table.
        // Otherwise, we'll add the provisional index's attributes to the existing index.

        Map<List<IndexColumnDef>,IndexDef> columnsToIndexes = new HashMap<List<IndexColumnDef>, IndexDef>();
        for (IndexDefHandle handle : currentTable.indexHandles) {
            if (!columnsToIndexes.containsKey(handle.real.columns)) {
                columnsToIndexes.put(handle.real.columns, handle.real);
            }
        }
        int id = 0;
        for (IndexDefHandle handle : provisionalIndexes) {
            final IndexDef real = handle.real;
            final IndexDef equivalent = columnsToIndexes.get(real.columns);
            if (equivalent == null) {
                real.name = String.format("_auto_generated_index_%d", id++);
                currentTable.indexHandles.add(handle);
                columnsToIndexes.put(real.columns, real);
            }
            else {
                if (real.qualifiers.contains(IndexQualifier.FOREIGN_KEY)) {
                    assert real.columns.equals(equivalent.columns) : real + " " + equivalent;
                    // two FK indexes, make sure they're compatible
                    if (equivalent.qualifiers.contains(IndexQualifier.FOREIGN_KEY)) {
                        if (equivalent.referenceTable != null
                                && !real.referenceTable.equals(equivalent.referenceTable)) {
                            throw new IllegalStateException("incompatible reference tables between provisional "
                                    + real + " and " + equivalent);
                        }
                        else if (!equivalent.referenceColumns.isEmpty()
                                && ! real.referenceColumns.equals(equivalent.referenceColumns)) {
                            throw new IllegalStateException("incompatible columns between provisional "
                                    + real + " and " + equivalent);
                        }
                    }
                    else {
                        // Assert there's no FK-like stuff here already, then add it
                        assert equivalent.referenceTable == null : equivalent.referenceTable;
                        assert equivalent.referenceColumns.isEmpty() : equivalent.referenceColumns;
                        equivalent.referenceTable = real.referenceTable;
                        equivalent.referenceColumns.addAll(real.referenceColumns);
                    }
                }
                equivalent.qualifiers.addAll(real.qualifiers);
                assert real.constraints.size() <= 1 : real.constraints;
                equivalent.constraints.addAll(real.constraints);
            }
        }
        provisionalIndexes.clear();

        // Finally, resolve all handles to their  real selves
        HashSet<IndexDef> seenDefs = new HashSet<IndexDef>();
        for (IndexDefHandle handle : currentTable.indexHandles) {
            if (seenDefs.add(handle.real)) {
                currentTable.indexes.add(handle.real);
            }
        }
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

    void setIndexReference(final CName referenceTableName) {
        currentIndex.referenceTable = referenceTableName;
    }

    void addIndexReferenceColumn(final String columnName) {
        currentIndex.referenceColumns.add(columnName);
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
            throw new SchemaDefException("AUTO_INCREMENT already defined: " + currentTable.autoIncrementColumn);
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
        currentColumn = null;
    }
    /**
     * Checks (via assert) that the comment doesn't appear to be an old-style grouping.
     * @param text the text of the comment, including slash-star and star-slash.
     */
    void comment(final String text) {
        if (text != null) {
            StringBuilder inner = new StringBuilder( text.substring(2, text.length()-2).trim() );
            inner.setLength("schema".length());
            assert ! "schema".equals(inner.toString()) : "found grouping comment " + text;
        }
    }

    private UserTableDef getUserTableDef(final CName tableName) {
        UserTableDef def = userTableMap.get(tableName);
        if (def == null) {
            def = new UserTableDef(tableName, userTableMap.size());
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
        int gposition;
        String comment;
        String charset;
        String collate;

        ColumnDef(final String name) {
            this.name = name;
        }

        ColumnDef(final String name, String typeName, String param1, String param2, boolean nullable,
                  String autoIncrement, String[] constraints) {
            this.name = name;
            this.typeName = typeName;
            this.typeParam1 = param1;
            this.typeParam2 = param2;
            this.nullable = nullable;
            this.constraints.addAll( Arrays.asList(constraints));
            setAutoIncrement(autoIncrement);
        }

        private void setAutoIncrement(String autoIncrement) {
            if (autoIncrement == null) {
                this.autoincrement = null;
                return;
            }
            try {
                this.autoincrement = Long.parseLong(autoIncrement);
            } catch (NumberFormatException e) {
                throw new SchemaDefException("Not a valid AUTO_INCREMENT value: " + autoIncrement);
            }
        }

        public String getName() {
            return name;
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
            if (autoincrement != null ? !autoincrement.equals(columnDef.autoincrement) : columnDef.autoincrement != null) {
                return false;
            }
            if (comment != null ? !comment.equals(columnDef.comment) : columnDef.comment != null) {
                return false;
            }
            if (constraints != null ? !constraints.equals(columnDef.constraints) : columnDef.constraints != null) {
                return false;
            }
            if (name != null ? !name.equals(columnDef.name) : columnDef.name != null) {
                return false;
            }
            if (typeName != null ? !typeName.equals(columnDef.typeName) : columnDef.typeName != null) {
                return false;
            }
            if (typeParam1 != null ? !typeParam1.equals(columnDef.typeParam1) : columnDef.typeParam1 != null) {
                return false;
            }
            if (typeParam2 != null ? !typeParam2.equals(columnDef.typeParam2) : columnDef.typeParam2 != null) {
                return false;
            }
            return true;
        }

        @Override
        public int hashCode() {
            int result = name != null ? name.hashCode() : 0;
            result = 31 * result + (typeName != null ? typeName.hashCode() : 0);
            result = 31 * result + (typeParam1 != null ? typeParam1.hashCode() : 0);
            result = 31 * result + (typeParam2 != null ? typeParam2.hashCode() : 0);
            result = 31 * result + (nullable ? 1 : 0);
            result = 31 * result + (autoincrement != null ? autoincrement.hashCode() : 0);
            result = 31 * result + (constraints != null ? constraints.hashCode() : 0);
            result = 31 * result + (comment != null ? comment.hashCode() : 0);
            return result;
        }

        @Override
        public String toString() {
            return "ColumnDef[" + name + ' ' + typeName + '(' + typeParam1 + ',' + typeParam2
                    + ") nullable=" + nullable + " autoinc=" + autoincrement + " constraints" + constraints;
        }
    }

    private static class IndexDefHandle {

        public IndexDefHandle(IndexDef real) {
            this.real = real;
        }

        IndexDef real;
    }

    public static class UserTableDef {
        CName groupName;
        CName name;
        List<ColumnDef> columns = new ArrayList<ColumnDef>();
        List<String> primaryKey = new ArrayList<String>();
        List<String> childJoinColumns = new ArrayList<String>();
        List<String> parentJoinColumns = new ArrayList<String>();
        List<IndexDef> indexes = new ArrayList<IndexDef>();
        UserTableDef parent;
        String engine = "akibandb";
        int id;
        private final List<IndexDefHandle> indexHandles = new ArrayList<IndexDefHandle>();
        private ColumnDef autoIncrementColumn = null;

        UserTableDef(final CName name, int id) {
            this.name = name;
            this.id = id;
        }

        public CName getCName() {
            return name;
        }

        public List<ColumnDef> getColumns() {
            return columns;
        }

        public List<String> getColumnNames() {
            List<String> ret = new ArrayList<String>(columns.size());
            for (ColumnDef col : columns) {
                assert !ret.contains(col.getName()) : col + " already in " + ret;
                ret.add(col.getName());
            }
            return ret;
        }

        public ColumnDef getAutoIncrementColumn() {
            return autoIncrementColumn;
        }

        public List<String> getPrimaryKey() {
            return Collections.unmodifiableList(primaryKey);
        }

        public int id() {
            return id;
        }
    }

    public static class IndexDef {
        String name;
        Set<IndexQualifier> qualifiers = EnumSet.noneOf(IndexQualifier.class);
        List<IndexColumnDef> columns = new ArrayList<IndexColumnDef>();
        CName referenceTable;
        List<String> referenceColumns = new ArrayList<String>();
        List<String> constraints = new ArrayList<String>();
        String comment;

        IndexDef(final String name) {
            this.name = name;
        }

        public IndexDef(String name, Set<IndexQualifier> qualifiers, List<IndexColumnDef> columns, CName referenceTable,
                        List<String> referenceColumns, List<String> constraints) {
            this.name = name;
            this.qualifiers = qualifiers;
            this.columns = columns;
            this.referenceTable = referenceTable;
            this.referenceColumns = referenceColumns;
            this.constraints = constraints;
        }

        /**
         * Gets the parent schema
         * @return the parsed schema name, or null if there was no parsed schema
         */
        public String getParentSchema() {
            return getParentSchema(null);
        }
        
        /**
         * Gets the parent schema, or the specified default if the parent schema is null
         * @param defaultSchema the specified default
         * @return the parsed schema name, or defaultSchema if there was no parsed schema
         */
        public String getParentSchema(String defaultSchema) {
            String ret = referenceTable.getSchema();
            return ret == null ? defaultSchema : ret;
        }

        public String getParentTable() {
            return referenceTable.getName();
        }

        public List<String> getParentColumns() {
            return new ArrayList<String>(referenceColumns);
        }

        public List<String> getChildColumns() {
            List<String> ret = new ArrayList<String>(referenceColumns.size());
            for (IndexColumnDef col : columns) {
                ret.add(col.columnName);
            }
            return ret;
        }

        @Override
        public String toString() {
            return "IndexDef[name=" + name + "; columns=" + columns + "; qualifiers=" + qualifiers
                    + "; references=" + referenceTable + " parent columns " + referenceColumns
                    + "; contraints=" + constraints + ']';
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
            if (columns != null ? !columns.equals(indexDef.columns) : indexDef.columns != null) {
                return false;
            }
            if (comment != null ? !comment.equals(indexDef.comment) : indexDef.comment != null) {
                return false;
            }
            if (constraints != null ? !constraints.equals(indexDef.constraints) : indexDef.constraints != null) {
                return false;
            }
            if (name != null ? !name.equals(indexDef.name) : indexDef.name != null) {
                return false;
            }
            if (qualifiers != null ? !qualifiers.equals(indexDef.qualifiers) : indexDef.qualifiers != null) {
                return false;
            }
            if (referenceColumns != null ? !referenceColumns.equals(indexDef.referenceColumns) : indexDef.referenceColumns != null) {
                return false;
            }
            if (referenceTable != null ? !referenceTable.equals(indexDef.referenceTable) : indexDef.referenceTable != null) {
                return false;
            }
            return true;
        }

        @Override
        public int hashCode() {
            int result = name != null ? name.hashCode() : 0;
            result = 31 * result + (qualifiers != null ? qualifiers.hashCode() : 0);
            result = 31 * result + (columns != null ? columns.hashCode() : 0);
            result = 31 * result + (referenceTable != null ? referenceTable.hashCode() : 0);
            result = 31 * result + (referenceColumns != null ? referenceColumns.hashCode() : 0);
            result = 31 * result + (constraints != null ? constraints.hashCode() : 0);
            result = 31 * result + (comment != null ? comment.hashCode() : 0);
            return result;
        }
    }

    public static class IndexColumnDef {
        String columnName;
        Integer indexedLength = null;
        boolean descending = false;

        IndexColumnDef(String columnName) {
            this.columnName = columnName;
        }

        /**
         * Returns true if o is an IndexColumnDef that describes the same column.
         * @param o the other object
         * @return whether the other object is an IndexColumn with the same column name
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
            return !(columnName != null ? !columnName.equals(that.columnName) : that.columnName != null);
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
        private String schema;
        private String name;

        CName(final String schema, final String name) {
            this.schema = schema;
            this.name = name;
        }

        CName(final SchemaDef schemaDef, final String schema, final String name) {
            String schemaName = schema;
            if (schemaName == null && schemaDef != null
                    && schemaDef.currentTable != null) {
                schemaName = schemaDef.currentTable.name.schema;
            }
            if (schemaName == null) {
                schemaName = schemaDef.getMasterSchemaName();
            }

            this.schema = schemaName;
            this.name = name;
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
            if (schema != null ? !schema.equals(cName.schema) : cName.schema != null) {
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
    }
}
