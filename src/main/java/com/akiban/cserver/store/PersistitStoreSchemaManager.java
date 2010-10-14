package com.akiban.cserver.store;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;
import java.util.regex.Pattern;

import com.akiban.ais.model.TableName;
import com.akiban.cserver.IndexDef;
import com.akiban.cserver.InvalidOperationException;
import com.akiban.cserver.RowDef;
import com.akiban.cserver.RowDefCache;
import com.akiban.message.ErrorCode;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.akiban.ais.ddl.DDLSource;
import com.akiban.ais.ddl.SchemaDef;
import com.akiban.cserver.CServerConstants;
import com.akiban.cserver.CServer.CreateTableStruct;
import com.persistit.Exchange;
import com.persistit.Key;
import com.persistit.KeyFilter;
import com.persistit.exception.PersistitException;

public class PersistitStoreSchemaManager implements CServerConstants {

    private static final Log LOG = LogFactory
            .getLog(PersistitStoreSchemaManager.class.getName());

    private final static String SCHEMA_TREE_NAME = "_schema_";
    
    private final static String AKIBA_INFORMATION_SCHEMA = "akiba_information_schema";

    private final static String BY_ID = "byId";

    private final static String BY_NAME = "byName";

    private final PersistitStore store;

    private final Set<ColumnName> knownColumns = new HashSet<ColumnName>(100);

    private static class ColumnName {
        private final String tableName;
        private final String columnName;

        private ColumnName(String tableName, String columnName) {
            assert tableName != null : "null tablename";
            assert columnName != null : "null columnName";
            this.tableName = tableName;
            this.columnName = columnName;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            ColumnName that = (ColumnName) o;
            if (!columnName.equals(that.columnName)) {
                return false;
            }
            if (!tableName.equals(that.tableName)) {
                return false;
            }
            return true;
        }

        @Override
        public int hashCode() {
            int result = tableName.hashCode();
            result = 31 * result + columnName.hashCode();
            return result;
        }

        @Override
        public String toString() {
            return String.format("(%s,%s)", tableName, columnName);
        }
    }

    public PersistitStoreSchemaManager(final PersistitStore store) {
        this.store = store;
    }
    
    void populateSchema(final List<CreateTableStruct> result) throws PersistitException {
        Exchange ex1 = null;
        Exchange ex2 = null;

        try {
            ex1 = store.getExchange(SCHEMA_TREE_NAME);
            ex2 = store.getExchange(SCHEMA_TREE_NAME);
            ex1.clear().append(BY_NAME);
            final KeyFilter keyFilter = new KeyFilter(ex1.getKey(), 4, 4);

            ex1.clear();
            while (ex1.next(keyFilter)) {
                // Traverse to the largest tableId (most recent)
                if (!ex1.to(Key.AFTER).previous()) {
                    continue;
                }
                final String schemaName = ex1.getKey().indexTo(1)
                        .decodeString();
                final String tableName = ex1.getKey().indexTo(2).decodeString();
                final int tableId = ex1.getKey().indexTo(3).decodeInt();

                ex2.clear().append(BY_ID).append(tableId).fetch();
                if (!ex2.getValue().isDefined()) {
                    if (LOG.isWarnEnabled()) {
                        LOG.warn("No table definition for " + ex1.getKey());
                    }
                }
                final String ddl = ex2.getValue().getString();
                result.add(new CreateTableStruct(tableId, schemaName, tableName,
                        ddl));
            }

        } catch (Throwable t) {
            LOG.error("createTable failed", t);
        } finally {
            store.releaseExchange(ex1);
            store.releaseExchange(ex2);
        }
    }

    private boolean checkForDuplicateColumns(SchemaDef.UserTableDef tableDef) {
        Set<ColumnName> tmp = new HashSet<ColumnName>(knownColumns);
        for (SchemaDef.ColumnDef cDef : tableDef.getColumns()) {
            ColumnName cName = new ColumnName(tableDef.getCName().getName(), cDef.getName());
            if (!tmp.add(cName)) {
                if (LOG.isDebugEnabled()) {
                    LOG.debug("table/column pair already exists: " + 
                            cName + " -- abandoning createTable");
                }
                return false;
            }
        }
        knownColumns.addAll(tmp);
        assert knownColumns.size() == tmp.size()
                : String.format("union not of equal size: %s after adding %s", knownColumns, tmp);
        return true;
    }

    public void forgetTableColumns(String tableName) {
        Iterator<ColumnName> iter = knownColumns.iterator();
        while (iter.hasNext()) {
            if (tableName.equals(iter.next().tableName)) {
                iter.remove();
            }
        }
    }

    final static Pattern UTF8_PATTERN = Pattern.compile("CHARACTER\\s+SET\\s*[=\\s]\\s*UTF8", Pattern.CASE_INSENSITIVE);
    /**
     * Attempts to create a table.
     * @param useSchemaName the table's schema name
     * @param ddl the table's raw DDL
     * @param outTableId will be set to the table's ID, but only if this method succeeds
     * @param rowDefCache the existing RowDefCache. Used to validate parent columns.
     * @return CServerConstants.OK iff everything worked
     * @throws InvalidOperationException if the table isn't valid (or can't be parsed)
     */
    void createTable(final String useSchemaName, final String ddl, AtomicReference<Integer> outTableId, RowDefCache rowDefCache) throws InvalidOperationException, PersistitException {
        Exchange ex = null;
        // Hacky way to avoid UTF-8
        if (UTF8_PATTERN.matcher(ddl).find()) {
            throw new InvalidOperationException(ErrorCode.UNSUPPORTED_CHARSET, "[%s] UTF8: %s", useSchemaName, ddl);
        }

        String canonical = DDLSource.canonicalStatement(ddl);
        final SchemaDef.UserTableDef tableDef;
        try {
            tableDef = new DDLSource().parseCreateTable(canonical);
        } catch (Exception e1) {
            throw new InvalidOperationException(ErrorCode.PARSE_EXCEPTION, "[%s] %s: %s", useSchemaName, e1.getMessage(), canonical);
        }
        if (AKIBA_INFORMATION_SCHEMA.equals(tableDef.getCName().getSchema()) || "akiba_objects".equals(tableDef.getCName().getSchema())) {
            throw new InvalidOperationException(ErrorCode.PROTECTED_TABLE, "[%s] %s is protected: %s", useSchemaName, AKIBA_INFORMATION_SCHEMA, ddl);
        }
        if (tableDef.getPrimaryKey().size() == 0) {
            throw new InvalidOperationException(ErrorCode.NO_PRIMARY_KEY, "[%s] %s", useSchemaName, ddl);
        }

        SchemaDef.IndexDef parentJoin = DDLSource.getAkibanJoin(tableDef);
        if (parentJoin != null) {
            if (AKIBA_INFORMATION_SCHEMA.equals(parentJoin.getParentSchema())
                    || "akiba_objects".equals(parentJoin.getParentSchema())) {
                throw new InvalidOperationException(ErrorCode.JOIN_TO_PROTECTED_TABLE, "[%s] to %s.*: %s", useSchemaName, parentJoin.getParentSchema(), ddl);
            }
            String parentSchema = parentJoin.getParentSchema();
            if (parentSchema == null) {
                parentSchema = (tableDef.getCName().getSchema() == null)
                        ? useSchemaName
                        : tableDef.getCName().getSchema();
            }
            final String parentName = RowDefCache.nameOf(
                    parentJoin.getParentSchema(useSchemaName),
                    parentJoin.getParentTable());
            final RowDef parentDef = rowDefCache.getRowDef(parentName);
            if (parentDef == null) {
                throw new InvalidOperationException(ErrorCode.JOIN_TO_UNKNOWN_TABLE, "[%s] to %s: %s", useSchemaName, parentName, ddl);
            }
            IndexDef parentPK = parentDef.getPKIndexDef();
            List<String> parentPKColumns = new ArrayList<String>(parentPK.getFields().length);
            for (int fieldIndex : parentPK.getFields()) {
                parentPKColumns.add( parentDef.getFieldDef(fieldIndex).getName() );
            }
            if (!parentPKColumns.equals( parentJoin.getParentColumns() )) {
                throw new InvalidOperationException(ErrorCode.JOIN_TO_WRONG_COLUMNS, "[%s] %s%s references %s%s: %s",
                        useSchemaName,
                        tableDef.getCName(), parentJoin.getParentColumns(), parentName, parentPKColumns,
                        ddl);
            }
        }

        // TODO: For now, we can't handle situations in which a tablename+columnname already exist
        //    This is because group table columns are qualified only by (uTable,uTableCol) so there
        //    is a collision if we have (s1, tbl, col) and (s2, tbl, col)
        if (!checkForDuplicateColumns(tableDef)) {
            throw new InvalidOperationException(ErrorCode.DUPLICATE_COLUMN_NAMES,
                    "[%s] knownColumns=%s  ddl=%s", useSchemaName, knownColumns, ddl);
        }

        try {
            ex = store.getExchange(SCHEMA_TREE_NAME);

            String schemaName = tableDef.getCName().getSchema();
            if (schemaName == null) {
                schemaName = useSchemaName;
                canonical = '`' + useSchemaName + "`." + canonical;
            }
            final String tableName = tableDef.getCName().getName();

            if (ex.clear().append(BY_NAME).append(schemaName).append(tableName)
                    .append(Key.AFTER).previous()) {
                final int tableId = ex.getKey().indexTo(-1).decodeInt();
                outTableId.set(tableId);
                ex.clear().append(BY_ID).append(tableId).fetch();
                final String previousValue = ex.getValue().getString();
                if (canonical.equals(previousValue)) {
                    return;
                }
            }
            
            final int tableId;
            if (ex.clear().append(BY_ID).append(Key.AFTER).previous()) {
                tableId = ex.getKey().indexTo(1).decodeInt() + 1;
            } else {
                tableId = 1;
            }
            outTableId.set(tableId);
            ex.getValue().put(canonical);
            ex.clear().append(BY_ID).append(tableId).store();
            ex.getValue().putNull();
            ex.clear().append(BY_NAME).append(schemaName).append(tableName)
                    .append(tableId).store();
            
            return;

            // } catch (StoreException e) {
            // if (verbose && LOG.isInfoEnabled()) {
            // LOG.info("createTable error " + e.getResult(), e);
            // }
            // return e.getResult();
        } finally {
            store.releaseExchange(ex);
        }
    }

    /**
     * Removes the create table statement(s) for the specified schema/table
     * 
     * @param schemaName the table's schema
     * @param tableName the table's name
     * @throws InvalidOperationException if the table is protected
     */
    void dropCreateTable(final String schemaName, final String tableName) throws PersistitException {
        
        if (AKIBA_INFORMATION_SCHEMA.equals(schemaName) || "akiba_objects".equals(schemaName)) {
            return;
        }
        
        Exchange ex1 = null;
        Exchange ex2 = null;
        try {
            ex1 = store.getExchange(SCHEMA_TREE_NAME);
            ex2 = store.getExchange(SCHEMA_TREE_NAME);
            ex1.clear().append(BY_NAME).append(schemaName).append(tableName);
            final KeyFilter keyFilter = new KeyFilter(ex1.getKey(), 4, 4);

            ex1.clear();
            while (ex1.next(keyFilter)) {
                final int tableId = ex1.getKey().indexTo(-1).decodeInt();
                ex2.clear().append(BY_ID).append(tableId).remove();
                ex1.remove();
            }
        } catch (PersistitException e) {
            LOG.error(TableName.create(schemaName, tableName).toString(), e);
            throw e;
        } finally {
            if (ex1 != null) {
                store.releaseExchange(ex1);
            }
            if (ex2 != null) {
                store.releaseExchange(ex2);
            }
        }
    }

}
