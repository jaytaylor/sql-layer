package com.akiban.cserver.store;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;

import com.akiban.cserver.IndexDef;
import com.akiban.cserver.RowDef;
import com.akiban.cserver.RowDefCache;
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

    /**
     * Attempts to create a table.
     * @param useSchemaName the table's schema name
     * @param ddl the table's raw DDL
     * @param outTableId will be set to the table's ID, but only if this method succeeds
     * @param rowDefCache the existing RowDefCache. Used to validate parent columns.
     * @return CServerConstants.OK iff everything worked
     */
    int createTable(final String useSchemaName, final String ddl, AtomicReference<Integer> outTableId, RowDefCache rowDefCache) {
        Exchange ex = null;
        String canonical = DDLSource.canonicalStatement(ddl);
        final SchemaDef.UserTableDef tableDef;
        try {
            tableDef = new DDLSource().parseCreateTable(canonical);
        } catch (Exception e1) {
            if (LOG.isDebugEnabled()) {
                LOG.debug("Failed to parse: " + canonical + 
                        " -- " + e1.getMessage());
            }
            return ERR;
        }
        if (AKIBA_INFORMATION_SCHEMA.equals(tableDef.getCName().getSchema())) {
            return OK;
        }
        if (tableDef.getPrimaryKey().size() == 0) {
            return ERR;
        }

        SchemaDef.IndexDef parentJoin = DDLSource.getAkibanJoin(tableDef);
        if (parentJoin != null) {
            if (AKIBA_INFORMATION_SCHEMA.equals(parentJoin.getParentSchema())
                    || "akiba_objects".equals(parentJoin.getParentSchema())) {
                return ERR;
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
                LOG.warn("parent table not found: " + parentName);
                return ERR;
            }
            IndexDef parentPK = parentDef.getPKIndexDef();
            List<String> parentPKColumns = new ArrayList<String>(parentPK.getFields().length);
            for (int fieldIndex : parentPK.getFields()) {
                parentPKColumns.add( parentDef.getFieldDef(fieldIndex).getName() );
            }
            if (!parentPKColumns.equals( parentJoin.getParentColumns() )) {
                LOG.warn(String.format("column mismatch: %s%s references %s%s",
                        tableDef.getCName(), parentJoin.getParentColumns(), parentName, parentPKColumns));
                return ERR;
            }
        }

        // TODO: For now, we can't handle situations in which a tablename+columnname already exist
        //    This is because group table columns are qualified only by (uTable,uTableCol) so there
        //    is a collision if we have (s1, tbl, col) and (s2, tbl, col)
        if (!checkForDuplicateColumns(tableDef)) {
            return ERR;
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
                    return OK;
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
            
            return OK;

            // } catch (StoreException e) {
            // if (verbose && LOG.isInfoEnabled()) {
            // LOG.info("createTable error " + e.getResult(), e);
            // }
            // return e.getResult();
        } catch (Exception t) {
            LOG.error("createTable failed", t);
            return ERR;
        } finally {
            store.releaseExchange(ex);
        }
    }

    /**
     * Removes the create table statement(s) for the specified schema/table
     * 
     * @param schemaName
     * @param tableName
     * @throws Exception
     */
    int dropCreateTable(final String schemaName, final String tableName) {
        
        if (AKIBA_INFORMATION_SCHEMA.equals(schemaName)) {
            return OK;
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
            return OK;

        } catch (Exception e) {
            LOG.error("dropSchemaTable(" + schemaName + "." + tableName
                    + ") failed", e);
            return ERR;
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
