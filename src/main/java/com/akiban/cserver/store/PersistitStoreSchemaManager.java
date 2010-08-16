package com.akiban.cserver.store;

import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

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
    
    private final AtomicLong schemaGeneration = new AtomicLong();

    public PersistitStoreSchemaManager(final PersistitStore store) {
        this.store = store;
    }

    long getCurrentSchemaGeneration() {
        return schemaGeneration.get();
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

    int createTable(final String useSchemaName, final String ddl) {
        Exchange ex = null;
        String canonical = DDLSource.canonicalStatement(ddl);
        final SchemaDef.UserTableDef tableDef;
        try {
            tableDef = new DDLSource().parseCreateTable(canonical);
        } catch (Exception e1) {
            System.err.println("Failed to parse: " + canonical);
            System.err.println(e1.getMessage());
            return ERR;
        }
        if (AKIBA_INFORMATION_SCHEMA.equals(tableDef.getCName().getSchema())) {
            return OK;
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
            ex.getValue().put(canonical);
            ex.clear().append(BY_ID).append(tableId).store();
            ex.getValue().putNull();
            ex.clear().append(BY_NAME).append(schemaName).append(tableName)
                    .append(tableId).store();

            schemaGeneration.incrementAndGet();
            
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
        
        schemaGeneration.incrementAndGet();
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
