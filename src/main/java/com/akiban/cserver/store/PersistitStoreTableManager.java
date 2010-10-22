package com.akiban.cserver.store;

import com.akiban.cserver.RowDef;
import com.persistit.Exchange;
import com.persistit.Key;
import com.persistit.encoding.ValueRenderer;
import com.persistit.exception.PersistitException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.util.List;
import java.util.Map;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Manage tables in a PersistitStore. Maintain a map of rowDefId->TableStatus.
 * Each TableStatus object represents the most up-to-date representation of a
 * tables status. TableStatus is persisted to a backing Persistit Tree from time
 * to time, and sometimes within a transaction.
 * 
 * @author peter
 * 
 */
public class PersistitStoreTableManager {

    private static final Log LOG = LogFactory
            .getLog(PersistitStoreTableManager.class.getName());

    private final static long DELAY = 10000L;

    private final static String STATUS_TREE_NAME = "_status_";

    private final Map<Integer, TableStatus> statusMap = new ConcurrentHashMap<Integer, TableStatus>();

    private final PersistitStore store;

    private final ValueRenderer encoder = new TableStatus.PersistitEncoder();

    private final Timer timer = new Timer("TableStatus_Flusher", true);

    PersistitStoreTableManager(final PersistitStore store) {
        this.store = store;
    }

    /**
     * Load up stored TableStatus objects and then start timer to flush updates
     * periodically.
     * 
     * @throws Exception
     */
    public void startUp() throws Exception {
        store.getDb().getCoderManager().registerValueCoder(TableStatus.class,
                encoder);
        //
        // Do this in the foreground thread to throw any configuration-
        // related Exception right away.
        //
        preload();
        //
        // Schedule Timer to flush every DELAY milliseconds.
        //
        timer.schedule(new TimerTask() {

            @Override
            public void run() {
                try {
                    updateTableStatus();
                } catch (Exception e) {
                    if (LOG.isErrorEnabled()) {
                        LOG.error("Failed to updateTableState", e);
                    }
                }
            }
        }, DELAY, DELAY);
    }

    /**
     * Shut down the background Timer and then flush all TableStatus objects
     * 
     * @throws Exception
     */
    public void shutDown() throws Exception {
        timer.cancel();
        timer.purge();

        final Exchange exchange = statusExchange();
        try {
            for (final TableStatus tableStatus : statusMap.values()) {
                if (tableStatus.testIsStale()) {
                    saveStatus(exchange, tableStatus);
                }
            }
        } finally {
            store.releaseExchange(exchange);
        }
    }

    public synchronized TableStatus getTableStatus(final int rowDefId)
            throws PersistitException {
        TableStatus tableStatus = statusMap.get(rowDefId);
        if (tableStatus == null) {
            tableStatus = new TableStatus(rowDefId);
            statusMap.put(rowDefId, tableStatus);
            final Exchange exchange = statusExchange();
            loadStatus(exchange, tableStatus);
        }
        return tableStatus;
    }

    void preload() throws PersistitException {
        Exchange exchange = statusExchange();
        try {
            exchange.clear().to(Key.BEFORE);
            while (exchange.next()) {
                final int rowDefId = exchange.getKey().reset().decodeInt();
                final TableStatus tableStatus = new TableStatus(rowDefId);
                if (exchange.getValue().isDefined()) {
                    exchange.getValue().get(tableStatus);
                    statusMap.put(tableStatus.getRowDefId(), tableStatus);
                }
            }
        } finally {
            store.releaseExchange(exchange);
        }
    }

    void updateTableStatus() throws PersistitException {
        final List<RowDef> rowDefs = store.getRowDefCache().getRowDefs();
        Exchange exchange = statusExchange();
        try {
            for (final RowDef rowDef : rowDefs) {
                TableStatus tableStatus = statusMap.get(rowDef.getRowDefId());
                if (tableStatus == null) {
                    tableStatus = new TableStatus(rowDef.getRowDefId());
                    loadStatus(exchange, tableStatus);
                }
            }
            for (final TableStatus tableStatus : statusMap.values()) {
                if (tableStatus.testIsStale()) {
                    saveStatus(exchange, tableStatus);
                }
            }
        } finally {
            store.releaseExchange(exchange);
        }
    }

    public void loadStatus(final TableStatus tableStatus)
            throws PersistitException {
        Exchange exchange = statusExchange();
        try {
            loadStatus(exchange, tableStatus);
        } finally {
            store.releaseExchange(exchange);
        }
    }

    public void saveStatus(final TableStatus tableStatus)
            throws PersistitException {
        Exchange exchange = statusExchange();
        try {
            saveStatus(exchange, tableStatus);
        } finally {
            store.releaseExchange(exchange);
        }
    }

    public void deleteStatus(final TableStatus tableStatus)
            throws PersistitException {
        Exchange exchange = statusExchange();
        try {
            exchange.clear().append(tableStatus.getRowDefId());
            exchange.remove();
            statusMap.remove(tableStatus.getRowDefId());
        } finally {
            store.releaseExchange(exchange);
        }
    }

    public void deleteStatus(final int rowDefId) throws PersistitException {
        Exchange exchange = statusExchange();
        try {
            exchange.clear().append(rowDefId);
            exchange.remove();
            statusMap.remove(rowDefId);
        } finally {
            store.releaseExchange(exchange);
        }
    }

    synchronized void loadStatus(final Exchange exchange,
            final TableStatus tableStatus) throws PersistitException {
        exchange.clear().append(tableStatus.getRowDefId());
        exchange.fetch();
        if (exchange.getValue().isDefined()) {
            exchange.getValue().get(tableStatus);
        }
    }

    synchronized void saveStatus(final Exchange exchange,
            final TableStatus tableStatus) throws PersistitException {
        exchange.clear().append(tableStatus.getRowDefId());
        exchange.getValue().put(tableStatus);
        exchange.store();
        tableStatus.flushed();
    }

    private Exchange statusExchange() throws PersistitException {
        return store.getExchange(STATUS_TREE_NAME);
    }

    static long now() {
        return System.nanoTime() / 1000L;
    }
}
