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

package com.akiban.server.store.statistics;

import com.akiban.ais.model.IndexColumn;
import com.akiban.qp.persistitadapter.PersistitAdapter;
import com.akiban.qp.persistitadapter.TempVolume;
import com.akiban.server.service.session.Session;
import com.akiban.server.service.tree.KeyCreator;
import com.akiban.server.store.PersistitStore;
import com.akiban.util.tap.InOutTap;
import com.akiban.util.tap.Tap;
import com.persistit.Exchange;
import com.persistit.Key;
import com.persistit.Value;
import com.persistit.exception.PersistitException;

import java.util.concurrent.atomic.AtomicInteger;

public class SingleColumnIndexStatisticsVisitor
{
    public void init()
    {
        exchange = TempVolume.takeExchange(store, session, treeName);
    }

    public void finish()
    {
        TempVolume.returnExchange(session, exchange);
    }

    public void visit(Key key, Value value)
    {
        key.indexTo(field);
        exchange.clear().getKey().appendKeySegment(key);
        try {
            if (exchange.traverse(Key.Direction.EQ, false)) {
                exchange.fetch();
                value = exchange.getValue();
                value.put(value.getInt() + 1);
            } else {
                // First occurrence of key
                exchange.clear().getKey().appendKeySegment(key);
                exchange.getValue().put(1);
            }
            exchange.store();
        } catch (PersistitException e) {
            PersistitAdapter.handlePersistitException(session, e);
        }
    }

    public SingleColumnIndexStatisticsVisitor(PersistitStore store,
                                              Session session,
                                              IndexColumn indexColumn,
                                              long indexRowCount,
                                              KeyCreator keyCreator)
    {
        this.store = store;
        this.session = session;
        this.indexColumn = indexColumn;
        this.field = indexColumn.getPosition();
        this.timestamp = System.currentTimeMillis();
        this.treeName = String.format("tempHistogram_%s_%s", counter.getAndIncrement(), timestamp);
        this.rowCount = 0;
    }

    private static InOutTap SINGLE_COLUMN_HISTOGRAM_LOAD_TAP = Tap.createTimer("singleColumnHistogram: load");
    private static final AtomicInteger counter = new AtomicInteger(0);

    private final PersistitStore store;
    private final Session session;
    private final IndexColumn indexColumn;
    private final int field;
    private final long timestamp;
    private final String treeName;
    private int rowCount;
    private Exchange exchange;
}
