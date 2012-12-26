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

import com.akiban.ais.model.Index;
import com.akiban.server.service.session.Session;
import com.akiban.server.service.tree.KeyCreator;
import com.akiban.server.store.IndexVisitor;
import com.akiban.server.store.PersistitStore;
import com.persistit.Key;
import com.persistit.Value;

import java.util.ArrayList;
import java.util.List;

public class PersistitIndexStatisticsVisitor extends IndexVisitor
{
    public PersistitIndexStatisticsVisitor(PersistitStore store,
                                           Session session,
                                           Index index,
                                           long indexRowCount,
                                           KeyCreator keyCreator)
    {
        this.multiColumnVisitor = new MultiColumnIndexStatisticsVisitor(index, indexRowCount, keyCreator);
        this.singleColumnVisitors = new ArrayList<SingleColumnIndexStatisticsVisitor>();
        this.nIndexColumns = index.getKeyColumns().size();
        for (int f = 0; f < nIndexColumns; f++) {
            SingleColumnIndexStatisticsVisitor singleColumnVisitor =
                new SingleColumnIndexStatisticsVisitor(store,
                                                       session,
                                                       index.getKeyColumns().get(f),
                                                       indexRowCount,
                                                       keyCreator);
            singleColumnVisitors.add(singleColumnVisitor);
        }
    }

    public void init()
    {
        multiColumnVisitor.init();
        for (int c = 0; c < nIndexColumns; c++) {
            singleColumnVisitors.get(c).init();
        }
    }

    public void finish()
    {
        multiColumnVisitor.finish();
        for (int c = 0; c < nIndexColumns; c++) {
            singleColumnVisitors.get(c).finish();
        }
    }

    protected void visit(Key key, Value value)
    {
        multiColumnVisitor.visit(key, value);
        for (int c = 0; c < nIndexColumns; c++) {
            singleColumnVisitors.get(c).visit(key, value);
        }
    }

    public IndexStatistics getIndexStatistics()
    {
        return multiColumnVisitor.getIndexStatistics();
    }

    public static final int BUCKETS_COUNT = 32;

    private final MultiColumnIndexStatisticsVisitor multiColumnVisitor;
    private final List<SingleColumnIndexStatisticsVisitor> singleColumnVisitors;
    private final int nIndexColumns;
}
