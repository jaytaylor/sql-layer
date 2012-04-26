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

package com.akiban.server.test.it.keyupdate;

import com.akiban.ais.model.GroupIndex;
import com.akiban.ais.model.Index;
import com.akiban.ais.model.Index.JoinType;
import com.akiban.ais.model.TableIndex;
import com.akiban.ais.model.UserTable;
import com.akiban.server.AccumulatorAdapter;
import com.akiban.server.AccumulatorAdapter.AccumInfo;
import com.akiban.server.error.PersistitAdapterException;
import com.akiban.server.service.dxl.IndexCheckResult;
import com.akiban.server.service.dxl.IndexCheckSummary;
import com.akiban.server.store.PersistitStore;
import com.akiban.server.store.statistics.IndexStatisticsService;
import com.akiban.server.test.it.ITBase;
import com.persistit.Exchange;
import com.persistit.exception.PersistitInterruptedException;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;

import static org.junit.Assert.assertEquals;

public final class FixCountStarIT extends ITBase {
    @Before
    public void createDb() {
        int pTable = createTable("idx_count", "p", "id int not null primary key, pname varchar(32)");
        int cTable = createTable("idx_count", "c", "id int not null primary key, pid int, cname varchar(32)",
                akibanFK("pid", "p", "id"));
        gi = createGroupIndex(getUserTable(pTable).getGroup().getName(), "pc_left", "p.pname, c.cname", JoinType.LEFT);
        pnameIdx = createIndex("idx_count", "p", "pname", "pname");
        cnameIdx = createIndex("idx_count", "c", "cname", "cname");

        writeRow(pTable, 1, "p one");
        writeRow(cTable, 10, 1, "c one");
        writeRow(cTable, 11, 2, "c two");
        checkRowCounts();
    }

    @After
    public void checkRowCounts() {
        checkIndexCount(gi, 1);
        checkIndexCount(pnameIdx, 1);
        checkIndexCount(cnameIdx, 2);
    }

    @Test
    public void fixTableIndex() {
        setIndexCount(cnameIdx, 31);
        checkIndexCount(cnameIdx, 31);
        IndexCheckSummary actual =  dxl().ddlFunctions().checkAndFixIndexes(session(), "idx_count", "c");
        IndexCheckSummary expected = new IndexCheckBuilder()
                .add(cnameIdx, 31, 2, 2)
                .add(gi, 1, 1, 1)
                .get();
        assertEquals("index fix summary", expected, actual);
    }

    @Test
    public void fixGroupIndex() {
        setIndexCount(gi, 31);
        checkIndexCount(gi, 31);
        IndexCheckSummary actual =  dxl().ddlFunctions().checkAndFixIndexes(session(), "idx_count", "c");
        IndexCheckSummary expected = new IndexCheckBuilder()
                .add(cnameIdx, 2, 2, 2)
                .add(gi, 31, 1, 1)
                .get();
        assertEquals("index fix summary", expected, actual);
    }

    private void setIndexCount(final Index index, final long newVal) {
        transactionallyUnchecked(new Runnable() {
            @Override
            public void run() {
                if (index.isTableIndex()) {
                    TableIndex tIndex = (TableIndex) index;
                    store().getPersistitStore().getTableStatus(tIndex.getTable()).setRowCount(newVal);
                }
                else {
                    PersistitStore store = store().getPersistitStore();
                    final Exchange ex = store.getExchange(session(), index);
                    try {
                        new AccumulatorAdapter(AccumInfo.ROW_COUNT, treeService(), ex.getTree()).set(newVal);
                    }
                    catch (PersistitInterruptedException e) {
                        throw new PersistitAdapterException(e);
                    }
                    finally {
                        store.releaseExchange(session(), ex);
                    }
                }
            }
        });
    }

    private void checkIndexCount(final Index index, long expectedCount) {
        final IndexStatisticsService iss = serviceManager().getServiceByClass(IndexStatisticsService.class);
        long actualCount = transactionallyUnchecked(new Callable<Long>() {
            @Override
            public Long call() throws Exception {
                return iss.countEntries(session(), index);
            }
        });
        assertEquals(String.valueOf(index), expectedCount, actualCount);
    }

    private static class IndexCheckBuilder {
        private List<IndexCheckResult> results = new ArrayList<IndexCheckResult>();

        public IndexCheckBuilder add(Index index, long expectedCount, long sawCount, long verifiedCount) {
            if (index.isTableIndex())
                index = ((UserTable)index.leafMostTable()).getPrimaryKey().getIndex();
            IndexCheckResult entry = new IndexCheckResult(index.getIndexName(), expectedCount, sawCount, verifiedCount);
            results.add(entry);
            return this;
        }

        public IndexCheckSummary get() {
            return new IndexCheckSummary(results, 0);
        }
    }

    private GroupIndex gi;
    private TableIndex pnameIdx;
    private TableIndex cnameIdx;
}
