/**
 * Copyright (C) 2009-2013 FoundationDB, LLC
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

package com.foundationdb.server.store;

import com.foundationdb.qp.row.Row;
import com.foundationdb.server.error.AkibanInternalException;
import com.foundationdb.server.error.QueryTimedOutException;
import com.foundationdb.server.test.it.ITBase;
import org.junit.Before;
import org.junit.Test;

import java.util.concurrent.Callable;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public class TransactionServiceIT extends ITBase
{
    private int tid = -1;

    @Before
    public void setupTable() {
        tid = createTable("test", "t", "id int not null primary key");
    }

    @Test
    public void runnableSuccess() {
        final Row row = row(tid, 1);
        txnService().run(session(), new Runnable() {
            @Override
            public void run() {
                writeRows(row);
            }
        });
        expectRows(tid, row);
    }

    @Test
    public void runnableFailure() {
        final RuntimeException ex = new RuntimeException();
        try {
            txnService().run(session(), new Runnable() {
                @Override
                public void run() {
                    writeRow(tid, 1);
                    throw ex;
                }
            });
            fail("Expected exception");
        } catch(RuntimeException e) {
            assertEquals(ex, e);
        }
        expectRowCount(tid, 0);
    }

    @Test
    public void singleRunnableRetry() {
        final Row row = row(tid, 1);
        final int[] failures = { 0 };
        txnService().run(session(), new Runnable() {
            @Override
            public void run() {
                writeRows(row);
                if(failures[0] < 1) {
                    ++failures[0];
                    throw new QueryTimedOutException(-1);
                }
            }
        });
        expectRows(tid, row);
    }

    @Test
    public void multipleRunnableRetry() {
        final Row row = row(tid, 1);
        final int failures[] = { 0 };
        txnService().run(session(), new Runnable() {
            @Override
            public void run() {
                writeRows(row);
                if(failures[0] < 2) {
                    ++failures[0];
                    throw new QueryTimedOutException(-1);
                }
            }
        });
        expectRows(tid, row);
    }

    @Test
    public void callableSuccess() {
        Row row = txnService().run(session(), new Callable<Row>() {
            @Override
            public Row call() {
                Row row = row(tid, 1);
                writeRows(row);
                return row;
            }
        });
        expectRows(tid, row);
    }

    @Test
    public void callableFailure() {
        final RuntimeException ex = new RuntimeException();
        try {
            txnService().run(session(), new Callable<Row>() {
                @Override
                public Row call() {
                    writeRow(tid, 1);
                    throw ex;
                }
            });
            fail("Expected exception");
        } catch(RuntimeException e) {
            assertEquals(ex, e);
        }
        expectRowCount(tid, 0);
    }

    @Test(expected=AkibanInternalException.class)
    public void callableThrowsException() {
        txnService().run(session(), new Callable<Void>() {
            @Override
            public Void call() throws Exception {
                throw new Exception();
            }
        });
    }

    @Test
    public void singleCallableRetry() {
        final int[] failures = { 0 };
        Row row = txnService().run(session(), new Callable<Row>() {
            @Override
            public Row call() {
                Row row = row(tid, 1);
                writeRows(row);
                if(failures[0] < 1) {
                    ++failures[0];
                    throw new QueryTimedOutException(-1);
                }
                return row;
            }
        });
        expectRows(tid, row);
    }

    @Test
    public void multipleCallableRetry() {
        final int[] failures = { 0 };
        Row row = txnService().run(session(), new Callable<Row>() {
            @Override
            public Row call() {
                Row row = row(tid, 1);
                writeRows(row);
                if(failures[0] < 2) {
                    ++failures[0];
                    throw new QueryTimedOutException(-1);
                }
                return row;
            }
        });
        expectRows(tid, row);
    }
}
