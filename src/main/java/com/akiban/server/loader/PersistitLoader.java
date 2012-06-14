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

package com.akiban.server.loader;

import java.sql.ResultSet;
import java.util.Collections;
import java.util.List;

import com.akiban.server.service.session.Session;
import com.akiban.server.service.session.SessionService;
import com.akiban.server.store.PersistitStore;
import com.persistit.exception.PersistitException;

public class PersistitLoader
{
    private final static boolean BUILD_INDEXES_DEFERRED = true;
    
    public PersistitLoader(PersistitStore store, DB db, Tracker tracker, SessionService sessionService)
            throws Exception
    {
        this.store = store;
        this.db = db;
        this.tracker = tracker;
        this.sessionService = sessionService;
        // this.transaction = store.getDb().getTransaction();
    }

    public void load(List<GenerateFinalTask> finalTasks) throws Exception
    {
        // transaction.begin();
        DB.Connection connection = db.new Connection();

        try {
        	Collections.sort(finalTasks, new TaskComparator());
        } catch (Exception e) {
        	tracker.error("Caught exception while sorting finalTasks", e);
        }

        Session session = sessionService.createSession();
        try {
            // TODO: Merge inputs from final tasks by hkey. This would require a
            // TODO: connection per table.
            for (GenerateFinalTask task : finalTasks) {
                load(task, connection);
            }
            
            store.buildAllIndexes(session, BUILD_INDEXES_DEFERRED);
            // transaction.commit();
        } catch (PersistitException e) {
            tracker.error("Caught exception while loading persistit", e);
/*
            try {
                transaction.rollback();
            } catch (PersistitException rollbackException) {
                tracker.error("Caught exception while rolling back following earlier failure", rollbackException);
            }
            throw e;
*/
        } finally {
            session.close();
            // transaction.end();
        }
    }

    private void load(final GenerateFinalTask task, DB.Connection connection)
            throws Exception
    {
        tracker.info(String.format("Loading persistit for %s", task.artifactTableName()));
        boolean deferIndexes = store.isDeferIndexes();
        store.setDeferIndexes(true);
        try {
            final PersistitAdapter persistitAdapter = new PersistitAdapter(store, task, tracker, sessionService);
            final int[] count = new int[1];
            connection.new Query(SQL_TEMPLATE, task.artifactTableName())
            {
                @Override
                protected void handleRow(ResultSet resultSet) throws Exception
                {
                    persistitAdapter.handleRow(resultSet);
                    count[0]++;
                    if (count[0] % LOG_INTERVAL == 0) {
                        tracker.info("%s: %s", task.artifactTableName(), count[0]);
                    }
                }
            }.execute();
            persistitAdapter.close();
            tracker.info(String.format("Loaded persistit for %s: %s rows", task.artifactTableName(), count[0]));
        } finally {
            store.setDeferIndexes(deferIndexes);
        }
    }

    private static final String SQL_TEMPLATE = "select * from %s";
    private static final int LOG_INTERVAL = 10 * 1000;

    private final DB db;
    private final PersistitStore store;
    private final Tracker tracker;
    private final SessionService sessionService;
    // private final Transaction transaction;
}
