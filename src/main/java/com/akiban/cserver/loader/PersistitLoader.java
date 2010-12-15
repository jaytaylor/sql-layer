package com.akiban.cserver.loader;

import java.sql.ResultSet;
import java.util.List;

import com.akiban.cserver.service.session.SessionImpl;
import com.akiban.cserver.store.PersistitStore;
import com.persistit.exception.PersistitException;

public class PersistitLoader
{
    public PersistitLoader(PersistitStore store, DB db, Tracker tracker)
            throws Exception
    {
        this.store = store;
        this.db = db;
        this.tracker = tracker;
        // this.transaction = store.getDb().getTransaction();
    }

    public void load(List<GenerateFinalTask> finalTasks) throws Exception
    {
        // transaction.begin();
        DB.Connection connection = db.new Connection();
        try {
            // TODO: Merge inputs from final tasks by hkey. This would require a
            // TODO: connection per table.
            for (GenerateFinalTask task : finalTasks) {
                load(task, connection);
            }
            store.buildIndexes(new SessionImpl(), "");
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
            final PersistitAdapter persistitAdapter = new PersistitAdapter(store, task, tracker);
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
            store.flushIndexes();
            store.setDeferIndexes(deferIndexes);
        }
    }

    private static final String SQL_TEMPLATE = "select * from %s";
    private static final int LOG_INTERVAL = 10 * 1000;

    private final DB db;
    private final PersistitStore store;
    private final Tracker tracker;
    // private final Transaction transaction;
}
