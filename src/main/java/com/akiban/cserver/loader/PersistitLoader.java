package com.akiban.cserver.loader;

import com.akiban.ais.model.AkibaInformationSchema;
import com.akiban.cserver.RowDefCache;
import com.akiban.cserver.store.PersistitStore;
import com.persistit.Transaction;
import com.persistit.exception.PersistitException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.sql.ResultSet;
import java.util.List;

public class PersistitLoader
{
    public PersistitLoader(PersistitStore store, DB db, AkibaInformationSchema ais) throws Exception
    {
        this.store = store;
        this.db = db;
        transaction = store.getDb().getTransaction();
    }

    public void load(List<GenerateFinalTask> finalTasks) throws Exception
    {
        transaction.begin();
        DB.Connection connection = db.new Connection();
        try {
            // TODO: Merge inputs from final tasks by hkey. This would require a connection per table.
            for (GenerateFinalTask task : finalTasks) {
                load(task, connection);
            }
            transaction.commit(true);
        } catch (PersistitException e) {
            try {
                transaction.rollback();
            } catch (PersistitException rollbackException) {
                logger.error("Caught exception while rolling back following earlier failure", rollbackException);
            }
            throw e;
        } finally {
            transaction.end();
        }
    }

    private void load(GenerateFinalTask task, DB.Connection connection) throws Exception
    {
        logger.info(String.format("Loading persistit for %s", task.artifactTableName()));
        final PersistitAdapter persistitAdapter = new PersistitAdapter(store, task);
        connection.new Query(SQL_TEMPLATE, task.artifactTableName())
        {
            @Override
            protected void handleRow(ResultSet resultSet) throws Exception
            {
                persistitAdapter.handleRow(resultSet);
            }
        }.execute();
        persistitAdapter.close();
    }

    private static final Log logger = LogFactory.getLog(PersistitLoader.class.getName());
    private static final String SQL_TEMPLATE = "select * from %s";

    private final DB db;
    private final PersistitStore store;
    private Transaction transaction;
}
