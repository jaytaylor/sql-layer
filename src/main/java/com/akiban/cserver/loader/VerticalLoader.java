package com.akiban.cserver.loader;

import com.akiban.ais.model.AkibaInformationSchema;
import com.akiban.cserver.RowDefCache;
import com.akiban.cserver.store.PersistitStore;
import com.akiban.cserver.store.VStore;
import com.persistit.Transaction;
import com.persistit.exception.PersistitException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.sql.ResultSet;
import java.util.List;

public class VerticalLoader
{
    public VerticalLoader(VStore store, DB db, AkibaInformationSchema ais) throws Exception
    {
        this.store = store;
        this.db = db;
    }

    public void load(List<GenerateFinalTask> finalTasks) throws Exception
    {
        DB.Connection connection = db.new Connection();
        try {
            for (GenerateFinalTask task : finalTasks) {
                load(task, connection);
            }
        } catch (PersistitException e) {
            throw e;
        } 
    }

    private void load(GenerateFinalTask task, DB.Connection connection) throws Exception
    {
        logger.info(String.format("Loading verticals for %s", task.artifactTableName()));
        final VerticalAdapter verticalAdapter = new VerticalAdapter(store, task);
        connection.new Query(SQL_TEMPLATE, task.artifactTableName())
        {
            @Override
            protected void handleRow(ResultSet resultSet) throws Exception
            {
                verticalAdapter.handleRow(resultSet);
            }
        }.execute();
        verticalAdapter.close();
    }

    private static final Log logger = LogFactory.getLog(VerticalLoader.class.getName());
    private static final String SQL_TEMPLATE = "select * from %s";

    private final DB db;
    private final VStore store;
}
