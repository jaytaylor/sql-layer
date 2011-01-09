package com.akiban.cserver.loader;

import java.io.IOException;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.IdentityHashMap;

import com.akiban.ais.model.UserTable;
import com.akiban.util.Command;

public class DataGrouper
{
    public DataGrouper(DB db, String artifactsSchema, Tracker tracker)
            throws SQLException
    {
        this.db = db;
        this.artifactsSchema = artifactsSchema;
        this.tracker = tracker;
    }

    public void run(IdentityHashMap<UserTable, TableTasks> tableTasksMap)
            throws Exception
    {
        DB.Connection connection = db.new Connection();
        try {
            saveTasks(connection, tableTasksMap);
            runTasks(connection);
        } finally {
            connection.close();
        }
    }

    public void resume() throws Exception
    {
        DB.Connection connection = db.new Connection();
        try {
            cleanupFailedTasks(connection);
            runTasks(connection);
        } finally {
            connection.close();
        }
    }

    private void cleanupFailedTasks(DB.Connection connection) throws Exception
    {
        tracker.info(String.format("Cleanup from failed tasks"));
        connection.new Query(TEMPLATE_ABANDONED_TASKS, artifactsSchema)
        {
            @Override
            protected void handleRow(ResultSet resultSet) throws Exception
            {
                int taskId = resultSet.getInt(1);
                String artifactTable = resultSet.getString(2);
                final DB.Connection cleanupConnection = db.new Connection();
                try {
                    cleanupConnection.new DDL(TEMPLATE_DROP_TABLE,
                                              artifactsSchema, artifactTable).execute();
                    cleanupConnection.new Update(TEMPLATE_RESET_TASK_STATE,
                                                 artifactsSchema, taskId).execute();
                } finally {
                    cleanupConnection.close();
                }
            }
        }.execute();
    }

    private void runTasks(DB.Connection connection) throws Exception
    {
        /*
         * Task execution order: 1. $child 2. $parent in order of increasing
         * depth 3. $final This guarantees that artifacts are created before
         * they are needed, without the need to track individual task
         * dependencies.
         */
        runTasks("child", TEMPLATE_CHILD_TASKS, connection);
        runTasks("parent", TEMPLATE_PARENT_TASKS_BY_DEPTH, connection);
        runTasks("final", TEMPLATE_FINAL_TASKS, connection);
    }

    private void saveTasks(DB.Connection connection,
                           IdentityHashMap<UserTable,
                                   TableTasks> tableTasksMap)
            throws SQLException
    {
        tracker.info("Saving tasks");
        for (TableTasks tableTasks : tableTasksMap.values()) {
            tableTasks.saveTasks(connection);
        }
    }

    private void runTasks(String label, String taskQueryTemplate,
                          DB.Connection connection) throws Exception
    {
        connection.new Query(taskQueryTemplate, artifactsSchema)
        {
            @Override
            protected void handleRow(ResultSet resultSet) throws SQLException, Command.Exception, IOException
            {
                // Need a separate connection for updates because the first
                // connection (running the tasks query),
                // is streaming results and can't support another statement at
                // the same time.
                DB.Connection updateConnection = db.new Connection();
                try {
                    int taskId = resultSet.getInt(1);
                    tracker.info("Starting task %s", taskId);
                    String command = resultSet.getString(2);
                    updateConnection.new Update(TEMPLATE_MARK_STARTED, artifactsSchema, taskId).execute();
                    long start = System.nanoTime();
                    db.spawn(command, tracker.logger());
                    long stop = System.nanoTime();
                    double timeSec = ((double) (stop - start)) / ONE_BILLION;
                    updateConnection.new Update(TEMPLATE_MARK_COMPLETED, artifactsSchema, timeSec, taskId).execute();
                    tracker.info("Finished task %s", taskId);
                } finally {
                    updateConnection.close();
                }
            }
        }.execute();
    }

    private static final String TEMPLATE_MARK_STARTED = "update %s.task "
                                                        + "set state = 'started' " + "where task_id = %s";
    private static final String TEMPLATE_MARK_COMPLETED = "update %s.task "
                                                          + "set state = 'completed', " + "    time_sec = %s "
                                                          + "where task_id = %s";
    private static final String TEMPLATE_FINAL_TASKS = "select task_id, command "
                                                       + "from %s.task "
                                                       + "where task_type like 'GenerateFinal%%' "
                                                       + "and   state = 'waiting'";
    private static final String TEMPLATE_PARENT_TASKS_BY_DEPTH = "select task_id, command "
                                                                 + "from %s.task "
                                                                 + "where task_type like 'GenerateParent%%' "
                                                                 + "and   state = 'waiting' " + "order by user_table_depth";
    private static final String TEMPLATE_CHILD_TASKS = "select task_id, command "
                                                       + "from %s.task "
                                                       + "where task_type = 'GenerateChild' "
                                                       + "and   state = 'waiting'";
    private static final String TEMPLATE_ABANDONED_TASKS = "select task_id, artifact_table "
                                                           + "from %s.task " + "where state = 'started'";
    private static final String TEMPLATE_DROP_TABLE = "drop table if exists %s.%s";
    private static final String TEMPLATE_RESET_TASK_STATE = "update %s.task "
                                                            + "set state = 'waiting' " + "where task_id = %s";
    private static final int ONE_BILLION = 1000 * 1000 * 1000;

    private final DB db;
    private final String artifactsSchema;
    private final Tracker tracker;
}
