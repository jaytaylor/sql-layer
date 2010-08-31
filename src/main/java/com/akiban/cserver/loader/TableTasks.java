package com.akiban.cserver.loader;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

public class TableTasks
{
    public void saveTasks(DB.Connection connection) throws SQLException
    {
        for (Task task : tasks()) {
            connection.new Update(TEMPLATE_SAVE_TASK, 
                                  task.artifactTableName().getSchemaName(),
                                  task.type(),
                                  task.table().getName().getSchemaName(),
                                  task.table().getName().getTableName(),
                                  task.table().getDepth(),
                                  task.artifactTableName().getSchemaName(),
                                  task.artifactTableName().getTableName(),
                                  task.sql()
            ).execute();
        }
    }

    public GenerateFinalTask generateFinal()
    {
        return this.generateFinal;
    }

    public GenerateParentTask generateParent()
    {
        return this.generateParent;
    }

    public GenerateChildTask generateChild()
    {
        return this.generateChild;
    }

    public void generateFinal(GenerateFinalTask task)
    {
        this.generateFinal = task;
    }

    public void generateParent(GenerateParentTask task)
    {
        this.generateParent = task;
    }

    public void generateChild(GenerateChildTask task)
    {
        this.generateChild = task;
    }

    private List<Task> tasks()
    {
        List<Task> tasks = new ArrayList<Task>();
        if (generateFinal != null) {
            tasks.add(generateFinal);
        }
        if (generateParent != null) {
            tasks.add(generateParent);
        }
        if (generateChild != null) {
            tasks.add(generateChild);
        }
        return tasks;
    }

    private static final String TEMPLATE_SAVE_TASK = "insert into %s.task("
                                                     + "    task_type, " + "    state, " + "    user_table_schema, "
                                                     + "    user_table_table, " + "    user_table_depth, "
                                                     + "    artifact_schema, " + "    artifact_table, " + "    command "
                                                     + ") values(" + "    '%s'," + "    'waiting'," + "    '%s',"
                                                     + "    '%s'," + "    '%s'," + "    '%s'," + "    '%s',"
                                                     + "    '%s'" + ")";

    private GenerateFinalTask generateFinal;
    private GenerateParentTask generateParent;
    private GenerateChildTask generateChild;
}
