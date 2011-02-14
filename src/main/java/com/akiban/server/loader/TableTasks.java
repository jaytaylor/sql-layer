/**
 * Copyright (C) 2011 Akiban Technologies Inc.
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License, version 3,
 * as published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see http://www.gnu.org/licenses.
 */

package com.akiban.server.loader;

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
        return generateFinal;
    }

    public GenerateParentTask generateParent()
    {
        return generateParent;
    }

    public GenerateChildTask generateChild()
    {
        return generateChild;
    }

    public void generateFinal(GenerateFinalTask task)
    {
        assert generateFinal == null : generateFinal;
        generateFinal = task;
    }

    public void generateParent(GenerateParentTask task)
    {
        assert generateParent == null : generateParent;
        generateParent = task;
    }

    public void generateChild(GenerateChildTask task)
    {
        assert generateChild == null : generateChild;
        generateChild = task;
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
