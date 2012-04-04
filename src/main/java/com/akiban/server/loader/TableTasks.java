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
