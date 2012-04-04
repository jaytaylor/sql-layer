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

import java.util.IdentityHashMap;

import com.akiban.ais.model.AkibanInformationSchema;
import com.akiban.ais.model.Join;
import com.akiban.ais.model.UserTable;

public class MySQLTaskGeneratorActions implements TaskGenerator.Actions
{
    // TaskGenerator.Actions interface

    @Override
    public void generateTasksForTableContainingHKeyColumns(BulkLoader loader,
                                                           UserTable table,
                                                           IdentityHashMap<UserTable, TableTasks> tasks)
    {
        TableTasks tableTasks = tableTasks(tasks, table);
        if (tableTasks.generateParent() != null) {
            // Shouldn't discover the need for this task twice
            throw new BulkLoader.InternalError(table.toString());
        }
        GenerateFinalBySortTask task = new GenerateFinalBySortTask(loader, table);
        tableTasks.generateFinal(task);
        tasks.put(table, tableTasks);
    }

    @Override
    public void generateTasksForTableNotContainingHKeyColumns(
            BulkLoader loader, Join join,
            IdentityHashMap<UserTable, TableTasks> tasks) throws Exception
    {
        // Generate parent's $parent
        UserTable parent = join.getParent();
        TableTasks parentTasks = tableTasks(tasks, parent);
        if (parentTasks.generateParent() == null) {
            parentTasks.generateParent(new GenerateParentBySortTask(loader, parent));
        }
        // Generate child's $child
        UserTable child = join.getChild();
        TableTasks childTasks = tableTasks(tasks, child);
        if (childTasks.generateChild() == null) {
            childTasks.generateChild(new GenerateChildTask(loader, child));
        }
        // Generate child's $parent
        if (childTasks.generateParent() == null) {
            childTasks.generateParent(new GenerateParentByMergeTask(loader,
                                                                    child,
                                                                    parentTasks.generateParent(),
                                                                    childTasks.generateChild(),
                                                                    ais));
        }
        // Generate parent's $final
        if (parentTasks.generateFinal() == null) {
            parentTasks.generateFinal(new GenerateFinalByMergeTask(loader,
                                                                   parent,
                                                                   parentTasks.generateParent(),
                                                                   ais));
        }
        // Generate child's $final
        if (childTasks.generateFinal() == null) {
            childTasks.generateFinal(new GenerateFinalByMergeTask(loader,
                                                                  child,
                                                                  childTasks.generateParent(),
                                                                  ais));
        }
    }

    // MySQLTaskGeneratorActions

    public MySQLTaskGeneratorActions(AkibanInformationSchema ais)
    {
        this.ais = ais;
    }

    // For use by this class

    private static TableTasks tableTasks(
            IdentityHashMap<UserTable, TableTasks> tasks, UserTable table)
    {
        TableTasks tableTasks = tasks.get(table);
        if (tableTasks == null) {
            tableTasks = new TableTasks();
            tasks.put(table, tableTasks);
        }
        return tableTasks;
    }

    // State

    private final AkibanInformationSchema ais;
}
