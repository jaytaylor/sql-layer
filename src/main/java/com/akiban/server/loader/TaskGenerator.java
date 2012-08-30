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

import java.util.ArrayList;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.akiban.ais.model.Group;
import com.akiban.ais.model.Join;
import com.akiban.ais.model.UserTable;

/*
 * The algorithm for generating tasks is discussed here:
 * http://akibaninc.onconfluence.com/display/db/An+Akiban+Bulk+Loader
 * in the section "Algorithm for generating tasks".
 */

public class TaskGenerator
{
    public TaskGenerator(BulkLoader loader, Actions actions)
    {
        this.loader = loader;
        this.actions = actions;
    }

    public IdentityHashMap<UserTable, TableTasks> generateTasks()
            throws Exception
    {
        Map<String, Group> aisGroups = loader.ais().getGroups();
        Set<String> aisGroupNames = aisGroups.keySet();
        List<String> loadGroupNames = new ArrayList<String>(loader.groups());
        if (!(aisGroupNames.containsAll(loadGroupNames))) {
            loadGroupNames.removeAll(aisGroupNames);
            throw new RuntimeException
                    (String.format("These groups are not present in the target schema: %s", loadGroupNames));
        }
        for (String groupName : loadGroupNames) {
            Group group = aisGroups.get(groupName);
            assert group != null;
            generateTasks(group);
        }
        return tasks;
    }

    private void generateTasks(Group group) throws Exception
    {
        logger.info("Generating tasks");
        UserTable root = group.getRoot();
        actions.generateTasksForTableContainingHKeyColumns(loader, root, tasks);
        for (Join join : root.getChildJoins()) {
            generateTasks(join);
        }
    }

    private void generateTasks(Join join)
            throws Exception
    {
        UserTable childTable = join.getChild();
        if (childTable.containsOwnHKey()) {
            actions.generateTasksForTableContainingHKeyColumns(loader, childTable, tasks);
        } else {
            actions.generateTasksForTableNotContainingHKeyColumns(loader, join, tasks);
        }
        for (Join childJoin : childTable.getChildJoins()) {
            generateTasks(childJoin);
        }
    }

    private static final Logger logger = LoggerFactory.getLogger(TaskGenerator.class.getName());

    private final BulkLoader loader;
    private final Actions actions;
    private final IdentityHashMap<UserTable, TableTasks> tasks = new IdentityHashMap<UserTable, TableTasks>();

    public interface Actions
    {
        void generateTasksForTableContainingHKeyColumns(BulkLoader loader,
                                                        UserTable table,
                                                        IdentityHashMap<UserTable, TableTasks> tasks);

        void generateTasksForTableNotContainingHKeyColumns(BulkLoader loader,
                                                           Join join,
                                                           IdentityHashMap<UserTable, TableTasks> tasks)
                throws Exception;
    }
}
