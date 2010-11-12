package com.akiban.cserver.loader;

import com.akiban.ais.model.*;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.util.*;

/*
 * The algorithm for generating tasks is discussed here:
 * http://akibainc.onconfluence.com/display/db/An+Akiban+Bulk+Loader
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
        UserTable root = group.getGroupTable().getRoot();
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

    private static final Log logger = LogFactory.getLog(TaskGenerator.class.getName());

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
