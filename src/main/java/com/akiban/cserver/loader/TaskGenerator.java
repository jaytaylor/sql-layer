package com.akiban.cserver.loader;

import java.util.ArrayList;
import java.util.IdentityHashMap;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.akiban.ais.model.Column;
import com.akiban.ais.model.Group;
import com.akiban.ais.model.Join;
import com.akiban.ais.model.JoinColumn;
import com.akiban.ais.model.UserTable;

/*
 * The algorithm for generating tasks is discussed here:
 * http://akibainc.onconfluence.com/display/db/An+Akiban+Bulk+Loader
 * in the section "Algorithm for generating tasks".
 */

public class TaskGenerator {
    public TaskGenerator(BulkLoader loader, Actions actions) {
        this.loader = loader;
        this.actions = actions;
    }

    public IdentityHashMap<UserTable, TableTasks> generateTasks()
            throws Exception {
        for (Group group : loader.ais().getGroups().values()) {
            if (loader.groups().contains(group.getName())) {
                generateTasks(group);
            }
        }
        return tasks;
    }

    private void generateTasks(Group group) throws Exception {
        logger.info("Generating tasks");
        UserTable root = group.getGroupTable().getRoot();
        actions.generateTasksForTableContainingHKeyColumns(loader, root, tasks);
        for (Join join : root.getChildJoins()) {
            // Compute initial hKeyColumns
            List<Column> hKeyColumns = new ArrayList<Column>();
            for (JoinColumn joinColumn : join.getJoinColumns()) {
                hKeyColumns.add(joinColumn.getParent());
            }
            generateTasks(hKeyColumns, join);
        }
    }

    private void generateTasks(List<Column> hKeyColumns, Join join)
            throws Exception {
        List<Column> childHKeyColumns = Task.columnsInChild(hKeyColumns, join);
        UserTable childTable = join.getChild();
        if (hKeyColumns.size() > 0
                && childHKeyColumns.size() == hKeyColumns.size()) {
            // Every hkey column in the parent has a counterpart in the child
            // via the join. So the child table
            // has a complete hkey.
            actions.generateTasksForTableContainingHKeyColumns(loader,
                    childTable, tasks);
            for (Join childJoin : childTable.getChildJoins()) {
                // Extend childHKeyColumns with child key columns not already
                // present
                List<Column> extendedChildHKeyColumns = new ArrayList<Column>(
                        childHKeyColumns);
                for (JoinColumn childJoinColumn : childJoin.getJoinColumns()) {
                    Column childJoinColumnParent = childJoinColumn.getParent();
                    if (!extendedChildHKeyColumns
                            .contains(childJoinColumnParent)) {
                        extendedChildHKeyColumns.add(childJoinColumnParent);
                    }
                }
                generateTasks(extendedChildHKeyColumns, childJoin);
            }
        } else {
            actions.generateTasksForTableNotContainingHKeyColumns(loader, join,
                    tasks);
            childHKeyColumns.clear(); // Indicates that child tables (and
            // descendents) don't have a complete
            // hkey.
            for (Join childJoin : childTable.getChildJoins()) {
                generateTasks(childHKeyColumns, childJoin);
            }
        }
    }

    private static final Log logger = LogFactory.getLog(TaskGenerator.class
            .getName());

    private final BulkLoader loader;
    private final Actions actions;
    private final IdentityHashMap<UserTable, TableTasks> tasks = new IdentityHashMap<UserTable, TableTasks>();

    public interface Actions {
        void generateTasksForTableContainingHKeyColumns(BulkLoader loader,
                UserTable table, IdentityHashMap<UserTable, TableTasks> tasks);

        void generateTasksForTableNotContainingHKeyColumns(BulkLoader loader,
                Join join, IdentityHashMap<UserTable, TableTasks> tasks)
                throws Exception;
    }
}
