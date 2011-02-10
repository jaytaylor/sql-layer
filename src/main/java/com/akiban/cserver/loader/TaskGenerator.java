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

package com.akiban.cserver.loader;

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
