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

package com.akiban.sql.optimizer.rule;

import static com.akiban.sql.optimizer.rule.IndexPicker.TableJoinsFinder;

import com.akiban.sql.optimizer.plan.*;

import com.akiban.server.error.UnsupportedSQLException;

import java.util.*;

/** Having sources table groups from indexes, get rows with XxxLookup
 * and join them together with Flatten, etc. */
public class BranchJoiner extends BaseRule
{
    @Override
    public void apply(PlanContext planContext) {
        List<TableJoins> groups = new TableJoinsFinder().find(planContext.getPlan());
        for (TableJoins tableJoins : groups) {
            joinBranches(tableJoins);
        }
    }

    protected void joinBranches(TableJoins tableJoins) {
        PlanNode scan = tableJoins.getScan();
        IndexScan index = null;
        TableSource indexTable = null;
        if (scan instanceof IndexScan) {
            index = (IndexScan)scan;
            indexTable = index.getLeafMostTable();
        }
        if (index == null) {
            scan = new Flatten(scan, trimJoins(tableJoins.getJoins(), null));
        }
        else if (!index.isCovering()) {
            List<TableSource> ancestors = new ArrayList<TableSource>();
            TableSource branch = null;
            for (TableSource table : index.getRequiredTables()) {
                if (ancestorOf(table, indexTable)) {
                    ancestors.add(table);
                }
                else if (ancestorOf(indexTable, table)) {
                    if (branch == null)
                        branch = indexTable;
                }
                else if ((branch == null) ||
                         ancestorOf(table, branch)) {
                    branch = table;
                }
                else if (!ancestorOf(branch, table)) {
                    throw new UnsupportedSQLException("Sibling branches: " + table + 
                                                      " and " + branch,
                                                      null);
                }
            }
            if (branch == indexTable) {
                ancestors.remove(indexTable);
                scan = new BranchLookup(scan, indexTable, branch);
                branch = null;
            }
            if (!ancestors.isEmpty()) {
                // Access in stable order.
                Collections.sort(ancestors,
                                 new Comparator<TableSource>() {
                                     public int compare(TableSource t1, TableSource t2) {
                                         return t1.getTable().getTable().getTableId().compareTo(t2.getTable().getTable().getTableId());
                                     }
                                 });
                scan = new AncestorLookup(scan, indexTable, ancestors);
            }
            if (branch != null) {
                scan = new BranchLookup(scan, 
                                        (ancestors.isEmpty() ? 
                                         indexTable : 
                                         ancestors.get(0)), 
                                        branch);
            }
            Joinable requiredJoins = trimJoins(tableJoins.getJoins(), 
                                               index.getRequiredTables());
            if (requiredJoins != null)
                scan = new Flatten(scan, requiredJoins);
        }
        // TODO: Can now prepone some of the conditions before the flatten.
        // TODO: Better to keep the tableJoins and just replace the
        // inside? Do we need its state any more?
        tableJoins.getOutput().replaceInput(tableJoins, scan);
    }
    
    protected Joinable trimJoins(Joinable joinable, Set<TableSource> requiredTables) {
        if (joinable instanceof TableSource) {
            if ((requiredTables == null) || requiredTables.contains(joinable))
                return joinable;
            else
                return null;
        }
        else if (joinable instanceof JoinNode) {
            JoinNode join = (JoinNode)joinable;
            removeGroupJoin(join);
            Joinable oleft = join.getLeft();
            Joinable oright = join.getRight();
            Joinable nleft = trimJoins(oleft, requiredTables);
            Joinable nright = trimJoins(oright, requiredTables);
            if ((oleft == nleft) && (oright == nright))
                return joinable;
            else if (nleft == null)
                return nright;
            else if (nright == null)
                return nleft;
            else {
                join.setLeft(nleft);
                join.setRight(nright);
                return join;
            }
        }
        else
            return joinable;
    }

    // We only want joins for their flattening pattern.
    // Make explain output more obvious by removing group join traces.
    // TODO: Also rejecting non-group joins; those could be supported with Select.
    protected void removeGroupJoin(JoinNode join) {
        List<ConditionExpression> conditions = join.getJoinConditions();
        int i = 0;
        while (i < conditions.size()) {
            ConditionExpression cond = conditions.get(i);
            if (cond.getImplementation() == 
                ConditionExpression.Implementation.GROUP_JOIN) {
                conditions.remove(i);
            }
            else {
                i++;
            }
        }
        if (!conditions.isEmpty())
            throw new UnsupportedSQLException("Non group join",
                                              conditions.get(0).getSQLsource());
        join.setGroupJoin(null);
    }

    /** Is <code>t1</code> an ancestor of <code>t2</code>? */
    protected boolean ancestorOf(TableSource t1, TableSource t2) {
        do {
            if (t1 == t2) return true;
            t2 = t2.getParentTable();
        } while (t2 != null);
        return false;
    }

}
