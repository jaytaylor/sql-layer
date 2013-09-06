/**
 * Copyright (C) 2009-2013 FoundationDB, LLC
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

package com.foundationdb.sql.optimizer.rule;

import com.foundationdb.sql.optimizer.plan.*;

import com.foundationdb.ais.model.Column;
import com.foundationdb.ais.model.IndexColumn;
import com.foundationdb.ais.model.Join;
import com.foundationdb.ais.model.JoinColumn;

import com.foundationdb.sql.optimizer.plan.BaseUpdateStatement.StatementType;
import com.foundationdb.sql.optimizer.plan.UpdateStatement.UpdateColumn;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

/** Identify queries that are susceptible to the Halloween problem.
 * <ul>
 * <li>Updating a primary or grouping foreign key, 
 * which can change hkeys and so group navigation.</li>
 * <li>Updating a field of an index that is scanned.</li>
 * <li>A <em>second</em> access to the target table.</li>
 * </ul>
 */
public class HalloweenRecognizer extends BaseRule
{
    private static final Logger logger = LoggerFactory.getLogger(HalloweenRecognizer.class);

    @Override
    protected Logger getLogger() {
        return logger;
    }

    @Override
    public void apply(PlanContext plan) {
        if (plan.getPlan() instanceof DMLStatement) {
            DMLStatement stmt = (DMLStatement)plan.getPlan();
            TableNode targetTable = stmt.getTargetTable();
            boolean indexWasUnique = false;
            boolean bufferRequired = false;
            Set<Column> updateColumns = null;
            UpdateStatement updateStmt = null;

            if (stmt.getType() == StatementType.UPDATE) {
                updateStmt = findBaseUpdateStatement(stmt, UpdateStatement.class);
                updateColumns = new HashSet<>();
                update: {
                    for (UpdateStatement.UpdateColumn updateColumn : updateStmt.getUpdateColumns()) {
                        updateColumns.add(updateColumn.getColumn());
                    }
                    for (Column pkColumn : targetTable.getTable().getPrimaryKeyIncludingInternal().getColumns()) {
                        if (updateColumns.contains(pkColumn)) {
                            indexWasUnique = true;
                            break update;
                        }
                    }
                    Join parentJoin = targetTable.getTable().getParentJoin();
                    if (parentJoin != null) {
                        for (JoinColumn joinColumn : parentJoin.getJoinColumns()) {
                            if (updateColumns.contains(joinColumn.getChild())) {
                                indexWasUnique = true;
                                break update;
                            }
                        }
                    }
                }
                bufferRequired = indexWasUnique;
            }

            if (!bufferRequired) {
                Checker checker = new Checker(targetTable,
                                              (stmt.getType() == StatementType.INSERT) ? 0 : 1,
                                              updateColumns);
                checker.check(stmt.getInput());
                bufferRequired = checker.bufferRequired;
                indexWasUnique = checker.indexWasUnique;
            }

            if(bufferRequired) {
                switch(stmt.getType()) {
                    case INSERT:
                    case DELETE:
                        injectBufferNode(stmt);
                        break;
                    case UPDATE:
                        if(indexWasUnique) {
                            DMLStatement newDML = transformUpdate(stmt, updateStmt);
                            plan.setPlan(newDML);
                        } else {
                            injectBufferNode(stmt);
                        }
                        break;
                    default:
                        assert false : stmt.getType();
                }
            }
        }
    }

    private static <T extends BaseUpdateStatement> T findBaseUpdateStatement(DMLStatement stmt, Class<T> clazz) {
        BasePlanWithInput node = stmt;
        do {
            node = (BasePlanWithInput)node.getInput();
        } while (node != null && !clazz.isInstance(node));
        assert node != null;
        return clazz.cast(node);
    }

    /** Convert {@link UpdateStatement#getUpdateColumns()} to a list of expressions suitable for Project */
    private static List<ExpressionNode> updateListToProjectList(UpdateStatement update, TableSource tableSource) {
        List<ExpressionNode> projects = new ArrayList<>();
        for(Column c : update.getTargetTable().getTable().getColumns()) {
            projects.add(new ColumnExpression(tableSource, c));
        }
        for(UpdateColumn updateCol : update.getUpdateColumns()) {
            projects.set(updateCol.getColumn().getPosition(), updateCol.getExpression());
        }
        return projects;
    }

    /**
     * Transform
     * <pre>Out() <- Update(col=5) <- In()</pre>
     * to
     * <pre>Out() <- Insert() <- Project(col=5) <- Buffer() <- Delete() <- In()</pre>
     */
    private static DMLStatement transformUpdate(DMLStatement dml, UpdateStatement update) {
        assert dml.getOutput() == null;

        TableSource tableSource = dml.getSelectTable();
        PlanNode scanSource = update.getInput();
        PlanWithInput updateDest = update.getOutput();

        InsertStatement insert = new InsertStatement(
            new Project(
                new Buffer(
                    new DeleteStatement(scanSource, update.getTargetTable(), tableSource)
                ),
                updateListToProjectList(update, tableSource)
            ),
            update.getTargetTable(),
            tableSource.getTable().getTable().getColumns(),
            tableSource
        );

        scanSource.setOutput(insert);
        updateDest.replaceInput(update, insert);

        return new DMLStatement(insert, StatementType.INSERT, dml.getSelectTable(), dml.getTargetTable(),
                                dml.getResultField(), dml.getReturningTable(), dml.getColumnEquivalencies());
    }

    /**
     * Add a buffer below the top level I/U/D.
     * <pre>
     * Insert/Update/Delete()
     *   Scan/Map/Etc()
     * </pre>
     * to
     * <pre>
     * Insert/Update/Delete()
     *   Buffer()
     *     Scan/Map/Etc()
     * </pre>
     */
    private static void injectBufferNode(DMLStatement dml) {
        PlanNode node = findBaseUpdateStatement(dml, BaseUpdateStatement.class).getInput();
        // Push it below a Project to avoid excess pack and unpack
        if(node instanceof Project) {
            node = ((Project)node).getInput();
        }
        PlanWithInput origDest = node.getOutput();
        PlanWithInput newInput = new Buffer(node);
        origDest.replaceInput(node, newInput);
    }

    static class Checker implements PlanVisitor, ExpressionVisitor {
        private final TableNode targetTable;
        private final Set<Column> updateColumns;
        private int targetMaxUses;
        private boolean bufferRequired;
        private boolean indexWasUnique;

        public Checker(TableNode targetTable, int targetMaxUse, Set<Column> updateColumns) {
            this.targetTable = targetTable;
            this.targetMaxUses = targetMaxUse;
            this.updateColumns = updateColumns;
        }

        public void check(PlanNode root) {
            bufferRequired = false;
            root.accept(this);
        }

        private boolean shouldContinue() {
            return !(bufferRequired && indexWasUnique);
        }

        private void indexScan(IndexScan scan) {
            if (scan instanceof SingleIndexScan) {
                SingleIndexScan single = (SingleIndexScan)scan;
                if (single.isCovering()) { // Non-covering loads via XxxLookup.
                    for (TableSource table : single.getTables()) {
                        if (table.getTable() == targetTable) {
                            targetMaxUses--;
                            if (targetMaxUses < 0) {
                                bufferRequired = true;
                            }
                            break;
                        }
                    }
                }
                if (updateColumns != null) {
                    for (IndexColumn indexColumn : single.getIndex().getAllColumns()) {
                        if (updateColumns.contains(indexColumn.getColumn())) {
                            bufferRequired = true;
                            break;
                        }
                    }
                }

                if(bufferRequired) {
                    indexWasUnique = single.getIndex().isUnique();
                }
            }
            else if (scan instanceof MultiIndexIntersectScan) {
                MultiIndexIntersectScan multi = (MultiIndexIntersectScan)scan;
                indexScan(multi.getOutputIndexScan());
                indexScan(multi.getSelectorIndexScan());
            }
        }

        @Override
        public boolean visitEnter(PlanNode n) {
            return visit(n);
        }

        @Override
        public boolean visitLeave(PlanNode n) {
            return shouldContinue();
        }

        @Override
        public boolean visit(PlanNode n) {
            if (n instanceof IndexScan) {
                indexScan((IndexScan)n);
            }
            else if (n instanceof TableLoader) {
                for (TableSource table : ((TableLoader)n).getTables()) {
                    if (table.getTable() == targetTable) {
                        targetMaxUses--;
                        if (targetMaxUses < 0) {
                            bufferRequired = true;
                            break;
                        }
                    }
                }
            }
            return shouldContinue();
        }

        @Override
        public boolean visitEnter(ExpressionNode n) {
            return visit(n);
        }

        @Override
        public boolean visitLeave(ExpressionNode n) {
            return shouldContinue();
        }

        @Override
        public boolean visit(ExpressionNode n) {
            return shouldContinue();
        }
    }
}
