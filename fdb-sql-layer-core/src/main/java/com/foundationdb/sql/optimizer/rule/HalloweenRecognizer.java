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

import com.foundationdb.ais.model.ForeignKey;
import com.foundationdb.ais.model.Index;
import com.foundationdb.ais.model.Table;
import com.foundationdb.server.error.UnsupportedSQLException;
import com.foundationdb.server.types.texpressions.Comparison;
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
                updateStmt = findInput(stmt, UpdateStatement.class);
                assert (updateStmt != null);
                updateColumns = new HashSet<>();
                Set<Column> vulnerableColumns = new HashSet<>();
                update: {
                    for (UpdateStatement.UpdateColumn updateColumn : updateStmt.getUpdateColumns()) {
                        updateColumns.add(updateColumn.getColumn());
                    }
                    for (Column pkColumn : targetTable.getTable().getPrimaryKeyIncludingInternal().getColumns()) {
                        vulnerableColumns.add(pkColumn);
                    }
                    Join parentJoin = targetTable.getTable().getParentJoin();
                    if (parentJoin != null) {
                        for (JoinColumn joinColumn : parentJoin.getJoinColumns()) {
                            vulnerableColumns.add(joinColumn.getChild());
                        }
                    }
                    for (Column column : vulnerableColumns) {
                        if (updateColumns.contains(column)) {
                            bufferRequired = true;
                            break update;
                        }
                    }
                }
                // Can avoid transforming plan if all vulnerable columns exist as exact predicates
                if (bufferRequired) {
                    bufferRequired = !allHaveEquality(updateStmt, vulnerableColumns);
                }
                indexWasUnique = bufferRequired;
            }

            if (!bufferRequired) {
                Checker checker = new Checker(targetTable.getTable(),
                                              (stmt.getType() == StatementType.INSERT) ? 0 : 1,
                                              updateColumns);
                checker.check(stmt.getInput());
                bufferRequired = checker.bufferRequired;
                indexWasUnique = checker.indexWasUnique();

                if(indexWasUnique && (stmt.getType() == StatementType.UPDATE)) {
                    Set<Column> columns = new HashSet<>();
                    for(IndexColumn ic : checker.index.getKeyColumns()) {
                        columns.add(ic.getColumn());
                    }
                    // Again, avoid transform is possible
                    bufferRequired = indexWasUnique = !allHaveEquality(findInput(stmt, UpdateStatement.class), columns);
                }
            }

            boolean isFKReferenced = !targetTable.getTable().getReferencedForeignKeys().isEmpty();
            if(isFKReferenced) {
                // Reject any DELETE or UPDATE containing the referencing table when the action may change it
                for(ForeignKey fk : targetTable.getTable().getReferencedForeignKeys()) {
                    ForeignKey.Action action = null;
                    switch(stmt.getType()) {
                        case DELETE:
                            action = fk.getDeleteAction();
                        break;
                        case UPDATE:
                            assert updateColumns != null;
                            for(Column c : fk.getReferencedColumns()) {
                                if(updateColumns.contains(c)) {
                                    action = fk.getUpdateAction();
                                    break;
                                }
                            }
                        break;
                    }
                    if((action != null) && checkForReferencing(stmt.getInput(), action, fk.getReferencingTable())) {
                        throw new UnsupportedSQLException(
                            String.format("DML includes both referenced and referencing table %s with FOREIGN KEY action %s",
                                          fk.getReferencingTable().getName().toString(),
                                          action.name()));
                    }
                }
            }

            if(bufferRequired) {
                switch(stmt.getType()) {
                    case INSERT:
                    case DELETE:
                        injectBufferNode(stmt);
                        break;
                    case UPDATE:
                        if(indexWasUnique) {
                            if(isFKReferenced) {
                                throw new UnsupportedSQLException("Halloween vulnerable query on referenced table");
                            }
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

    private static <T> T findInput(PlanNode node, Class<T> clazz) {
        while(node != null) {
            if(clazz.isInstance(node)) {
                return clazz.cast(node);
            }
            if(node instanceof BasePlanWithInput) {
                node = ((BasePlanWithInput)node).getInput();
            } else {
                break;
            }
        }
        return null;
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
        PlanNode node = findInput(dml, BaseUpdateStatement.class).getInput();
        // Push it below a Project to avoid double pack/unpack.
        // TODO: Pushing below Flatten and BaseLookup would buffer smaller rows but requires RowType.ancestorHKey(),
        //       which isn't generally true coming out of Sorters (e.g. ValuesHolderRow)
        while(node instanceof Project) {
            node = ((BasePlanWithInput)node).getInput();
        }
        PlanWithInput origDest = node.getOutput();
        PlanWithInput newInput = new Buffer(node);
        origDest.replaceInput(node, newInput);
    }

    /** Check if all columns are have exact equality conditions */
    private static boolean equalityConditionsPresent(Collection<Column> columns, List<ConditionExpression> conditions) {
        for (Column column : columns) {
            if (!equalityConditionPresent(column, conditions)) {
                return false;
            }
        }
        return true;
    }

    /** Check if column has an exact equality conditions */
    private static boolean equalityConditionPresent(Column column, List<ConditionExpression> conditions) {
        for (ConditionExpression cond : conditions) {
            if (cond instanceof ComparisonCondition) {
                ComparisonCondition cc = (ComparisonCondition)cond;
                ColumnExpression colExpr = null;
                ExpressionNode otherExpr = null;
                if(cc.getLeft() instanceof ColumnExpression) {
                    colExpr = (ColumnExpression)cc.getLeft();
                    otherExpr = cc.getRight();
                } else if (cc.getRight() instanceof ColumnExpression) {
                    colExpr = (ColumnExpression)cc.getRight();
                    otherExpr = cc.getLeft();
                }
                if ((colExpr != null) &&
                    (cc.getOperation() == Comparison.EQ) &&
                    (colExpr.getColumn() == column) &&
                    (otherExpr instanceof ConstantExpression || otherExpr instanceof ParameterExpression)) {
                    return true;
                }
            }
        }
        return false;
    }

    /** Check if all of the columns have equality conditions present. */
    private static boolean allHaveEquality(UpdateStatement updateStmt, Collection<Column> vulnerableColumns) {
        ExpressionsHKeyScan hkeyScan = findInput(updateStmt, ExpressionsHKeyScan.class);
        boolean allExact = (hkeyScan != null) &&
            equalityConditionsPresent(vulnerableColumns, hkeyScan.getConditions());
        if (!allExact) {
            Select select = findInput(updateStmt, Select.class);
            allExact = (select != null) &&
                equalityConditionsPresent(vulnerableColumns, select.getConditions());
        }
        if (!allExact) {
            SingleIndexScan singleIndex = findInput(updateStmt, SingleIndexScan.class);
            allExact = (singleIndex != null) &&
                equalityConditionsPresent(vulnerableColumns, singleIndex.getConditions());
        }
        return allExact;
    }

    /** @return {@code true} if {@code action} is vulnerable to referencing side changing and plan uses referencing. */
    private static boolean checkForReferencing(PlanNode planNode, ForeignKey.Action action, Table referencing) {
        switch(action) {
            case NO_ACTION:
            case RESTRICT:
                // Safe as no changes are made to referencing rows
                return false;
            default:
                Checker checker = new Checker(referencing, 0, null);
                checker.check(planNode);
                return checker.bufferRequired;
        }
    }


    static class Checker implements PlanVisitor, ExpressionVisitor {
        private final Table targetTable;
        private final Set<Column> updateColumns;
        private int targetMaxUses;
        private boolean bufferRequired;
        private Index index;

        public Checker(Table targetTable, int targetMaxUses, Set<Column> updateColumns) {
            this.targetTable = targetTable;
            this.targetMaxUses = targetMaxUses;
            this.updateColumns = updateColumns;
        }

        public void check(PlanNode root) {
            bufferRequired = false;
            root.accept(this);
        }

        public boolean indexWasUnique() {
            return (index != null) && index.isUnique();
        }

        private boolean shouldContinue() {
            // Need to identify *any* index that is driving the scan
            return !(bufferRequired && indexWasUnique());
        }

        private void indexScan(IndexScan scan) {
            if (scan instanceof SingleIndexScan) {
                boolean newBufferRequired = false;
                SingleIndexScan single = (SingleIndexScan)scan;
                if (single.isCovering()) { // Non-covering loads via XxxLookup.
                    for (TableSource table : single.getTables()) {
                        if (table.getTable().getTable() == targetTable) {
                            targetMaxUses--;
                            if (targetMaxUses < 0) {
                                newBufferRequired = true;
                            }
                            break;
                        }
                    }
                }
                if (updateColumns != null) {
                    for (IndexColumn indexColumn : single.getIndex().getAllColumns()) {
                        if (updateColumns.contains(indexColumn.getColumn())) {
                            newBufferRequired = true;
                            break;
                        }
                    }
                }

                if(newBufferRequired) {
                    bufferRequired = true;
                    index = single.getIndex();
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
                    if (table.getTable().getTable() == targetTable) {
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
