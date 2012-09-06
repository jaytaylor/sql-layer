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

package com.akiban.sql.optimizer.rule;

import com.akiban.sql.optimizer.plan.*;

import com.akiban.ais.model.Column;
import com.akiban.ais.model.IndexColumn;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

/** Identify queries that are susceptible to the Halloween problem.
 * <ul>
 * <li>Updating a primary key, which can change hkeys are so group navigation.</li>
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
        if (plan.getPlan() instanceof BaseUpdateStatement) {
            BaseUpdateStatement stmt = (BaseUpdateStatement)plan.getPlan();
            TableNode targetTable = stmt.getTargetTable();
            boolean requireStepIsolation = false;
            Set<Column> updateColumns = new HashSet<Column>();
            if (stmt instanceof UpdateStatement) {
                for (UpdateStatement.UpdateColumn updateColumn : ((UpdateStatement)stmt).getUpdateColumns()) {
                    updateColumns.add(updateColumn.getColumn());
                }
                for (Column pkColumn : targetTable.getTable().getPrimaryKey().getColumns()) {
                    if (updateColumns.contains(pkColumn)) {
                        requireStepIsolation = true;
                        break;
                    }
                }
            }
            if (!requireStepIsolation) {
                Checker checker = new Checker(targetTable,
                                              (stmt instanceof InsertStatement) ? 0 : 1,
                                              updateColumns);
                requireStepIsolation = checker.check(stmt.getQuery());
            }
            stmt.setRequireStepIsolation(requireStepIsolation);
        }
    }

    static class Checker implements PlanVisitor, ExpressionVisitor {
        private TableNode targetTable;
        private int targetMaxUses;
        private Set<Column> updateColumns;
        private boolean requireStepIsolation;

        public Checker(TableNode targetTable, int targetMaxUses, Set<Column> updateColumns) {
            this.targetTable = targetTable;
            this.targetMaxUses = targetMaxUses;
            this.updateColumns = updateColumns;
        }

        public boolean check(PlanNode root) {
            requireStepIsolation = false;
            root.accept(this);
            return requireStepIsolation;
        }

        private void indexScan(IndexScan scan) {
            if (scan instanceof SingleIndexScan) {
                SingleIndexScan single = (SingleIndexScan)scan;
                if (single.isCovering()) { // Non-covering loads via XxxLookup.
                    for (TableSource table : single.getTables()) {
                        if (table.getTable() == targetTable) {
                            targetMaxUses--;
                            if (targetMaxUses < 0) {
                                requireStepIsolation = true;
                            }
                            break;
                        }
                    }
                }
                if (updateColumns != null) {
                    for (IndexColumn indexColumn : single.getIndex().getAllColumns()) {
                        if (updateColumns.contains(indexColumn.getColumn())) {
                            requireStepIsolation = true;
                            break;
                        }
                    }
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
            return !requireStepIsolation;
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
                            requireStepIsolation = true;
                            break;
                        }
                    }
                }
            }
            return !requireStepIsolation;
        }

        @Override
        public boolean visitEnter(ExpressionNode n) {
            return visit(n);
        }

        @Override
        public boolean visitLeave(ExpressionNode n) {
            return !requireStepIsolation;
        }

        @Override
        public boolean visit(ExpressionNode n) {
            return !requireStepIsolation;
        }
    }
}
