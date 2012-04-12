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
import com.akiban.sql.optimizer.plan.Sort.OrderByExpression;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

/** Turn aggregate with only keys into distinct.
 */
public class AggregateToDistinctMapper extends BaseRule
{
    private static final Logger logger = LoggerFactory.getLogger(AggregateToDistinctMapper.class);

    @Override
    protected Logger getLogger() {
        return logger;
    }

    @Override
    public void apply(PlanContext plan) {
        List<AggregateSource> sources = new AggregateSourceFinder().find(plan.getPlan());
        for (AggregateSource source : sources) {
            Mapper m = new Mapper(plan.getRulesContext(), source);
            m.remap();
        }
    }

    static class AggregateSourceFinder implements PlanVisitor, ExpressionVisitor {
        List<AggregateSource> result = new ArrayList<AggregateSource>();

        public List<AggregateSource> find(PlanNode root) {
            root.accept(this);
            return result;
        }

        @Override
        public boolean visitEnter(PlanNode n) {
            return visit(n);
        }
        @Override
        public boolean visitLeave(PlanNode n) {
            return true;
        }
        @Override
        public boolean visit(PlanNode n) {
            if (n instanceof AggregateSource) {
                AggregateSource a = (AggregateSource)n;
                if (a.getAggregates().isEmpty())
                    result.add(a);
            }
            return true;
        }

        @Override
        public boolean visitEnter(ExpressionNode n) {
            return visit(n);
        }
        @Override
        public boolean visitLeave(ExpressionNode n) {
            return true;
        }
        @Override
        public boolean visit(ExpressionNode n) {
            return true;
        }
    }

    static class Mapper implements ExpressionRewriteVisitor {
        private RulesContext rulesContext;
        private AggregateSource source;
        private Project project;

        public Mapper(RulesContext rulesContext, AggregateSource source) {
            this.rulesContext = rulesContext;
            this.source = source;
        }

        public void remap() {
            project = new Project(source.getInput(), source.getGroupBy());
            Distinct distinct = new Distinct(project);
            source.getOutput().replaceInput(source, distinct);
            PlanNode n = distinct;
            while (true) {
                // Keep going as long as we're feeding something we understand.
                n = n.getOutput();
                if (n instanceof Select) {
                    remap(((Select)n).getConditions());
                }
                else if (n instanceof Sort) {
                    Sort sort = (Sort)n;
                    List<OrderByExpression> sorts = sort.getOrderBy();
                    List<ExpressionNode> exprs = project.getFields();
                    remapA(sorts);
                    // Try to do the Sort for new Distinct at the same time.
                    // Cf. ASTStatementLoader.Loader.adjustSortsForDistinct
                    boolean merge = true;
                    BitSet used = new BitSet(exprs.size());
                    for (OrderByExpression orderBy : sorts) {
                        ExpressionNode expr = orderBy.getExpression();
                        if (!(expr instanceof ColumnExpression)) {
                            merge = false;
                            break;
                        }
                        ColumnExpression column = (ColumnExpression)expr;
                        if (column.getTable() != project) {
                            merge = false;
                            break;
                        }
                        used.set(column.getPosition());
                    }
                    if (merge) {
                        for (int i = 0; i < exprs.size(); i++) {
                            if (!used.get(i)) {
                                ExpressionNode expr = exprs.get(i);
                                ExpressionNode cexpr = new ColumnExpression(project, i,
                                                                            expr.getSQLtype(),
                                                                            expr.getSQLsource());
                                OrderByExpression orderBy = new OrderByExpression(cexpr,
                                                                                  sorts.get(0).isAscending());
                                sorts.add(orderBy);
                            }
                        }
                        n = sort.getInput();
                        sort.getOutput().replaceInput(sort, n);
                        sort.replaceInput(n, distinct.getInput());
                        distinct.replaceInput(distinct.getInput(), sort);
                        distinct.setImplementation(Distinct.Implementation.EXPLICIT_SORT);
                    }
                }
                else if (n instanceof Project) {
                    // This will commonly be equivalent to the project we just added.
                    List<ExpressionNode> fields = ((Project)n).getFields();
                    boolean unnecessary = fields.size() == project.getFields().size();
                    if (unnecessary) {
                        for (int i = 0; i < fields.size(); i++) {
                            ExpressionNode expr = fields.get(i);
                            if (!(expr instanceof ColumnExpression)) {
                                unnecessary = false;
                                break;
                            }
                            ColumnExpression column = (ColumnExpression)expr;
                            if (!((column.getTable() == source) &&
                                  (column.getPosition() == i))) {
                                unnecessary = false;
                                break;
                            }
                        }
                    }
                    if (unnecessary) {
                        Project project2 = (Project)n;
                        n = project2.getInput();
                        project2.getOutput().replaceInput(project2, n);
                    }
                    else
                        remap(fields);
                }
                else if (n instanceof Limit) {
                    Limit limit = (Limit)n;
                    if (limit.getInput() instanceof Project) {
                        // One that was necessary above. Swap places
                        // so that Limit can apply to Distinct.
                        n = moveBeneath(limit, (Project)limit.getInput());
                    }
                }
                else
                    break;
            }
        }

        protected PlanNode moveBeneath(BasePlanWithInput node, 
                                       BasePlanWithInput output) {
            PlanNode next = node.getInput();    // Where to continue.
            // Remove from current position.
            node.getOutput().replaceInput(node, next);
            // Splice below current input to desired output.
            PlanNode input = output.getInput();
            node.replaceInput(next, input);
            output.replaceInput(input, node);
            return next;
        }

        protected <T extends ExpressionNode> void remap(List<T> exprs) {
            for (int i = 0; i < exprs.size(); i++) {
                exprs.set(i, (T)exprs.get(i).accept(this));
            }
        }

        protected void remapA(List<? extends AnnotatedExpression> exprs) {
            for (AnnotatedExpression expr : exprs) {
                expr.setExpression(expr.getExpression().accept(this));
            }
        }

        @Override
        public boolean visitChildrenFirst(ExpressionNode expr) {
            return false;
        }

        @Override
        public ExpressionNode visit(ExpressionNode expr) {
            if (expr instanceof ColumnExpression) {
                ColumnExpression column = (ColumnExpression)expr;
                if (column.getTable() == source) {
                    return new ColumnExpression(project, column.getPosition(),
                                                expr.getSQLtype(), expr.getAkType(), expr.getSQLsource());
                }
            }
            return expr;
        }
    }

}
