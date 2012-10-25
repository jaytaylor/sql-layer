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

import static com.akiban.sql.optimizer.rule.OldExpressionAssembler.*;

import com.akiban.server.t3expressions.OverloadResolver;
import com.akiban.server.t3expressions.OverloadResolver.OverloadResult;
import com.akiban.server.t3expressions.T3RegistryService;
import com.akiban.server.types3.mcompat.mtypes.MString;
import com.akiban.server.types3.pvalue.PValue;
import com.akiban.server.types3.pvalue.PValueSources;
import com.akiban.server.types3.texpressions.TPreparedLiteral;
import com.akiban.server.types3.texpressions.TValidatedScalar;
import com.akiban.sql.optimizer.*;
import com.akiban.sql.optimizer.plan.*;
import com.akiban.sql.optimizer.plan.ExpressionsSource.DistinctState;
import com.akiban.sql.optimizer.plan.PhysicalSelect.PhysicalResultColumn;
import com.akiban.sql.optimizer.plan.ResultSet.ResultField;
import com.akiban.sql.optimizer.plan.Sort.OrderByExpression;
import com.akiban.sql.optimizer.plan.UpdateStatement.UpdateColumn;

import com.akiban.sql.optimizer.rule.range.ColumnRanges;
import com.akiban.sql.optimizer.rule.range.RangeSegment;
import com.akiban.sql.types.DataTypeDescriptor;
import com.akiban.sql.parser.ParameterNode;

import com.akiban.ais.model.Column;
import com.akiban.ais.model.Group;
import com.akiban.ais.model.Index;
import com.akiban.ais.model.IndexColumn;
import com.akiban.ais.model.Sequence;
import com.akiban.ais.model.TableName;
import com.akiban.ais.model.UserTable;
import com.akiban.qp.operator.API.InputPreservationOption;
import com.akiban.qp.operator.API.JoinType;
import com.akiban.server.collation.AkCollator;
import com.akiban.server.types.AkType;
import com.akiban.server.types3.TCast;
import com.akiban.server.types3.TExecutionContext;
import com.akiban.server.types3.TInstance;
import com.akiban.server.types3.TPreptimeValue;
import com.akiban.server.types3.Types3Switch;
import com.akiban.server.types3.texpressions.AnySubqueryTExpression;
import com.akiban.server.types3.texpressions.ExistsSubqueryTExpression;
import com.akiban.server.types3.texpressions.ResultSetSubqueryTExpression;
import com.akiban.server.types3.texpressions.ScalarSubqueryTExpression;
import com.akiban.server.types3.texpressions.TCastExpression;
import com.akiban.server.types3.texpressions.TNullExpression;
import com.akiban.server.types3.texpressions.TPreparedExpression;
import com.akiban.server.types3.texpressions.TPreparedField;
import com.akiban.server.types3.texpressions.TPreparedFunction;

import com.akiban.server.error.AkibanInternalException;
import com.akiban.server.error.UnsupportedSQLException;

import com.akiban.server.expression.Expression;
import com.akiban.server.expression.std.Expressions;
import com.akiban.server.expression.std.FieldExpression;
import com.akiban.server.expression.std.IfNullExpression;
import com.akiban.server.expression.std.LiteralExpression;
import com.akiban.server.expression.std.SequenceNextValueExpression;
import com.akiban.server.expression.subquery.AnySubqueryExpression;
import com.akiban.server.expression.subquery.ExistsSubqueryExpression;
import com.akiban.server.expression.subquery.ResultSetSubqueryExpression;
import com.akiban.server.expression.subquery.ScalarSubqueryExpression;

import com.akiban.qp.operator.API;
import com.akiban.qp.operator.IndexScanSelector;
import com.akiban.qp.operator.Operator;
import com.akiban.qp.operator.UpdateFunction;
import com.akiban.qp.row.BindableRow;
import com.akiban.qp.row.Row;
import com.akiban.qp.rowtype.*;

import com.akiban.qp.expression.IndexBound;
import com.akiban.qp.expression.IndexKeyRange;
import com.akiban.qp.expression.RowBasedUnboundExpressions;
import com.akiban.qp.expression.UnboundExpressions;

import com.akiban.server.explain.*;

import com.akiban.server.api.dml.ColumnSelector;
import com.akiban.util.tap.PointTap;
import com.akiban.util.tap.Tap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

public class OperatorAssembler extends BaseRule
{
    private static final Logger logger = LoggerFactory.getLogger(OperatorAssembler.class);
    private static final PointTap SELECT_COUNT = Tap.createCount("sql: select");
    private static final PointTap INSERT_COUNT = Tap.createCount("sql: insert");
    private static final PointTap UPDATE_COUNT = Tap.createCount("sql: update");
    private static final PointTap DELETE_COUNT = Tap.createCount("sql: delete");

    public static final int INSERTION_SORT_MAX_LIMIT = 100;

    private final boolean usePValues;

    @SuppressWarnings("unused") // used by reflection in RulesTestHelper
    public OperatorAssembler() {
        this.usePValues = Types3Switch.ON;
    }
    
    public OperatorAssembler(boolean usePValues) {
        this.usePValues = usePValues;
    }

    @Override
    protected Logger getLogger() {
        return logger;
    }

    @Override
    public void apply(PlanContext plan) {
        new Assembler(plan, usePValues).apply();
    }

    interface PartialAssembler<T extends Explainable> extends SubqueryOperatorAssembler<T> {
        List<T> assembleExpressions(List<ExpressionNode> expressions,
                                    ColumnExpressionToIndex fieldOffsets);
        List<T> assembleExpressionsA(List<? extends AnnotatedExpression> expressions,
                                     ColumnExpressionToIndex fieldOffsets);
        T assembleExpression(ExpressionNode expr, ColumnExpressionToIndex fieldOffsets);
        void assembleExpressionInto(ExpressionNode expr, ColumnExpressionToIndex fieldOffsets, T[] arr, int i);
        Operator assembleAggregates(Operator inputOperator, RowType inputRowType, int inputsIndex,
                                    AggregateSource aggregateSource);
        T sequenceGenerator(Sequence sequence, Column column, T expression);
        T field(RowType rowType, int position);
        
        RowType valuesRowType(ExpressionsSource expressionsSource);

        void fillNulls(Index index, T[] keys);
        List<T> assembleUpdates(UserTableRowType targetRowType, List<UpdateColumn> updateColumns,
                                         ColumnExpressionToIndex fieldOffsets);
        T[] createNulls(Index index, int nkeys);
        Operator ifEmptyNulls(Operator input, RowType rowType,
                              InputPreservationOption inputPreservation);

        API.Ordering createOrdering();

        ExpressionNode resolveAddedExpression(ExpressionNode expr, PlanContext planContext);
    }

    private static final PartialAssembler<?> NULL_PARTIAL_ASSEMBLER = new PartialAssembler<Explainable>() {
        @Override
        public List<Explainable> assembleExpressions(List<ExpressionNode> expressions,
                                                ColumnExpressionToIndex fieldOffsets) {
            return null;
        }

        @Override
        public void assembleExpressionInto(ExpressionNode expr, ColumnExpressionToIndex fieldOffsets, Explainable[] arr,
                                           int i)
        { // nothing; arr is null
        }

        @Override
        public void fillNulls(Index index, Explainable[] keys) {
            // nothing; keys are null
        }

        @Override
        public RowType valuesRowType(ExpressionsSource expressionsSource) {
            throw new UnsupportedOperationException(); // only the active assembler should be called for this
        }

        @Override
        public List<Explainable> assembleExpressionsA(List<? extends AnnotatedExpression> expressions,
                                                 ColumnExpressionToIndex fieldOffsets) {
            return null;
        }

        @Override
        public Explainable assembleExpression(ExpressionNode expr, ColumnExpressionToIndex fieldOffsets) {
            return null;
        }

        @Override
        public Explainable assembleSubqueryExpression(SubqueryExpression subqueryExpression) {
            return null;
        }

        @Override
        public Operator assembleAggregates(Operator inputOperator, RowType inputRowType, int inputsIndex,
                                           AggregateSource aggregateSource) {
            throw new AssertionError();
        }

        @Override
        public List<Explainable> assembleUpdates(UserTableRowType targetRowType, List<UpdateColumn> updateColumns,
                                                ColumnExpressionToIndex fieldOffsets) {
            return null;
        }

        @Override
        public Explainable[] createNulls(Index index, int nkeys) {
            return null;
        }

        @Override
        public Operator ifEmptyNulls(Operator input, RowType rowType,
                                     InputPreservationOption inputPreservation) {
            return null;
        }

        @Override
        public API.Ordering createOrdering() {
            throw new UnsupportedOperationException();
        }

        @Override
        public Explainable sequenceGenerator(Sequence sequence, Column column, Explainable expression) {
            return null;
        }
        @Override
        public Explainable field(RowType rowType, int position) {
            return null;
        }

        @Override
        public ExpressionNode resolveAddedExpression(ExpressionNode expr, PlanContext planContext) {
            return expr;
        }
    };

    @SuppressWarnings("unchecked")
    private static <T extends Explainable> PartialAssembler<T> nullAssembler() {
        return (PartialAssembler<T>) NULL_PARTIAL_ASSEMBLER;
    }

    static class Assembler {

        abstract class BasePartialAssembler<T extends Explainable> implements PartialAssembler<T> {

            protected BasePartialAssembler(ExpressionAssembler<T> expressionAssembler) {
                this.expressionAssembler = expressionAssembler;
            }

            private ExpressionAssembler<T> expressionAssembler;

            // Assemble a list of expressions from the given nodes.
            @Override
            public List<T> assembleExpressions(List<ExpressionNode> expressions,
                                                           ColumnExpressionToIndex fieldOffsets) {
                List<T> result = new ArrayList<T>(expressions.size());
                for (ExpressionNode expr : expressions) {
                    result.add(assembleExpression(expr, fieldOffsets));
                }
                return result;
            }

            @Override
            public void assembleExpressionInto(ExpressionNode expr, ColumnExpressionToIndex fieldOffsets, T[] arr,
                                               int i) {
                T result = assembleExpression(expr, fieldOffsets);
                arr[i] = result;
            }

            // Assemble a list of expressions from the given nodes.
            @Override
            public List<T> assembleExpressionsA(List<? extends AnnotatedExpression> expressions,
                                                            ColumnExpressionToIndex fieldOffsets) {
                List<T> result = new ArrayList<T>(expressions.size());
                for (AnnotatedExpression aexpr : expressions) {
                    result.add(assembleExpression(aexpr.getExpression(), fieldOffsets));
                }
                return result;
            }

            // Assemble an expression against the given row offsets.
            @Override
            public T assembleExpression(ExpressionNode expr, ColumnExpressionToIndex fieldOffsets) {
                ColumnExpressionContext context = getColumnExpressionContext(fieldOffsets);
                return expressionAssembler.assembleExpression(expr, context, this);
            }

            // Assemble an aggregate operator
            @Override
            public Operator assembleAggregates(Operator inputOperator, RowType inputRowType, int inputsIndex,
                                               AggregateSource aggregateSource) {
                return expressionAssembler.assembleAggregates(inputOperator, inputRowType, inputsIndex, aggregateSource);
            }

            protected abstract T existsExpression(Operator operator, RowType outerRowType,
                                                  RowType innerRowType,
                                                  int bindingPosition);
            protected abstract T anyExpression(Operator operator, T innerExpression, RowType outerRowType,
                                               RowType innerRowType,
                                               int bindingPosition);
            protected abstract T scalarSubqueryExpression(Operator operator, T innerExpression,
                                                          RowType outerRowType,
                                                          RowType innerRowType,
                                                          int bindingPosition);
            protected abstract T resultSetSubqueryExpression(Operator operator,
                                                             TPreptimeValue preptimeValue,
                                                             RowType outerRowType,
                                                             RowType innerRowType,
                                                             int bindingPosition);
            protected abstract T nullExpression(RowType rowType, int i);

            public abstract T sequenceGenerator(Sequence sequence, Column column, T expression);

            protected List<? extends T> createNulls(RowType rowType) {
                int nfields = rowType.nFields();
                List<T> result = new ArrayList<T>(nfields);
                for (int i = 0; i < nfields; ++i)
                    result.add(nullExpression(rowType, i));
                return result;
            }

            @Override
            public T assembleSubqueryExpression(SubqueryExpression sexpr) {
                ColumnExpressionToIndex fieldOffsets = columnBoundRows.current;
                RowType outerRowType = null;
                if (fieldOffsets != null)
                    outerRowType = fieldOffsets.getRowType();
                pushBoundRow(fieldOffsets);
                PlanNode subquery = sexpr.getSubquery().getQuery();
                ExpressionNode expression = null;
                boolean fieldExpression = false;
                if ((sexpr instanceof AnyCondition) ||
                        (sexpr instanceof SubqueryValueExpression)) {
                    if (subquery instanceof ResultSet)
                        subquery = ((ResultSet)subquery).getInput();
                    if (subquery instanceof Project) {
                        Project project = (Project)subquery;
                        subquery = project.getInput();
                        expression = project.getFields().get(0);
                    }
                    else {
                        fieldExpression = true;
                    }
                }
                RowStream stream = assembleQuery(subquery);
                T innerExpression = null;
                if (fieldExpression)
                    innerExpression = field(stream.rowType, 0);
                else if (expression != null)
                    innerExpression = assembleExpression(expression, stream.fieldOffsets);
                T result = assembleSubqueryExpression(sexpr,
                        stream.operator,
                        innerExpression,
                        outerRowType,
                        stream.rowType,
                        currentBindingPosition());
                popBoundRow();
                columnBoundRows.current = fieldOffsets;
                return result;
            }

            private T assembleSubqueryExpression(SubqueryExpression sexpr,
                                                 Operator operator,
                                                 T innerExpression,
                                                 RowType outerRowType,
                                                 RowType innerRowType,
                                                 int bindingPosition) {
                if (sexpr instanceof ExistsCondition)
                    return existsExpression(operator, outerRowType,
                            innerRowType, bindingPosition);
                else if (sexpr instanceof AnyCondition)
                    return anyExpression(operator, innerExpression,
                            outerRowType, innerRowType, bindingPosition);
                else if (sexpr instanceof SubqueryValueExpression)
                    return scalarSubqueryExpression(operator, innerExpression,
                                                    outerRowType, innerRowType,
                                                    bindingPosition);
                else if (sexpr instanceof SubqueryResultSetExpression)
                    return resultSetSubqueryExpression(operator, sexpr.getPreptimeValue(),
                                                       outerRowType, innerRowType, 
                                                       bindingPosition);
                else
                    throw new UnsupportedSQLException("Unknown subquery", sexpr.getSQLsource());
            }

            @Override
            public List<T> assembleUpdates(UserTableRowType targetRowType, List<UpdateColumn> updateColumns,
                                           ColumnExpressionToIndex fieldOffsets) {
                List<T> updates = assembleExpressionsA(updateColumns, fieldOffsets);
                // Have a list of expressions in the order specified.
                // Want a list as wide as the target row with Java nulls
                // for the gaps.
                // TODO: It might be simpler to have an update function
                // that knew about column offsets for ordered expressions.
                T[] row = array(targetRowType.nFields());
                for (int i = 0; i < updateColumns.size(); i++) {
                    UpdateColumn column = updateColumns.get(i);
                    row[column.getColumn().getPosition()] = updates.get(i);
                }
                updates = Arrays.asList(row);
                return updates;
            }

            @Override
            public T[] createNulls(Index index, int nkeys) {
                T[] arr = array(nkeys);
                fillNulls(index, arr);
                return arr;
            }

            protected abstract T[] array(int size);

            @Override
            public ExpressionNode resolveAddedExpression(ExpressionNode expr,
                                                         PlanContext planContext) {
                return expr;
            }
        }

        private class OldPartialAssembler extends BasePartialAssembler<Expression> {

            private OldPartialAssembler(PlanContext context) {
                super(new OldExpressionAssembler(context));
            }

            @Override
            public void fillNulls(Index index, Expression[] keys) {
                for (int i = 0; i < keys.length; ++i) {
                    if (keys[i] == null)
                        keys[i] = LiteralExpression.forNull();
                }
            }

            @Override
            public RowType valuesRowType(ExpressionsSource expressionsSource) {
                AkType[] types = expressionsSource.getFieldTypes();
                return schema.newValuesType(types);
            }
            
            @Override
            public Expression sequenceGenerator (Sequence sequence, Column column, Expression expression) {

                Expression seqExpr = new com.akiban.server.expression.std.CastExpression (column.getType().akType(),
                        new SequenceNextValueExpression(AkType.LONG, 
                        new LiteralExpression (AkType.VARCHAR, sequence.getSequenceName().getSchemaName()),
                        new LiteralExpression (AkType.VARCHAR, sequence.getSequenceName().getTableName())));
                // If the row expression is not null (i.e. the user supplied values for this column)
                // and the column is has "BY DEFAULT" as the identity generator
                // replace the SequenceNextValue is a IFNULL(<user value>, <sequence>) expression. 
                if (expression != null && 
                        column.getDefaultIdentity() != null &&
                        column.getDefaultIdentity().booleanValue()) { 
                    List<Expression> expr = new ArrayList<Expression>(2);
                    expr.add(expression);
                    expr.add(seqExpr);
                    seqExpr = new IfNullExpression (expr);
                }
                return seqExpr; 
            }

            @Override
            protected Expression existsExpression(Operator operator, RowType outerRowType, RowType innerRowType,
                                                  int bindingPosition) {
                return new ExistsSubqueryExpression(operator, outerRowType, innerRowType, bindingPosition);
            }

            @Override
            protected Expression anyExpression(Operator operator, Expression innerExpression, RowType outerRowType,
                                               RowType innerRowType,
                                               int bindingPosition) {
                return new AnySubqueryExpression(operator, innerExpression, outerRowType, innerRowType,
                        bindingPosition);
            }

            @Override
            protected Expression scalarSubqueryExpression(Operator operator, Expression innerExpression,
                                                          RowType outerRowType, RowType innerRowType,
                                                          int bindingPosition) {
                return new ScalarSubqueryExpression(operator, innerExpression, outerRowType, innerRowType,
                        bindingPosition);
            }

            @Override
            protected Expression resultSetSubqueryExpression(Operator operator, TPreptimeValue preptimeValue,
                                                             RowType outerRowType, RowType innerRowType,
                                                             int bindingPosition) {
                return new ResultSetSubqueryExpression(operator, outerRowType, innerRowType, bindingPosition);
            }

            @Override
            public Expression field(RowType rowType, int position) {
                return new FieldExpression(rowType, position);
            }

            @Override
            protected Expression[] array(int size) {
                return new Expression[size];
            }

            @Override
            public Operator ifEmptyNulls(Operator input, RowType rowType,
                                         InputPreservationOption inputPreservation) {
                return API.ifEmpty_Default(input, rowType, createNulls(rowType), null, inputPreservation);
            }

            @Override
            protected Expression nullExpression(RowType rowType, int i) {
                return LiteralExpression.forNull();
            }

            @Override
            public API.Ordering createOrdering() {
                return API.ordering(false);
            }
        }

        private class NewPartialAssembler extends BasePartialAssembler<TPreparedExpression> {
            private NewPartialAssembler(PlanContext context) {
                super(new NewExpressionAssembler(context));
            }

            @Override
            public void fillNulls(Index index, TPreparedExpression[] keys) {
                List<IndexColumn> indexColumns = index.getAllColumns();
                for (int i = 0; i < keys.length; ++i) {
                    if (keys[i] == null)
                        keys[i] = new TNullExpression(indexColumns.get(i).getColumn().tInstance());
                }
            }

            @Override
            public RowType valuesRowType(ExpressionsSource expressionsSource) {
                TInstance[] types = expressionsSource.getFieldTInstances();
                return schema.newValuesType(types);
            }

            @Override
            protected TPreparedExpression existsExpression(Operator operator, RowType outerRowType,
                                                           RowType innerRowType,
                                                           int bindingPosition) {
                return new ExistsSubqueryTExpression(operator, outerRowType, innerRowType, bindingPosition);
            }

            @Override
            protected TPreparedExpression anyExpression(Operator operator, TPreparedExpression innerExpression,
                                                        RowType outerRowType,
                                                        RowType innerRowType, int bindingPosition) {
                return new AnySubqueryTExpression(operator, innerExpression, outerRowType, innerRowType, bindingPosition);
            }

            @Override
            protected TPreparedExpression scalarSubqueryExpression(Operator operator, TPreparedExpression innerExpression,
                                                                   RowType outerRowType, RowType innerRowType,
                                                                   int bindingPosition) {
                return new ScalarSubqueryTExpression(operator, innerExpression, outerRowType, innerRowType, bindingPosition);
            }

            @Override
            protected TPreparedExpression resultSetSubqueryExpression(Operator operator, TPreptimeValue preptimeValue,
                                                                      RowType outerRowType, RowType innerRowType, int bindingPosition) {
                return new ResultSetSubqueryTExpression(operator, preptimeValue.instance(), outerRowType, innerRowType, bindingPosition);
            }

            @Override
            public TPreparedExpression field(RowType rowType, int position) {
                return new TPreparedField(rowType.typeInstanceAt(position), position);
            }

            @Override
            protected TPreparedExpression[] array(int size) {
                return new TPreparedExpression[size];
            }

            @Override
            public Operator ifEmptyNulls(Operator input, RowType rowType,
                                         InputPreservationOption inputPreservation) {
                return API.ifEmpty_Default(input, rowType, null, createNulls(rowType), inputPreservation);
            }

            @Override
            protected TPreparedExpression nullExpression(RowType rowType, int i) {
                return new TNullExpression(rowType.typeInstanceAt(i));
            }

            @Override
            public API.Ordering createOrdering() {
                return API.ordering(true);
            }

            @Override
            public ExpressionNode resolveAddedExpression(ExpressionNode expr,
                                                         PlanContext planContext) {
                ExpressionRewriteVisitor visitor = OverloadAndTInstanceResolver.getResolver(planContext);
                return expr.accept(visitor);
            }

            @Override
            public TPreparedExpression sequenceGenerator(Sequence sequence, Column column, TPreparedExpression expression) {
                T3RegistryService registry = rulesContext.getT3Registry();
                OverloadResolver<TValidatedScalar> resolver = registry.getScalarsResolver();
                TInstance instance = column.tInstance();
                
                List<TPreptimeValue> input = new ArrayList<TPreptimeValue>(2);
                input.add(PValueSources.fromObject(sequence.getSequenceName().getSchemaName(), AkType.VARCHAR));
                input.add(PValueSources.fromObject(sequence.getSequenceName().getTableName(), AkType.VARCHAR));

                TValidatedScalar overload = resolver.get("NEXTVAL", input).getOverload();

                List<TPreparedExpression> arguments = new ArrayList<TPreparedExpression>(2);
                arguments.add(new TPreparedLiteral(input.get(0).instance(), input.get(0).value()));
                arguments.add(new TPreparedLiteral(input.get(1).instance(), input.get(1).value()));

                TInstance overloadResultInstance = overload.resultStrategy().fixed(column.getNullable());
                TPreparedExpression seqExpr =  new TPreparedFunction(overload, overloadResultInstance,
                                arguments, planContext.getQueryContext());

                if (!instance.equals(overloadResultInstance)) {
                    TCast tcast = registry.getCastsResolver().cast(seqExpr.resultType(), instance);
                    seqExpr = 
                            new TCastExpression(seqExpr, tcast, instance, planContext.getQueryContext());
                }
                // If the row expression is not null (i.e. the user supplied values for this column)
                // and the column is has "BY DEFAULT" as the identity generator
                // replace the SequenceNextValue is a IFNULL(<user value>, <sequence>) expression. 
                if (expression != null && 
                        column.getDefaultIdentity() != null &&
                        column.getDefaultIdentity().booleanValue()) { 
                    List<TPreptimeValue> ifNullInput = new ArrayList<TPreptimeValue>(2);
                    ifNullInput.add(new TNullExpression(expression.resultType()).evaluateConstant(planContext.getQueryContext()));
                    ifNullInput.add(new TNullExpression(seqExpr.resultType()).evaluateConstant(planContext.getQueryContext()));

                    OverloadResult<TValidatedScalar> ifNullResult = resolver.get("IFNULL", ifNullInput);
                    TValidatedScalar ifNullOverload = ifNullResult.getOverload();
                    List<TPreparedExpression> ifNullArgs = new ArrayList<TPreparedExpression>(2);
                    ifNullArgs.add(expression);
                    ifNullArgs.add(seqExpr);
                    seqExpr = new TPreparedFunction(ifNullOverload, ifNullResult.getPickedInstance(),
                            ifNullArgs, planContext.getQueryContext());
                }
                
                return seqExpr;
            }
        }

        private PlanContext planContext;
        private SchemaRulesContext rulesContext;
        private PlanExplainContext explainContext;
        private Schema schema;
        private boolean usePValues;
        private final PartialAssembler<Expression> oldPartialAssembler;
        private final PartialAssembler<TPreparedExpression> newPartialAssembler;
        private final PartialAssembler<?> partialAssembler;

        public Assembler(PlanContext planContext, boolean usePValues) {
            this.usePValues = usePValues;
            this.planContext = planContext;
            rulesContext = (SchemaRulesContext)planContext.getRulesContext();
            if (planContext instanceof ExplainPlanContext)
                explainContext = ((ExplainPlanContext)planContext).getExplainContext();
            schema = rulesContext.getSchema();
            if (usePValues) {
                newPartialAssembler = new NewPartialAssembler(planContext);
                oldPartialAssembler = nullAssembler();
                partialAssembler = newPartialAssembler;
            }
            else {
                newPartialAssembler = nullAssembler();
                oldPartialAssembler = new OldPartialAssembler(planContext);
                partialAssembler = oldPartialAssembler;
            }
            computeBindingsOffsets();
        }

        public void apply() {
            planContext.setPlan(assembleStatement((BaseStatement)planContext.getPlan()));
        }
        
        protected BasePlannable assembleStatement(BaseStatement plan) {
            if (plan instanceof SelectQuery) {
                SELECT_COUNT.hit();
                return selectQuery((SelectQuery)plan);
            } else if (plan instanceof DMLStatement) {
                return dmlStatement ((DMLStatement)plan);
            } else
                throw new UnsupportedSQLException("Cannot assemble plan: " + plan, null);
        }

        protected PhysicalSelect selectQuery(SelectQuery selectQuery) {
            PlanNode planQuery = selectQuery.getQuery();
            RowStream stream = assembleQuery(planQuery);
            List<PhysicalResultColumn> resultColumns;
            if (planQuery instanceof ResultSet) {
                List<ResultField> results = ((ResultSet)planQuery).getFields();
                resultColumns = getResultColumns(results);
            }
            else {
                // VALUES results in column1, column2, ...
                resultColumns = getResultColumns(stream.rowType.nFields());
            }
            return new PhysicalSelect(stream.operator, stream.rowType, resultColumns, 
                                      getParameterTypes());
        }

        protected PhysicalUpdate dmlStatement (DMLStatement statement) {
            
            PlanNode planQuery = statement.getInput();
            RowStream stream = assembleStream(planQuery);
            
            //If we're returning results we need the resultColumns,
            // including names and types for returning to the user.
            List<PhysicalResultColumn> resultColumns = null;
            if (statement.getResultField() != null) {
                resultColumns = getResultColumns(statement.getResultField());
            }
            // Returning rows, if the table is not null, the insert is returning rows 
            // which need to be passed to the user. 
            boolean returning = !(statement.getTable() == null);
            return new PhysicalUpdate(stream.operator, getParameterTypes(),
                    stream.rowType,
                    resultColumns,
                    returning,
                    statement.isRequireStepIsolation());
        }

        protected RowStream assembleInsertStatement (InsertStatement insert) {
            INSERT_COUNT.hit();
            
            PlanNode planQuery = insert.getInput();
            List<ExpressionNode> projectFields = null;
            if (planQuery instanceof Project) {
                Project project = (Project)planQuery;
                projectFields = project.getFields();
                planQuery = project.getInput();
            }
            RowStream stream = assembleQuery(planQuery);
            
            stream = assembleInsertProjectTable (stream, projectFields, insert);
            
            stream.operator = API.insert_Returning(stream.operator, usePValues);
            
            if (explainContext != null)
                explainInsertStatement(stream.operator, insert);
            
            return stream;
        }
        
        protected RowStream assembleInsertProjectTable (RowStream input, 
                List<ExpressionNode> projectFields, InsertStatement insert) {

            UserTableRowType targetRowType = 
                    tableRowType(insert.getTargetTable());
            UserTable table = insert.getTargetTable().getTable();            

            List<Expression> inserts = null;
            List<TPreparedExpression> insertsP = null;
            if (projectFields != null) {
                // In the common case, we can project into a wider row
                // of the correct type directly.
                insertsP = newPartialAssembler.assembleExpressions(projectFields, input.fieldOffsets);
                inserts = oldPartialAssembler.assembleExpressions(projectFields, input.fieldOffsets);
            }
            else {
                // VALUES just needs each field, which will get rearranged below.
                int nfields = input.rowType.nFields();
                if (usePValues) {
                    insertsP = new ArrayList<TPreparedExpression>(nfields);
                    for (int i = 0; i < nfields; ++i) {
                        insertsP.add(new TPreparedField(input.rowType.typeInstanceAt(i), i));
                    }
                }
                else {
                    inserts = new ArrayList<Expression>(nfields);
                    for (int i = 0; i < nfields; i++) {
                        inserts.add(Expressions.field(input.rowType, i));
                    }
                }
            }

            // Types 2 processing 
            if (inserts != null) {
                Expression[] row = new Expression[targetRowType.nFields()];
                int ncols = inserts.size();

                for (int i = 0; i < ncols; i++) {
                    Column column = insert.getTargetColumns().get(i);
                    int pos = column.getPosition();
                    row[pos] = inserts.get(i);

                    if (column.getType().akType() != row[pos].valueType() ) {
                        row[pos] = new com.akiban.server.expression.std.CastExpression 
                                (column.getType().akType(), row[pos]);
                    }
                }
                // Insert the sequence generator values and default value processing
                for (int i = 0; i < targetRowType.nFields(); i++) {
                    Column column = table.getColumnsIncludingInternal().get(i);
                    if (column.getIdentityGenerator() != null) {
                        Sequence sequence = table.getColumn(i).getIdentityGenerator();
                        row[i] = oldPartialAssembler.sequenceGenerator(sequence, column, row[i]);
                    } else if (row[i] == null) {
                        row[i] = new com.akiban.server.expression.std.CastExpression 
                                (column.getType().akType(), new LiteralExpression(AkType.VARCHAR, column.getDefaultValue()));
                    }
                }
                inserts = Arrays.asList(row);
                // else do the types 3 processing. 
            } else { 
                TPreparedExpression[] row = new TPreparedExpression[targetRowType.nFields()];
                int ncols = insertsP.size();
                for (int i = 0; i < ncols; i++) {
                    Column column = insert.getTargetColumns().get(i);
                    TInstance instance = column.tInstance();
                    int pos = column.getPosition();
                    row[pos] = insertsP.get(i);
                    
                    if (!instance.equals(row[pos].resultType())) {
                        T3RegistryService registry = rulesContext.getT3Registry();
                        TCast tcast = registry.getCastsResolver().cast(instance.typeClass(), row[pos].resultType().typeClass());
                        row[pos] = 
                                new TCastExpression(row[pos], tcast, instance, planContext.getQueryContext());
                    }
                }
                // Insert the sequence generator and column default values
                for (int i = 0, len = targetRowType.nFields(); i < len; ++i) {
                    Column column = table.getColumnsIncludingInternal().get(i);
                    if (column.getIdentityGenerator() != null) {
                        Sequence sequence = table.getColumn(i).getIdentityGenerator();
                        row[i] = newPartialAssembler.sequenceGenerator(sequence, column, row[i]);
                    } 
                    else if (row[i] == null) {
                        TInstance tinst = targetRowType.typeInstanceAt(i);
                        final String defaultValue = column.getDefaultValue();
                        final PValue defaultValueSource;
                        if(defaultValue == null) {
                            defaultValueSource = new PValue(tinst.typeClass().underlyingType());
                            defaultValueSource.putNull();
                        } else {
                            TCast cast = tinst.typeClass().castFromVarchar();
                            if (cast != null) {
                                defaultValueSource = new PValue(tinst.typeClass().underlyingType());
                                TInstance valInst = MString.VARCHAR.instance(defaultValue.length(), false);
                                TExecutionContext executionContext = new TExecutionContext(
                                        Collections.singletonList(valInst),
                                        tinst, planContext.getQueryContext());
                                cast.evaluate(executionContext, new PValue(defaultValue), defaultValueSource);
                            } else {
                                defaultValueSource = new PValue (defaultValue);
                            }
                        }
                        row[i] = new TPreparedLiteral(tinst, defaultValueSource);
                    }
                }
                insertsP = Arrays.asList(row);
            }
            
            input.operator = API.project_Table(input.operator, input.rowType,
                    targetRowType, inserts, insertsP);
            input.rowType = input.operator.rowType();
            input.fieldOffsets = new ColumnSourceFieldOffsets(insert.getTable(), targetRowType);
            return input;
        }

        protected void explainInsertStatement(Operator plan, InsertStatement insertStatement) {
            Attributes atts = new Attributes();
            TableName tableName = insertStatement.getTargetTable().getTable().getName();
            atts.put(Label.TABLE_SCHEMA, PrimitiveExplainer.getInstance(tableName.getSchemaName()));
            atts.put(Label.TABLE_NAME, PrimitiveExplainer.getInstance(tableName.getTableName()));
            for (Column column : insertStatement.getTargetColumns()) {
                atts.put(Label.COLUMN_NAME, PrimitiveExplainer.getInstance(column.getName()));
            }
            explainContext.putExtraInfo(plan, new CompoundExplainer(Type.EXTRA_INFO, atts));
        }
        
        protected RowStream assembleUpdateStatement (UpdateStatement updateStatement) {
            UPDATE_COUNT.hit();
            RowStream stream = assembleQuery (updateStatement.getInput());
            UserTableRowType targetRowType = tableRowType(updateStatement.getTargetTable());
            assert (stream.rowType == targetRowType);

            List<UpdateColumn> updateColumns = updateStatement.getUpdateColumns();
            List<Expression> updates = oldPartialAssembler.assembleUpdates(targetRowType, updateColumns,
                    stream.fieldOffsets);
            List<TPreparedExpression> updatesP = newPartialAssembler.assembleUpdates(targetRowType, updateColumns,
                    stream.fieldOffsets);
            UpdateFunction updateFunction = 
                new ExpressionRowUpdateFunction(updates, updatesP, targetRowType);

            stream.operator = API.update_Returning(stream.operator, updateFunction, usePValues);
            stream.fieldOffsets = new ColumnSourceFieldOffsets (updateStatement.getTable(), targetRowType);
            if (explainContext != null)
                explainUpdateStatement(stream.operator, updateStatement, updateColumns, updates, updatesP);            
            return stream;
        }

        protected void explainUpdateStatement(Operator plan, UpdateStatement updateStatement, List<UpdateColumn> updateColumns, List<Expression> updates, List<TPreparedExpression> updatesP) {
            Attributes atts = new Attributes();
            TableName tableName = updateStatement.getTargetTable().getTable().getName();
            atts.put(Label.TABLE_SCHEMA, PrimitiveExplainer.getInstance(tableName.getSchemaName()));
            atts.put(Label.TABLE_NAME, PrimitiveExplainer.getInstance(tableName.getTableName()));
            for (UpdateColumn column : updateColumns) {
                atts.put(Label.COLUMN_NAME, PrimitiveExplainer.getInstance(column.getColumn().getName()));
                if (usePValues)
                    atts.put(Label.EXPRESSIONS, updatesP.get(column.getColumn().getPosition()).getExplainer(explainContext));
                else
                    atts.put(Label.EXPRESSIONS, updates.get(column.getColumn().getPosition()).getExplainer(explainContext));
            }
            explainContext.putExtraInfo(plan, new CompoundExplainer(Type.EXTRA_INFO, atts));
        }
      
        protected RowStream assembleDeleteStatement (DeleteStatement delete) {
            DELETE_COUNT.hit();
            RowStream stream = assembleQuery(delete.getInput());
            
            //stream = assembleDeleteProjectTable (stream, projectFields, delete);
            UserTableRowType targetRowType = tableRowType(delete.getTargetTable());
            
            stream.operator = API.delete_Returning(stream.operator, usePValues);
            stream.fieldOffsets = new ColumnSourceFieldOffsets(delete.getTable(), targetRowType);
            
            if (explainContext != null)
                explainDeleteStatement(stream.operator, delete);
            
            return stream;
        
        }

        protected void explainDeleteStatement(Operator plan, DeleteStatement deleteStatement) {
            Attributes atts = new Attributes();
            TableName tableName = deleteStatement.getTargetTable().getTable().getName();
            atts.put(Label.TABLE_SCHEMA, PrimitiveExplainer.getInstance(tableName.getSchemaName()));
            atts.put(Label.TABLE_NAME, PrimitiveExplainer.getInstance(tableName.getTableName()));
            explainContext.putExtraInfo(plan, new CompoundExplainer(Type.EXTRA_INFO, atts));
        }
        
       // Assemble the top-level query. If there is a ResultSet at
        // the top, it is not handled here, since its meaning is
        // different for the different statement types.
        protected RowStream assembleQuery(PlanNode planQuery) {
            if (planQuery instanceof ResultSet)
                planQuery = ((ResultSet)planQuery).getInput();
            return assembleStream(planQuery);
        }

        // Assemble an ordinary stream node.
        protected RowStream assembleStream(PlanNode node) {
            if (node instanceof IndexScan)
                return assembleIndexScan((IndexScan) node);
            else if (node instanceof GroupScan)
                return assembleGroupScan((GroupScan) node);
            else if (node instanceof Select)
                return assembleSelect((Select) node);
            else if (node instanceof Flatten)
                return assembleFlatten((Flatten) node);
            else if (node instanceof AncestorLookup)
                return assembleAncestorLookup((AncestorLookup) node);
            else if (node instanceof BranchLookup)
                return assembleBranchLookup((BranchLookup) node);
            else if (node instanceof MapJoin)
                return assembleMapJoin((MapJoin) node);
            else if (node instanceof Product)
                return assembleProduct((Product) node);
            else if (node instanceof AggregateSource)
                return assembleAggregateSource((AggregateSource) node);
            else if (node instanceof Distinct)
                return assembleDistinct((Distinct) node);
            else if (node instanceof Sort)
                return assembleSort((Sort) node);
            else if (node instanceof Limit)
                return assembleLimit((Limit) node);
            else if (node instanceof NullIfEmpty)
                return assembleNullIfEmpty((NullIfEmpty) node);
            else if (node instanceof OnlyIfEmpty)
                return assembleOnlyIfEmpty((OnlyIfEmpty) node);
            else if (node instanceof Project)
                return assembleProject((Project) node);
            else if (node instanceof ExpressionsSource)
                return assembleExpressionsSource((ExpressionsSource) node);
            else if (node instanceof SubquerySource)
                return assembleSubquerySource((SubquerySource) node);
            else if (node instanceof NullSource)
                return assembleNullSource((NullSource) node);
            else if (node instanceof UsingBloomFilter)
                return assembleUsingBloomFilter((UsingBloomFilter) node);
            else if (node instanceof BloomFilterFilter)
                return assembleBloomFilterFilter((BloomFilterFilter) node);
            else if (node instanceof InsertStatement) 
                return assembleInsertStatement((InsertStatement)node);
            else if (node instanceof DeleteStatement)
                return assembleDeleteStatement((DeleteStatement)node);
            else if (node instanceof UpdateStatement)
                return assembleUpdateStatement((UpdateStatement)node);
            else
                throw new UnsupportedSQLException("Plan node " + node, null);
        }
        
        protected RowStream assembleIndexScan(IndexScan index) {
            return assembleIndexScan(index, false, useSkipScan(index));
        }

        protected RowStream assembleIndexScan(IndexScan index, boolean forIntersection, boolean useSkipScan) {
            if (index instanceof SingleIndexScan)
                return assembleSingleIndexScan((SingleIndexScan) index, forIntersection);
            else if (index instanceof MultiIndexIntersectScan)
                return assembleIndexIntersection((MultiIndexIntersectScan) index, useSkipScan);
            else
                throw new UnsupportedSQLException("Plan node " + index, null);
        }

        private RowStream assembleIndexIntersection(MultiIndexIntersectScan index, boolean useSkipScan) {
            RowStream stream = new RowStream();
            RowStream outputScan = assembleIndexScan(index.getOutputIndexScan(), 
                                                     true, useSkipScan);
            RowStream selectorScan = assembleIndexScan(index.getSelectorIndexScan(), 
                                                       true, useSkipScan);
            stream.operator = API.intersect_Ordered(
                    outputScan.operator,
                    selectorScan.operator,
                    (IndexRowType) outputScan.rowType,
                    (IndexRowType) selectorScan.rowType,
                    index.getOutputOrderingFields(),
                    index.getSelectorOrderingFields(),
                    index.getComparisonFieldDirections(),
                    JoinType.INNER_JOIN,
                    (useSkipScan) ? 
                    EnumSet.of(API.IntersectOption.OUTPUT_LEFT, 
                               API.IntersectOption.SKIP_SCAN) :
                    EnumSet.of(API.IntersectOption.OUTPUT_LEFT, 
                               API.IntersectOption.SEQUENTIAL_SCAN),
                    usePValues);
            stream.rowType = outputScan.rowType;
            stream.fieldOffsets = new IndexFieldOffsets(index, stream.rowType);

            return stream;
        }

        protected RowStream assembleSingleIndexScan(SingleIndexScan indexScan, boolean forIntersection) {
            RowStream stream = new RowStream();
            Index index = indexScan.getIndex();
            IndexRowType indexRowType = schema.indexRowType(index);
            IndexScanSelector selector;
            if (index.isTableIndex()) {
                selector = IndexScanSelector.inner(index);
            }
            else {
                switch (index.getJoinType()) {
                case LEFT:
                    selector = IndexScanSelector
                        .leftJoinAfter(index, 
                                       indexScan.getLeafMostInnerTable().getTable().getTable());
                    break;
                case RIGHT:
                    selector = IndexScanSelector
                        .rightJoinUntil(index, 
                                        indexScan.getRootMostInnerTable().getTable().getTable());
                    break;
                default:
                    throw new AkibanInternalException("Unknown index join type " +
                                                      index);
                }
            }
            if (index.isSpatial()) {
                stream.operator = API.indexScan_Default(indexRowType,
                                                        assembleSpatialIndexKeyRange(indexScan, null),
                                                        API.ordering(), // TODO: what ordering?
                                                        selector,
                                                        usePValues);
                indexRowType = indexRowType.physicalRowType();
                stream.rowType = indexRowType;
            }
            else if (indexScan.getConditionRange() == null) {
                stream.operator = API.indexScan_Default(indexRowType,
                                                        assembleIndexKeyRange(indexScan, null),
                                                        assembleIndexOrdering(indexScan, indexRowType),
                                                        selector,
                                                        usePValues);
                stream.rowType = indexRowType;
            }
            else {
                ColumnRanges range = indexScan.getConditionRange();
                // Non-single-point ranges are ordered by the ranges
                // themselves as part of merging segments, so that index
                // column is an ordering column.
                // Single-point ranges have only one value for the range column,
                // so they can order by the following columns if we're
                // willing to do the more expensive ordered union.
                // Determine whether anything is taking advantage of this:
                // * Index is being intersected.
                // * Index is effective for query ordering.
                // ** See also special case in AggregateSplitter.directIndexMinMax().
                boolean unionOrdered = (range.isAllSingle() && 
                     (forIntersection || (indexScan.getOrderEffectiveness() != IndexScan.OrderEffectiveness.NONE)));
                for (RangeSegment rangeSegment : range.getSegments()) {
                    Operator scan = API.indexScan_Default(indexRowType,
                                                          assembleIndexKeyRange(indexScan, null, rangeSegment),
                                                          assembleIndexOrdering(indexScan, indexRowType),
                                                          selector,
                                                          usePValues);
                    if (stream.operator == null) {
                        stream.operator = scan;
                        stream.rowType = indexRowType;
                    }
                    else if (unionOrdered) {
                        int nequals = indexScan.getNEquality();
                        List<OrderByExpression> ordering = indexScan.getOrdering();
                        int nordering = ordering.size() - nequals;
                        boolean[] ascending = new boolean[nordering];
                        for (int i = 0; i < nordering; i++) {
                            ascending[i] = ordering.get(nequals + i).isAscending();
                        }
                        stream.operator = API.union_Ordered(stream.operator, scan,
                                                            (IndexRowType)stream.rowType, indexRowType,
                                                            nordering, nordering, 
                                                            ascending,
                                                            usePValues);
                    }
                    else {
                        stream.operator = API.unionAll(stream.operator, stream.rowType, scan, indexRowType, usePValues);
                        stream.rowType = stream.operator.rowType();
                    }
                }
            }
            stream.fieldOffsets = new IndexFieldOffsets(indexScan, indexRowType);
            if (explainContext != null)
                explainSingleIndexScan(stream.operator, indexScan, index);
            return stream;
        }

        protected void explainSingleIndexScan(Operator operator, SingleIndexScan indexScan, Index index) {
            Attributes atts = new Attributes();
            atts.put(Label.ORDER_EFFECTIVENESS, PrimitiveExplainer.getInstance(indexScan.getOrderEffectiveness().name()));
            atts.put(Label.USED_COLUMNS, PrimitiveExplainer.getInstance(indexScan.usesAllColumns() ? indexScan.getColumns().size() : indexScan.getNKeyColumns()));
            explainContext.putExtraInfo(operator, new CompoundExplainer(Type.EXTRA_INFO, atts));
        }

        /**
         * If there are this many or more scans feeding into a tree
         * of intersection / union, then skip scan is enabled for it.
         * (3 scans means 2 intersections or intersection with a two-value union.)
         */
        public static final int SKIP_SCAN_MIN_COUNT_DEFAULT = 3;

        protected boolean useSkipScan(IndexScan index) {
            if (!(index instanceof MultiIndexIntersectScan))
                return false;
            int count = countScans(index);
            int minCount;
            String prop = rulesContext.getProperty("skipScanMinCount");
            if (prop != null)
                minCount = Integer.valueOf(prop);
            else
                minCount = SKIP_SCAN_MIN_COUNT_DEFAULT;
            return (count >= minCount);
        }

        private int countScans(IndexScan index) {
            if (index instanceof SingleIndexScan) {
                SingleIndexScan sindex = (SingleIndexScan)index;
                if (sindex.getConditionRange() == null)
                    return 1;
                else
                    return sindex.getConditionRange().getSegments().size();
            }
            else if (index instanceof MultiIndexIntersectScan) {
                MultiIndexIntersectScan mindex = (MultiIndexIntersectScan)index;
                return countScans(mindex.getOutputIndexScan()) +
                       countScans(mindex.getSelectorIndexScan());
            }
            else
                return 0;
        }

        protected RowStream assembleGroupScan(GroupScan groupScan) {
            RowStream stream = new RowStream();
            Group group = groupScan.getGroup().getGroup();
            stream.operator = API.groupScan_Default(group);
            stream.unknownTypesPresent = true;
            return stream;
        }

        protected RowStream assembleExpressionsSource(ExpressionsSource expressionsSource) {
            RowStream stream = new RowStream();
            stream.rowType = partialAssembler.valuesRowType(expressionsSource);
            List<BindableRow> bindableRows = new ArrayList<BindableRow>();
            for (List<ExpressionNode> exprs : expressionsSource.getExpressions()) {
                List<Expression> expressions = oldPartialAssembler.assembleExpressions(exprs, stream.fieldOffsets);
                List<TPreparedExpression> tExprs = newPartialAssembler.assembleExpressions(exprs, stream.fieldOffsets);
                bindableRows.add(BindableRow.of(stream.rowType, expressions, tExprs, planContext.getQueryContext()));
            }
            stream.operator = API.valuesScan_Default(bindableRows, stream.rowType);
            stream.fieldOffsets = new ColumnSourceFieldOffsets(expressionsSource, 
                                                               stream.rowType);
            if (expressionsSource.getDistinctState() == DistinctState.NEED_DISTINCT) {
                // Add Sort (usually _InsertionLimited) and Distinct.
                assembleSort(stream, stream.rowType.nFields(), expressionsSource,
                             API.SortOption.SUPPRESS_DUPLICATES);
            }
            return stream;
        }

        protected RowStream assembleSubquerySource(SubquerySource subquerySource) {
            PlanNode subquery = subquerySource.getSubquery().getQuery();
            if (subquery instanceof ResultSet)
                subquery = ((ResultSet)subquery).getInput();
            RowStream stream = assembleStream(subquery);
            stream.fieldOffsets = new ColumnSourceFieldOffsets(subquerySource, 
                                                               stream.rowType);
            return stream;
        }

        protected RowStream assembleNullSource(NullSource node) {
            return assembleExpressionsSource(new ExpressionsSource(Collections.<List<ExpressionNode>>emptyList()));
        }

        protected RowStream assembleSelect(Select select) {
            RowStream stream = assembleStream(select.getInput());
            ConditionDependencyAnalyzer dependencies = null;
            for (ConditionExpression condition : select.getConditions()) {
                RowType rowType = stream.rowType;
                ColumnExpressionToIndex fieldOffsets = stream.fieldOffsets;
                if (rowType == null) {
                    // Pre-flattening case: get the single table this
                    // condition must have come from and use its row-type.
                    // TODO: Would it be better if earlier rule saved this?
                    if (dependencies == null)
                        dependencies = new ConditionDependencyAnalyzer(select);
                    TableSource table = (TableSource)dependencies.analyze(condition);
                    rowType = tableRowType(table);
                    fieldOffsets = new ColumnSourceFieldOffsets(table, rowType);
                }
                if (usePValues) {
                    stream.operator = API.select_HKeyOrdered(stream.operator,
                            rowType,
                            newPartialAssembler.assembleExpression(condition,
                                    fieldOffsets));
                }
                else {
                    stream.operator = API.select_HKeyOrdered(stream.operator,
                                                             rowType,
                                                             oldPartialAssembler.assembleExpression(condition,
                                                                     fieldOffsets));
                }
            }
            return stream;
        }

        protected RowStream assembleFlatten(Flatten flatten) {
            RowStream stream = assembleStream(flatten.getInput());
            List<TableNode> tableNodes = flatten.getTableNodes();
            TableNode tableNode = tableNodes.get(0);
            RowType tableRowType = tableRowType(tableNode);
            stream.rowType = tableRowType;
            int ntables = tableNodes.size();
            if (ntables == 1) {
                TableSource tableSource = flatten.getTableSources().get(0);
                if (tableSource != null)
                    stream.fieldOffsets = new ColumnSourceFieldOffsets(tableSource, 
                                                                       tableRowType);
            }
            else {
                Flattened flattened = new Flattened();
                flattened.addTable(tableRowType, flatten.getTableSources().get(0));
                for (int i = 1; i < ntables; i++) {
                    tableNode = tableNodes.get(i);
                    tableRowType = tableRowType(tableNode);
                    flattened.addTable(tableRowType, flatten.getTableSources().get(i));
                    API.JoinType flattenType = null;
                    switch (flatten.getJoinTypes().get(i-1)) {
                    case INNER:
                        flattenType = API.JoinType.INNER_JOIN;
                        break;
                    case LEFT:
                        flattenType = API.JoinType.LEFT_JOIN;
                        break;
                    case RIGHT:
                        flattenType = API.JoinType.RIGHT_JOIN;
                        break;
                    case FULL_OUTER:
                        flattenType = API.JoinType.FULL_JOIN;
                        break;
                    }
                    stream.operator = API.flatten_HKeyOrdered(stream.operator, 
                                                              stream.rowType,
                                                              tableRowType,
                                                              flattenType);
                    stream.rowType = stream.operator.rowType();
                }
                flattened.setRowType(stream.rowType);
                stream.fieldOffsets = flattened;
            }
            if (stream.unknownTypesPresent) {
                stream.operator = API.filter_Default(stream.operator,
                                                     Collections.singletonList(stream.rowType));
                stream.unknownTypesPresent = false;
            }
            return stream;
        }

        protected RowStream assembleAncestorLookup(AncestorLookup ancestorLookup) {
            RowStream stream;
            Group group = ancestorLookup.getDescendant().getGroup();
            List<UserTableRowType> ancestorTypes =
                new ArrayList<UserTableRowType>(ancestorLookup.getAncestors().size());
            for (TableNode table : ancestorLookup.getAncestors()) {
                ancestorTypes.add(tableRowType(table));
            }
            if (ancestorLookup.getInput() instanceof GroupLoopScan) {
                stream = new RowStream();
                int rowIndex = lookupNestedBoundRowIndex(((GroupLoopScan)ancestorLookup.getInput()));
                ColumnExpressionToIndex boundRow = boundRows.get(rowIndex);
                stream.operator = API.ancestorLookup_Nested(group,
                                                            boundRow.getRowType(),
                                                            ancestorTypes,
                                                            rowIndex + loopBindingsOffset);
            }
            else {
                stream = assembleStream(ancestorLookup.getInput());
                RowType inputRowType = stream.rowType; // The index row type.
                API.InputPreservationOption flag = API.InputPreservationOption.DISCARD_INPUT;
                if (!(inputRowType instanceof IndexRowType)) {
                    // Getting from branch lookup.
                    inputRowType = tableRowType(ancestorLookup.getDescendant());
                    flag = API.InputPreservationOption.KEEP_INPUT;
                }
                stream.operator = API.ancestorLookup_Default(stream.operator,
                                                             group,
                                                             inputRowType,
                                                             ancestorTypes,
                                                             flag);
            }
            stream.rowType = null;
            stream.fieldOffsets = null;
            return stream;
        }

        protected RowStream assembleBranchLookup(BranchLookup branchLookup) {
            RowStream stream;
            Group group = branchLookup.getSource().getGroup();
            if (branchLookup.getInput() == null) {
                // Simple version for Product_NestedLoops.
                stream = new RowStream();
                API.InputPreservationOption flag = API.InputPreservationOption.KEEP_INPUT;
                stream.operator = API.branchLookup_Nested(group,
                                                          tableRowType(branchLookup.getSource()),
                                                          tableRowType(branchLookup.getAncestor()),
                                                          tableRowType(branchLookup.getBranch()), 
                                                          flag,
                                                          currentBindingPosition());
                
            }
            else if (branchLookup.getInput() instanceof GroupLoopScan) {
                // Fuller version for group join across subquery boundary.
                stream = new RowStream();
                API.InputPreservationOption flag = API.InputPreservationOption.DISCARD_INPUT;
                int rowIndex = lookupNestedBoundRowIndex(((GroupLoopScan)branchLookup.getInput()));
                ColumnExpressionToIndex boundRow = boundRows.get(rowIndex);
                stream.operator = API.branchLookup_Nested(group,
                                                          boundRow.getRowType(),
                                                          tableRowType(branchLookup.getAncestor()),
                                                          tableRowType(branchLookup.getBranch()), 
                                                          flag,
                                                          rowIndex + loopBindingsOffset);
            }
            else {
                // Ordinary inline version.
                stream = assembleStream(branchLookup.getInput());
                RowType inputRowType = stream.rowType; // The index row type.
                API.InputPreservationOption flag = API.InputPreservationOption.DISCARD_INPUT;
                if (!(inputRowType instanceof IndexRowType)) {
                    // Getting from ancestor lookup.
                    inputRowType = tableRowType(branchLookup.getSource());
                    flag = API.InputPreservationOption.KEEP_INPUT;
                }
                stream.operator = API.branchLookup_Default(stream.operator,
                                                           group,
                                                           inputRowType,
                                                           tableRowType(branchLookup.getBranch()), 
                                                           flag);
            }
            stream.rowType = null;
            stream.unknownTypesPresent = true;
            stream.fieldOffsets = null;
            return stream;
        }

        protected RowStream assembleMapJoin(MapJoin mapJoin) {
            int pos = pushBoundRow(null); // Allocate slot in case loops in outer.
            PlanNode outer = mapJoin.getOuter();
            RowStream ostream = assembleStream(outer);
            boundRows.set(pos, ostream.fieldOffsets);
            RowStream stream = assembleStream(mapJoin.getInner());
            stream.operator = API.map_NestedLoops(ostream.operator, 
                                                  stream.operator,
                                                  currentBindingPosition());
            popBoundRow();
            return stream;
        }

        protected RowStream assembleProduct(Product product) {
            UserTableRowType ancestorRowType = null;
            if (product.getAncestor() != null)
                ancestorRowType = tableRowType(product.getAncestor());
            RowStream pstream = new RowStream();
            Flattened flattened = new Flattened();
            int nbound = 0;
            for (PlanNode subplan : product.getSubplans()) {
                if (pstream.operator != null) {
                    // The actual bound row is the branch row, which
                    // we don't access directly. Just give each
                    // product a separate position; nesting doesn't
                    // matter.
                    pushBoundRow(null);
                    nbound++;
                }
                RowStream stream = assembleStream(subplan);
                if (pstream.operator == null) {
                    pstream.operator = stream.operator;
                    pstream.rowType = stream.rowType;
                }
                else {
                    pstream.operator = API.product_NestedLoops(pstream.operator,
                                                               stream.operator,
                                                               pstream.rowType,
                                                               ancestorRowType,
                                                               stream.rowType,
                                                               currentBindingPosition());
                    pstream.rowType = pstream.operator.rowType();
                }
                if (stream.fieldOffsets instanceof ColumnSourceFieldOffsets) {
                    TableSource table = ((ColumnSourceFieldOffsets)
                                         stream.fieldOffsets).getTable();
                    flattened.addTable(tableRowType(table), table);
                }
                else {
                    flattened.product((Flattened)stream.fieldOffsets);
                }
            }
            while (nbound > 0) {
                popBoundRow();
                nbound--;
            }
            flattened.setRowType(pstream.rowType);
            pstream.fieldOffsets = flattened;
            return pstream;
        }

        protected RowStream assembleAggregateSource(AggregateSource aggregateSource) {
            AggregateSource.Implementation impl = aggregateSource.getImplementation();
            if (impl == null)
              impl = AggregateSource.Implementation.SORT;
            int nkeys = aggregateSource.getNGroupBy();
            RowStream stream;
            switch (impl) {
            case COUNT_STAR:
            case COUNT_TABLE_STATUS:
                {
                    assert !aggregateSource.isProjectSplitOff();
                    assert ((nkeys == 0) &&
                            (aggregateSource.getNAggregates() == 1));
                    if (impl == AggregateSource.Implementation.COUNT_STAR) {
                        stream = assembleStream(aggregateSource.getInput());
                        // TODO: Could be removed, since aggregate_Partial works as well.
                        stream.operator = API.count_Default(stream.operator, 
                                                            stream.rowType,
                                                            usePValues);
                    }
                    else {
                        stream = new RowStream();
                        stream.operator = API.count_TableStatus(tableRowType(aggregateSource.getTable()), usePValues);
                    }
                    stream.rowType = stream.operator.rowType();
                    stream.fieldOffsets = new ColumnSourceFieldOffsets(aggregateSource, 
                                                                       stream.rowType);
                    return stream;
                }                
            }
            assert aggregateSource.isProjectSplitOff();
            stream = assembleStream(aggregateSource.getInput());
            switch (impl) {
            case PRESORTED:
            case UNGROUPED:
                break;
            case FIRST_FROM_INDEX:
                {
                    assert ((nkeys == 0) &&
                            (aggregateSource.getNAggregates() == 1));
                    stream.operator = API.limit_Default(stream.operator, 1);
                    stream = assembleNullIfEmpty(stream);
                    stream.fieldOffsets = new ColumnSourceFieldOffsets(aggregateSource, 
                                                                       stream.rowType);
                    return stream;
                }
            default:
                // TODO: Could pre-aggregate now in PREAGGREGATE_RESORT case.
                assembleSort(stream, nkeys, aggregateSource.getInput(),
                             API.SortOption.PRESERVE_DUPLICATES);
                break;
            }
            PartialAssembler<?> partialAssembler = usePValues ? newPartialAssembler : oldPartialAssembler;
            stream.operator = partialAssembler.assembleAggregates(stream.operator, stream.rowType, nkeys,
                    aggregateSource);
            stream.rowType = stream.operator.rowType();
            stream.fieldOffsets = new ColumnSourceFieldOffsets(aggregateSource,
                                                               stream.rowType);
            return stream;
        }

        protected RowStream assembleDistinct(Distinct distinct) {
            Distinct.Implementation impl = distinct.getImplementation();
            if (impl == Distinct.Implementation.EXPLICIT_SORT) {
                PlanNode input = distinct.getInput();
                if (input instanceof Sort) {
                    // Explicit Sort is still there; combine.
                    return assembleSort((Sort)input, distinct.getOutput(), 
                                        API.SortOption.SUPPRESS_DUPLICATES);
                }
                // Sort removed but Distinct remains.
                impl = Distinct.Implementation.PRESORTED;
            }
            RowStream stream = assembleStream(distinct.getInput());
            if (impl == null)
              impl = Distinct.Implementation.SORT;
            switch (impl) {
            case PRESORTED:
                List<AkCollator> collators = findCollators(distinct.getInput());
                if (collators != null) {
                    stream.operator = API.distinct_Partial(stream.operator, stream.rowType, collators, usePValues);
                } else {
                    throw new UnsupportedOperationException(String.format(
                        "Can't use Distinct_Partial except following a projection. Try again when types3 is in place"));
                }
                break;
            default:
                assembleSort(stream, stream.rowType.nFields(), distinct.getInput(),
                             API.SortOption.SUPPRESS_DUPLICATES);
                break;
            }
            return stream;
        }

        // Hack to handle some easy and common cases until types3.
        private List<AkCollator> findCollators(PlanNode node)
        {
            if (node instanceof Sort) {
                return findCollators(((Sort)node).getInput());
            } else if (node instanceof MapJoin) {
                return findCollators(((MapJoin)node).getInner());
            } else if (node instanceof Project) {
                List<AkCollator> collators = new ArrayList<AkCollator>();
                Project project = (Project) node;
                for (ExpressionNode expressionNode : project.getFields()) {
                    collators.add(expressionNode.getCollator());
                }
                return collators;
            } else if (node instanceof IndexScan) {
                List<AkCollator> collators = new ArrayList<AkCollator>();
                IndexScan indexScan = (IndexScan) node;
                for (IndexColumn indexColumn : indexScan.getIndexColumns()) {
                    collators.add(indexColumn.getColumn().getCollator());
                }
                return collators;
            } else {
                return null;
            }
        }

        protected RowStream assembleSort(Sort sort) {
            return assembleSort(sort, 
                                sort.getOutput(), API.SortOption.PRESERVE_DUPLICATES);
        }

        protected RowStream assembleSort(Sort sort, 
                                         PlanNode output, API.SortOption sortOption) {
            RowStream stream = assembleStream(sort.getInput());
            API.Ordering ordering = partialAssembler.createOrdering();
            for (OrderByExpression orderBy : sort.getOrderBy()) {
                Expression expr = oldPartialAssembler.assembleExpression(orderBy.getExpression(),
                        stream.fieldOffsets);
                TPreparedExpression tExpr = newPartialAssembler.assembleExpression(orderBy.getExpression(),
                        stream.fieldOffsets);
                ordering.append(expr, tExpr, orderBy.isAscending(), orderBy.getCollator());
            }
            assembleSort(stream, ordering, sort.getInput(), output, sortOption);
            return stream;
        }

        protected void assembleSort(RowStream stream, API.Ordering ordering,
                                    PlanNode input, PlanNode output, 
                                    API.SortOption sortOption) {
            int maxrows = -1;
            if (output instanceof Project) {
                output = output.getOutput();
            }
            if (output instanceof Limit) {
                Limit limit = (Limit)output;
                if (!limit.isOffsetParameter() && !limit.isLimitParameter()) {
                    maxrows = limit.getOffset() + limit.getLimit();
                }
            }
            else if (input instanceof ExpressionsSource) {
                ExpressionsSource expressionsSource = (ExpressionsSource)input;
                maxrows = expressionsSource.getExpressions().size();
            }
            if ((maxrows >= 0) && (maxrows <= INSERTION_SORT_MAX_LIMIT))
                stream.operator = API.sort_InsertionLimited(stream.operator, stream.rowType,
                                                            ordering, sortOption, maxrows);
            else
                stream.operator = API.sort_Tree(stream.operator, stream.rowType, ordering, sortOption, usePValues);
        }

        protected void assembleSort(RowStream stream, int nkeys, PlanNode input,
                                    API.SortOption sortOption) {
            List<AkCollator> collators = findCollators(input);
            API.Ordering ordering = partialAssembler.createOrdering();
            for (int i = 0; i < nkeys; i++) {
                Expression expr = oldPartialAssembler.field(stream.rowType, i);
                TPreparedExpression tExpr = newPartialAssembler.field(stream.rowType, i);
                ordering.append(expr, tExpr, true,
                                (collators == null) ? null : collators.get(i));
            }
            assembleSort(stream, ordering, input, null, sortOption);
        }

        protected RowStream assembleLimit(Limit limit) {
            RowStream stream = assembleStream(limit.getInput());
            int nlimit = limit.getLimit();
            if ((nlimit < 0) && !limit.isLimitParameter())
                nlimit = Integer.MAX_VALUE; // Slight disagreement in saying unlimited.
            stream.operator = API.limit_Default(stream.operator, 
                                                limit.getOffset(), limit.isOffsetParameter(),
                                                nlimit, limit.isLimitParameter(), usePValues);
            return stream;
        }

        protected RowStream assembleNullIfEmpty(NullIfEmpty nullIfEmpty) {
            RowStream stream = assembleStream(nullIfEmpty.getInput());
            return assembleNullIfEmpty(stream);
        }

        protected RowStream assembleNullIfEmpty(RowStream stream) {
            stream.operator = partialAssembler.ifEmptyNulls(stream.operator, stream.rowType, API.InputPreservationOption.KEEP_INPUT);
            return stream;
        }

        protected RowStream assembleOnlyIfEmpty(OnlyIfEmpty onlyIfEmpty) {
            RowStream stream = assembleStream(onlyIfEmpty.getInput());
            stream.operator = API.limit_Default(stream.operator, 0, false, 1, false, usePValues);
            // Nulls here have no semantic meaning, but they're easier than trying to
            // figure out an interesting non-null value for each
            // AkType in the row. All that really matters is that the
            // row is there.
            stream.operator = partialAssembler.ifEmptyNulls(stream.operator, stream.rowType, API.InputPreservationOption.DISCARD_INPUT);
            return stream;
        }

        protected RowStream assembleUsingBloomFilter(UsingBloomFilter usingBloomFilter) {
            BloomFilter bloomFilter = usingBloomFilter.getBloomFilter();
            int pos = pushHashTable(bloomFilter);
            RowStream lstream = assembleStream(usingBloomFilter.getLoader());
            RowStream stream = assembleStream(usingBloomFilter.getInput());
            List<AkCollator> collators = null;
            if (usingBloomFilter.getLoader() instanceof IndexScan) {
                collators = new ArrayList<AkCollator>();
                IndexScan indexScan = (IndexScan) usingBloomFilter.getLoader();
                for (IndexColumn indexColumn : indexScan.getIndexColumns()) {
                    collators.add(indexColumn.getColumn().getCollator());
                }
            }
            stream.operator = API.using_BloomFilter(lstream.operator,
                                                    lstream.rowType,
                                                    bloomFilter.getEstimatedSize(),
                                                    pos,
                                                    stream.operator,
                                                    collators,
                                                    usePValues);
            popHashTable(bloomFilter);
            return stream;
        }

        protected RowStream assembleBloomFilterFilter(BloomFilterFilter bloomFilterFilter) {
            BloomFilter bloomFilter = bloomFilterFilter.getBloomFilter();
            int pos = getHashTablePosition(bloomFilter);
            RowStream stream = assembleStream(bloomFilterFilter.getInput());
            boundRows.set(pos, stream.fieldOffsets);
            RowStream cstream = assembleStream(bloomFilterFilter.getCheck());
            boundRows.set(pos, null);
            List<Expression> fields = oldPartialAssembler.assembleExpressions(bloomFilterFilter.getLookupExpressions(),
                    stream.fieldOffsets);
            List<TPreparedExpression> tFields = newPartialAssembler.assembleExpressions(bloomFilterFilter.getLookupExpressions(),
                    stream.fieldOffsets);
            List<AkCollator> collators = new ArrayList<AkCollator>();
            for (ExpressionNode expressionNode : bloomFilterFilter.getLookupExpressions()) {
                collators.add(expressionNode.getCollator());
            }
            stream.operator = API.select_BloomFilter(stream.operator,
                                                     cstream.operator,
                                                     fields,
                                                     tFields,
                                                     collators,
                                                     pos);
            return stream;
        }

        protected RowStream assembleProject(Project project) {
            RowStream stream = assembleStream(project.getInput());
            List<Expression> oldProjections;
            List<? extends TPreparedExpression> pExpressions;
            if (usePValues) {
                pExpressions = newPartialAssembler.assembleExpressions(project.getFields(), stream.fieldOffsets);
                oldProjections = null;
            }
            else {
                pExpressions = null;
                oldProjections = oldPartialAssembler.assembleExpressions(project.getFields(), stream.fieldOffsets);
            }
            stream.operator = API.project_Default(stream.operator,
                                                  stream.rowType,
                                                  oldProjections,
                                                  pExpressions);
            stream.rowType = stream.operator.rowType();
            stream.fieldOffsets = new ColumnSourceFieldOffsets(project,
                                                               stream.rowType);
            return stream;
        }

        // Get a list of result columns based on ResultSet expression names.
        protected List<PhysicalResultColumn> getResultColumns(List<ResultField> fields) {
            List<PhysicalResultColumn> columns = 
                new ArrayList<PhysicalResultColumn>(fields.size());
            for (ResultField field : fields) {
                columns.add(rulesContext.getResultColumn(field));
            }
            return columns;
        }

        // Get a list of result columns for unnamed columns.
        // This would correspond to top-level VALUES, which the parser
        // does not currently support.
        protected List<PhysicalResultColumn> getResultColumns(int ncols) {
            List<PhysicalResultColumn> columns = 
                new ArrayList<PhysicalResultColumn>(ncols);
            for (int i = 0; i < ncols; i++) {
                columns.add(rulesContext.getResultColumn(new ResultField("column" + (i+1))));
            }
            return columns;
        }

        // Generate key range bounds.
        protected IndexKeyRange assembleIndexKeyRange(SingleIndexScan index, ColumnExpressionToIndex fieldOffsets) {
            return assembleIndexKeyRange(
                    index,
                    fieldOffsets,
                    index.getLowComparand(),
                    index.isLowInclusive(),
                    index.getHighComparand(),
                    index.isHighInclusive()
            );
        }

        protected IndexKeyRange assembleIndexKeyRange(SingleIndexScan index, ColumnExpressionToIndex fieldOffsets,
                                                      RangeSegment segment)
        {
            return assembleIndexKeyRange(
                    index,
                    fieldOffsets,
                    segment.getStart().getValueExpression(),
                    segment.getStart().isInclusive(),
                    segment.getEnd().getValueExpression(),
                    segment.getEnd().isInclusive()
            );
        }

        private IndexKeyRange assembleIndexKeyRange(SingleIndexScan index,
                                                    ColumnExpressionToIndex fieldOffsets,
                                                    ExpressionNode lowComparand,
                                                    boolean lowInclusive, ExpressionNode highComparand,
                                                    boolean highInclusive)
        {
            List<ExpressionNode> equalityComparands = index.getEqualityComparands();
            IndexRowType indexRowType = getIndexRowType(index);
            if ((equalityComparands == null) &&
                    (lowComparand == null) && (highComparand == null))
                return IndexKeyRange.unbounded(indexRowType, usePValues);

            int nkeys = 0;
            if (equalityComparands != null)
                nkeys = equalityComparands.size();
            if ((lowComparand != null) || (highComparand != null))
                nkeys++;
            TPreparedExpression[] pkeys = usePValues ? new TPreparedExpression[nkeys] : null;
            Expression[] keys = usePValues ? null : new Expression[nkeys];

            int kidx = 0;
            if (equalityComparands != null) {
                for (ExpressionNode comp : equalityComparands) {
                    if (comp != null) {
                        newPartialAssembler.assembleExpressionInto(comp, fieldOffsets, pkeys, kidx);
                        oldPartialAssembler.assembleExpressionInto(comp, fieldOffsets, keys, kidx);
                    }
                    kidx++;
                }
            }

            if ((lowComparand == null) && (highComparand == null)) {
                IndexBound eq = getIndexBound(index.getIndex(), keys, pkeys, kidx);
                return IndexKeyRange.bounded(indexRowType, eq, true, eq, true);
            }
            else {
                Expression[] lowKeys = null, highKeys = null;
                TPreparedExpression[] lowPKeys = null, highPKeys = null;
                boolean lowInc = false, highInc = false;
                int lidx = kidx, hidx = kidx;
                if ((lidx > 0) || (lowComparand != null)) {
                    lowKeys = keys;
                    lowPKeys = pkeys;
                    if ((hidx > 0) || (highComparand != null)) {
                        if (usePValues) {
                            highPKeys = new TPreparedExpression[nkeys];
                            System.arraycopy(pkeys, 0, highPKeys, 0, nkeys);
                        }
                        else {
                            highKeys = new Expression[nkeys];
                            System.arraycopy(keys, 0, highKeys, 0, nkeys);
                        }
                    }
                }
                else if (highComparand != null) {
                    highKeys = keys;
                    highPKeys = pkeys;
                }
                if (lowComparand != null) {
                    oldPartialAssembler.assembleExpressionInto(lowComparand, fieldOffsets, lowKeys, lidx);
                    newPartialAssembler.assembleExpressionInto(lowComparand, fieldOffsets, lowPKeys, lidx);
                    lidx++;
                    lowInc = lowInclusive;
                }
                if (highComparand != null) {
                    oldPartialAssembler.assembleExpressionInto(highComparand, fieldOffsets, highKeys, hidx);
                    newPartialAssembler.assembleExpressionInto(highComparand, fieldOffsets, highPKeys, hidx);
                    hidx++;
                    highInc = highInclusive;
                }
                int bounded = lidx > hidx ? lidx : hidx;
                IndexBound lo = getIndexBound(index.getIndex(), lowKeys, lowPKeys, bounded);
                IndexBound hi = getIndexBound(index.getIndex(), highKeys, highPKeys, bounded);
                assert lo != null || hi != null;
                if (lo == null) {
                    lo = getNullIndexBound(index.getIndex(), hidx);
                }
                if (hi == null) {
                    hi = getNullIndexBound(index.getIndex(), lidx);
                }
                return IndexKeyRange.bounded(indexRowType, lo, lowInc, hi, highInc);
            }
        }

        protected API.Ordering assembleIndexOrdering(IndexScan index,
                                                     IndexRowType indexRowType) {
            API.Ordering ordering = partialAssembler.createOrdering();
            List<OrderByExpression> indexOrdering = index.getOrdering();
            for (int i = 0; i < indexOrdering.size(); i++) {
                Expression expr = oldPartialAssembler.field(indexRowType, i);
                TPreparedExpression tExpr = newPartialAssembler.field(indexRowType, i);
                ordering.append(expr,
                                tExpr,
                                indexOrdering.get(i).isAscending(),
                                index.getIndexColumns().get(i).getColumn().getCollator());
            }
            return ordering;
        }

        protected UserTableRowType tableRowType(TableSource table) {
            return tableRowType(table.getTable());
        }

        protected UserTableRowType tableRowType(TableNode table) {
            return schema.userTableRowType(table.getTable());
        }

        protected ValuesRowType valuesRowType(AkType[] fields) {
            return schema.newValuesType(fields);
        }

        protected IndexRowType getIndexRowType(SingleIndexScan index) {
            return schema.indexRowType(index.getIndex());
        }

        /** Return an index bound for the given index and expressions.
         * @param index the index in use
         * @param keys {@link Expression}s for index lookup key
         * @param nBoundKeys number of keys actually in use
         */
        protected IndexBound getIndexBound(Index index, Expression[] keys, TPreparedExpression[] pKeys,
                                           int nBoundKeys) {
            if (keys == null && pKeys == null)
                return null;
            Expression[] boundKeys;
            TPreparedExpression[] boundPKeys;
            boolean usePKeys = pKeys != null;
            int nkeys = usePKeys ? pKeys.length : keys.length;
            if (nBoundKeys < nkeys) {
                Object[] source, dest;
                if (usePKeys) {
                    boundKeys = null;
                    boundPKeys = new TPreparedExpression[nBoundKeys];
                    source = pKeys;
                    dest = boundPKeys;
                }
                else {
                    boundKeys = new Expression[nBoundKeys];
                    boundPKeys = null;
                    source = keys;
                    dest = boundKeys;
                }
                System.arraycopy(source, 0, dest, 0, nBoundKeys);
            } else {
                boundKeys = keys;
                boundPKeys = pKeys;
            }
            newPartialAssembler.fillNulls(index, pKeys);
            oldPartialAssembler.fillNulls(index, keys);
            return new IndexBound(getIndexExpressionRow(index, boundKeys, boundPKeys),
                                  getIndexColumnSelector(index, nBoundKeys));
        }

        /** Return an index bound for the given index containing all nulls.
         * @param index the index in use
         * @param nkeys number of keys actually in use
         */
        protected IndexBound getNullIndexBound(Index index, int nkeys) {
            Expression[] keys = oldPartialAssembler.createNulls(index, nkeys);
            TPreparedExpression[] pKeys = newPartialAssembler.createNulls(index, nkeys);
            return new IndexBound(getIndexExpressionRow(index, keys, pKeys),
                                  getIndexColumnSelector(index, nkeys));
        }

        protected IndexKeyRange assembleSpatialIndexKeyRange(SingleIndexScan index, ColumnExpressionToIndex fieldOffsets) {
            FunctionExpression func = (FunctionExpression)index.getLowComparand();
            List<ExpressionNode> operands = func.getOperands();
            IndexRowType indexRowType = getIndexRowType(index);
            if ("_center".equals(func.getFunction())) {
                return IndexKeyRange.spatial(indexRowType,
                                             assembleSpatialIndexPoint(index,
                                                                       operands.get(0),
                                                                       operands.get(1),
                                                                       fieldOffsets),
                                             null);
            }
            else if ("_center_radius".equals(func.getFunction())) {
                ExpressionNode centerY = operands.get(0);
                ExpressionNode centerX = operands.get(1);
                ExpressionNode radius = operands.get(2);
                // Make circle into box. Comparison still remains to eliminate corners.
                // TODO: May need some casts.
                ExpressionNode bottom = new FunctionExpression("minus",
                                                               Arrays.asList(centerY, radius),
                                                               null, null);
                ExpressionNode left = new FunctionExpression("minus",
                                                             Arrays.asList(centerX, radius),
                                                             null, null);
                ExpressionNode top = new FunctionExpression("plus",
                                                            Arrays.asList(centerY, radius),
                                                            null, null);
                ExpressionNode right = new FunctionExpression("plus",
                                                              Arrays.asList(centerX, radius),
                                                              null, null);
                bottom = newPartialAssembler.resolveAddedExpression(bottom, planContext);
                left = newPartialAssembler.resolveAddedExpression(left, planContext);
                top = newPartialAssembler.resolveAddedExpression(top, planContext);
                right = newPartialAssembler.resolveAddedExpression(right, planContext);
                return IndexKeyRange.spatial(indexRowType,
                                             assembleSpatialIndexPoint(index, bottom, left, fieldOffsets),
                                             assembleSpatialIndexPoint(index, top, right, fieldOffsets));
            }
            else {
                throw new AkibanInternalException("Unrecognized spatial index " + index);
            }
        }

        protected IndexBound assembleSpatialIndexPoint(SingleIndexScan index, ExpressionNode y, ExpressionNode x, ColumnExpressionToIndex fieldOffsets) {
            List<ExpressionNode> equalityComparands = index.getEqualityComparands();
            int nkeys = 0;
            if (equalityComparands != null)
                nkeys = equalityComparands.size();
            nkeys += 2;
            TPreparedExpression[] pkeys = usePValues ? new TPreparedExpression[nkeys] : null;
            Expression[] keys = usePValues ? null : new Expression[nkeys];
            int kidx = 0;
            if (equalityComparands != null) {
                for (ExpressionNode comp : equalityComparands) {
                    if (comp != null) {
                        newPartialAssembler.assembleExpressionInto(comp, fieldOffsets, pkeys, kidx);
                        oldPartialAssembler.assembleExpressionInto(comp, fieldOffsets, keys, kidx);
                    }
                    kidx++;
                }
            }
            newPartialAssembler.assembleExpressionInto(y, fieldOffsets, pkeys, kidx);
            oldPartialAssembler.assembleExpressionInto(y, fieldOffsets, keys, kidx++);
            newPartialAssembler.assembleExpressionInto(x, fieldOffsets, pkeys, kidx);
            oldPartialAssembler.assembleExpressionInto(x, fieldOffsets, keys, kidx++);
            assert (kidx == nkeys);
            return getIndexBound(index.getIndex(), keys, pkeys, nkeys);
        }

        /** Return a column selector that enables the first <code>nkeys</code> fields
         * of a row of the index's user table. */
        protected ColumnSelector getIndexColumnSelector(final Index index, 
                                                        final int nkeys) {
            assert nkeys <= index.getAllColumns().size() : index + " " + nkeys;
            return new ColumnSelector() {
                    public boolean includesColumn(int columnPosition) {
                        return columnPosition < nkeys;
                    }
                };
        }

        /** Return a {@link Row} for the given index containing the given
         * {@link Expression} values.  
         */
        protected UnboundExpressions getIndexExpressionRow(Index index, 
                                                           Expression[] keys, TPreparedExpression[] pKeys) {
            RowType rowType = schema.indexRowType(index);
            List<Expression> expressions = keys == null ? null : Arrays.asList(keys);
            List<TPreparedExpression> pExprs = pKeys == null ? null : Arrays.asList(pKeys);
            return new RowBasedUnboundExpressions(rowType, expressions, pExprs);
        }

        // Get the required type for any parameters to the statement.
        protected DataTypeDescriptor[] getParameterTypes() {
            AST ast = ASTStatementLoader.getAST(planContext);
            if (ast == null)
                return null;
            List<ParameterNode> params = ast.getParameters();
            if ((params == null) || params.isEmpty())
                return null;
            int nparams = 0;
            for (ParameterNode param : params) {
                if (nparams < param.getParameterNumber() + 1)
                    nparams = param.getParameterNumber() + 1;
            }
            DataTypeDescriptor[] result = new DataTypeDescriptor[nparams];
            for (ParameterNode param : params) {
                result[param.getParameterNumber()] = param.getType();
            }        
            return result;
        }

        /* Bindings-related state */

        protected int expressionBindingsOffset, loopBindingsOffset;
        protected Stack<ColumnExpressionToIndex> boundRows = new Stack<ColumnExpressionToIndex>(); // Needs to be List<>.
        protected Map<BaseHashTable,Integer> hashTablePositions = new HashMap<BaseHashTable,Integer>();

        protected void computeBindingsOffsets() {
            expressionBindingsOffset = 0;

            // Binding positions start with parameter positions.
            AST ast = ASTStatementLoader.getAST(planContext);
            if (ast != null) {
                List<ParameterNode> params = ast.getParameters();
                if (params != null) {
                    expressionBindingsOffset = ast.getParameters().size();
                }
            }

            loopBindingsOffset = expressionBindingsOffset;
        }

        protected int pushBoundRow(ColumnExpressionToIndex boundRow) {
            int position = boundRows.size();
            boundRows.push(boundRow);
            return position;
        }

        protected void popBoundRow() {
            boundRows.pop();
        }

        protected int currentBindingPosition() {
            return loopBindingsOffset + boundRows.size() - 1;
        }

        protected int pushHashTable(BaseHashTable hashTable) {
            int position = pushBoundRow(null);
            hashTablePositions.put(hashTable, position);
            return position;
        }

        protected void popHashTable(BaseHashTable hashTable) {
            popBoundRow();
            int position = hashTablePositions.remove(hashTable);
            assert (position == boundRows.size());
        }

        protected int getHashTablePosition(BaseHashTable hashTable) {
            return hashTablePositions.get(hashTable);
        }

        class ColumnBoundRows implements ColumnExpressionContext {
            ColumnExpressionToIndex current;

            @Override
            public ColumnExpressionToIndex getCurrentRow() {
                return current;
            }

            @Override
            public List<ColumnExpressionToIndex> getBoundRows() {
                return boundRows;
            }

            @Override
            public int getExpressionBindingsOffset() {
                return expressionBindingsOffset;
            }

            @Override
            public int getLoopBindingsOffset() {
                return loopBindingsOffset;
            }
        }
        
        ColumnBoundRows columnBoundRows = new ColumnBoundRows();

        protected ColumnExpressionContext getColumnExpressionContext(ColumnExpressionToIndex current) {
            columnBoundRows.current = current;
            return columnBoundRows;
        }

        protected int lookupNestedBoundRowIndex(GroupLoopScan scan) {
            // Find the outside key's binding position.
            ColumnExpression joinColumn = scan.getOutsideJoinColumn();
            for (int rowIndex = boundRows.size() - 1; rowIndex >= 0; rowIndex--) {
                ColumnExpressionToIndex boundRow = boundRows.get(rowIndex);
                if (boundRow == null) continue;
                int fieldIndex = boundRow.getIndex(joinColumn);
                if (fieldIndex >= 0) return rowIndex;
            }
            throw new AkibanInternalException("Outer loop not found " + scan);
        }
    }

    // Struct for multiple value return from assembly.
    static class RowStream {
        Operator operator;
        RowType rowType;
        boolean unknownTypesPresent;
        ColumnExpressionToIndex fieldOffsets;
    }

    static abstract class BaseColumnExpressionToIndex implements ColumnExpressionToIndex {
        protected RowType rowType;

        BaseColumnExpressionToIndex(RowType rowType) {
            this.rowType = rowType;
        }

        @Override
        public RowType getRowType() {
            return rowType;
        }
    }

    // Single table-like source.
    static class ColumnSourceFieldOffsets extends BaseColumnExpressionToIndex {
        private ColumnSource source;

        public ColumnSourceFieldOffsets(ColumnSource source, RowType rowType) {
            super(rowType);
            this.source = source;
        }

        public ColumnSource getSource() {
            return source;
        }

        public TableSource getTable() {
            return (TableSource)source;
        }

        @Override
        public int getIndex(ColumnExpression column) {
            if (column.getTable() != source) 
                return -1;
            else
                return column.getPosition();
        }

        @Override
        public String toString() {
            return super.toString() + "(" + source + ")";
        }
    }

    // Index used as field source (e.g., covering).
    static class IndexFieldOffsets extends BaseColumnExpressionToIndex {
        private IndexScan index;

        public IndexFieldOffsets(IndexScan index, RowType rowType) {
            super(rowType);
            this.index = index;
        }

        @Override
        // Access field of the index row itself. 
        // (Covering index or condition before lookup.)
        public int getIndex(ColumnExpression column) {
            return index.getColumns().indexOf(column);
        }


        @Override
        public String toString() {
            return super.toString() + "(" + index + ")";
        }
    }

    // Flattened row.
    static class Flattened extends BaseColumnExpressionToIndex {
        Map<TableSource,Integer> tableOffsets = new HashMap<TableSource,Integer>();
        int nfields;
            
        Flattened() {
            super(null);        // Worked out later.
        }

        public void setRowType(RowType rowType) {
            this.rowType = rowType;
        }

        @Override
        public int getIndex(ColumnExpression column) {
            Integer tableOffset = tableOffsets.get(column.getTable());
            if (tableOffset == null)
                return -1;
            return tableOffset + column.getPosition();
        }

        public void addTable(RowType rowType, TableSource table) {
            if (table != null)
                tableOffsets.put(table, nfields);
            nfields += rowType.nFields();
        }

        // Tack on another flattened using product rules.
        public void product(final Flattened other) {
            List<TableSource> otherTables = 
                new ArrayList<TableSource>(other.tableOffsets.keySet());
            Collections.sort(otherTables,
                             new Comparator<TableSource>() {
                                 @Override
                                 public int compare(TableSource x, TableSource y) {
                                     return other.tableOffsets.get(x) - other.tableOffsets.get(y);
                                 }
                             });
            for (int i = 0; i < otherTables.size(); i++) {
                TableSource otherTable = otherTables.get(i);
                if (!tableOffsets.containsKey(otherTable)) {
                    tableOffsets.put(otherTable, nfields);
                    // Width in other.tableOffsets.
                    nfields += (((i+1 >= otherTables.size()) ?
                                 other.nfields :
                                 other.tableOffsets.get(otherTables.get(i+1))) -
                                other.tableOffsets.get(otherTable));
                }
            }
        }


        @Override
        public String toString() {
            return super.toString() + "(" + tableOffsets.keySet() + ")";
        }
    }
}
