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

import com.akiban.ais.model.Column;
import com.akiban.ais.model.ColumnContainer;
import com.akiban.qp.operator.QueryContext;
import com.akiban.server.error.AkibanInternalException;
import com.akiban.server.t3expressions.OverloadResolver;
import com.akiban.server.t3expressions.OverloadResolver.OverloadResult;
import com.akiban.server.types3.ErrorHandlingMode;
import com.akiban.server.types3.LazyListBase;
import com.akiban.server.types3.TAggregator;
import com.akiban.server.types3.TCast;
import com.akiban.server.types3.TClass;
import com.akiban.server.types3.TExecutionContext;
import com.akiban.server.types3.TInstance;
import com.akiban.server.types3.TOverloadResult;
import com.akiban.server.types3.TPreptimeContext;
import com.akiban.server.types3.TPreptimeValue;
import com.akiban.server.types3.aksql.aktypes.AkBool;
import com.akiban.server.types3.pvalue.PValue;
import com.akiban.server.types3.pvalue.PValueSource;
import com.akiban.server.types3.pvalue.PValueSources;
import com.akiban.server.types3.texpressions.TValidatedOverload;
import com.akiban.sql.optimizer.TypesTranslation;
import com.akiban.sql.optimizer.plan.AggregateFunctionExpression;
import com.akiban.sql.optimizer.plan.AggregateSource;
import com.akiban.sql.optimizer.plan.AnyCondition;
import com.akiban.sql.optimizer.plan.BasePlanWithInput;
import com.akiban.sql.optimizer.plan.BooleanConstantExpression;
import com.akiban.sql.optimizer.plan.BooleanOperationExpression;
import com.akiban.sql.optimizer.plan.CastExpression;
import com.akiban.sql.optimizer.plan.ColumnExpression;
import com.akiban.sql.optimizer.plan.ColumnSource;
import com.akiban.sql.optimizer.plan.ComparisonCondition;
import com.akiban.sql.optimizer.plan.ConstantExpression;
import com.akiban.sql.optimizer.plan.Distinct;
import com.akiban.sql.optimizer.plan.ExistsCondition;
import com.akiban.sql.optimizer.plan.ExpressionNode;
import com.akiban.sql.optimizer.plan.ExpressionRewriteVisitor;
import com.akiban.sql.optimizer.plan.ExpressionsSource;
import com.akiban.sql.optimizer.plan.FunctionExpression;
import com.akiban.sql.optimizer.plan.IfElseExpression;
import com.akiban.sql.optimizer.plan.InListCondition;
import com.akiban.sql.optimizer.plan.InsertStatement;
import com.akiban.sql.optimizer.plan.Limit;
import com.akiban.sql.optimizer.plan.NullSource;
import com.akiban.sql.optimizer.plan.ParameterCondition;
import com.akiban.sql.optimizer.plan.ParameterExpression;
import com.akiban.sql.optimizer.plan.PlanNode;
import com.akiban.sql.optimizer.plan.PlanVisitor;
import com.akiban.sql.optimizer.plan.Project;
import com.akiban.sql.optimizer.plan.ResultSet;
import com.akiban.sql.optimizer.plan.ResultSet.ResultField;
import com.akiban.sql.optimizer.plan.Subquery;
import com.akiban.sql.optimizer.plan.SubqueryResultSetExpression;
import com.akiban.sql.optimizer.plan.SubquerySource;
import com.akiban.sql.optimizer.plan.SubqueryValueExpression;
import com.akiban.sql.optimizer.plan.TableSource;
import com.akiban.sql.optimizer.plan.TypedPlan;
import com.akiban.sql.optimizer.plan.UpdateStatement;
import com.akiban.sql.optimizer.plan.UpdateStatement.UpdateColumn;
import com.akiban.sql.optimizer.rule.ConstantFolder.NewFolder;
import com.akiban.sql.types.DataTypeDescriptor;
import com.google.common.base.Objects;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.BitSet;
import java.util.Collections;
import java.util.List;

public final class OverloadAndTInstanceResolver extends BaseRule {
    private static final Logger logger = LoggerFactory.getLogger(OverloadAndTInstanceResolver.class);

    @Override
    protected Logger getLogger() {
        return logger;
    }

    @Override
    public void apply(PlanContext plan) {
        NewFolder folder = new NewFolder(plan);
        new ResolvingVistor(plan, folder).resolve(plan.getPlan());
        new TopLevelCastingVistor(folder).apply(plan.getPlan());

    }

    static class ResolvingVistor implements PlanVisitor, ExpressionRewriteVisitor {

        private NewFolder folder;
        private OverloadResolver resolver;
        private QueryContext queryContext;

        ResolvingVistor(PlanContext context, NewFolder folder) {
            this.folder = folder;
            SchemaRulesContext src = (SchemaRulesContext)context.getRulesContext();
            resolver = src.getOverloadResolver();
            this.queryContext = context.getQueryContext();
        }

        public void resolve(PlanNode root) {
            root.accept(this);
        }

        @Override
        public boolean visitChildrenFirst(ExpressionNode n) {
            return true;
        }

        @Override
        public boolean visitEnter(PlanNode n) {
            return visit(n);
        }

        @Override
        public boolean visitLeave(PlanNode n) {
            if (n instanceof ResultSet) {
                ResultSet rs = (ResultSet) n;
                TypedPlan typedInput = findTypedPlanNode(rs);
                if (typedInput != null) {
                    List<ResultField> rsFields = rs.getFields();
                    assert rsFields.size() == typedInput.nFields() : rsFields + " not applicable to " + typedInput;
                    for (int i = 0, size = rsFields.size(); i < size; i++) {
                        ResultField rsField = rsFields.get(i);
                        rsField.setTInstance(typedInput.getTypeAt(i));
                    }
                }
                else {
                    logger.warn("no Project node found for ResultSet: {}", rs);
                }
            }
            else if (n instanceof ExpressionsSource) {
                handleExpressionsSource((ExpressionsSource)n);
            }
            return true;
        }

        private TypedPlan findTypedPlanNode(PlanNode n) {
            while (true) {
                if (n instanceof TypedPlan)
                    return (TypedPlan) n;
                if ( (n instanceof ResultSet)
                        || (n instanceof Limit)
                        || (n instanceof Distinct))
                    n = ((BasePlanWithInput)n).getInput();
                else
                    return null;
            }
        }

        @Override
        public boolean visit(PlanNode n) {
            return true;
        }

        @Override
        public ExpressionNode visit(ExpressionNode n) {
            if (n instanceof CastExpression)
                n = handleCastExpression((CastExpression) n);
            else if (n instanceof FunctionExpression)
                n = handleFunctionExpression((FunctionExpression) n);
            else if (n instanceof IfElseExpression)
                n = handleIfElseExpression((IfElseExpression) n);
            else if (n instanceof AggregateFunctionExpression)
                n = handleAggregateFunctionExpression((AggregateFunctionExpression) n);
            else if (n instanceof ExistsCondition)
                n = handleExistsCondition((ExistsCondition) n);
            else if (n instanceof SubqueryValueExpression)
                n = handleSubqueryValueExpression((SubqueryValueExpression) n);
            else if (n instanceof SubqueryResultSetExpression)
                n = handleSubqueryResultSetExpression((SubqueryResultSetExpression) n);
            else if (n instanceof AnyCondition)
                n = handleAnyCondition((AnyCondition) n);
            else if (n instanceof ComparisonCondition)
                n = handleComparisonCondition((ComparisonCondition) n);
            else if (n instanceof ColumnExpression)
                n = handleColumnExpression((ColumnExpression) n);
            else if (n instanceof InListCondition)
                n = handleInListCondition((InListCondition) n);
            else if (n instanceof ParameterCondition)
                n = handleParameterCondition((ParameterCondition) n);
            else if (n instanceof ParameterExpression)
                n = handleParameterExpression((ParameterExpression) n);
            else if (n instanceof BooleanOperationExpression)
                n = handleBooleanOperationExpression((BooleanOperationExpression) n);
            else if (n instanceof BooleanConstantExpression)
                n = handleBooleanConstantExpression((BooleanConstantExpression) n);
            else if (n instanceof ConstantExpression)
                n = handleConstantExpression((ConstantExpression) n);
            else
                logger.warn("unrecognized ExpressionNode subclass: {}", n.getClass());

            n = folder.foldConstants(n);
            // Set nullability of TInstance if it hasn't been given explicitly
            // At the same time, update the node's DataTypeDescriptor to match its TInstance
            TPreptimeValue tpv = n.getPreptimeValue();
            if (tpv != null) {
                TInstance tInstance = tpv.instance();
                if (tInstance != null) {
                    if (tInstance.nullability() == null)
                        tInstance.setNullable(n.getSQLtype().isNullable());
                    DataTypeDescriptor newDtd = tInstance.dataTypeDescriptor();
                    n.setSQLtype(newDtd);
                }
            }
            return n;
        }

        private void handleExpressionsSource(ExpressionsSource node) {
            // For each field, we'll fold the instances of that field per row into common types. At the same time,
            // we'll record on a per-field basis whether any expressions of that field need to be casted (that is,
            // are not the eventual common type). If so, we'll do the casts in a second pass; if we tried to do them
            // all in the same path, some fields could end up with unnecessary (and potentially wrong) chained casts.
            // A null TInstance means an unknown type, which could be a parameter, a literal NULL or of course the
            // initial fold state.

            List<List<ExpressionNode>> rows = node.getExpressions();
            List<ExpressionNode> firstRow = rows.get(0);
            int nfields = firstRow.size();
            TInstance[] instances = new TInstance[nfields];
            BitSet needCasts = new BitSet(nfields);

            // First pass. Assume that instances[f] contains the TInstance of the top operand at field f. This could
            // be null, if that operand doesn't have a type; this is definitely true of the first row, but it can
            // also happen if an ExpressionNode is a constant NULL.
            for (int rownum = 0, expressionsSize = rows.size(); rownum < expressionsSize; rownum++) {
                List<ExpressionNode> row = rows.get(rownum);
                assert row.size() == nfields : "jagged rows: " + node;
                for (int field = 0; field < nfields; ++field) {
                    TInstance botInstance = tinst(row.get(field));

                    // If the two are the same, we know we don't need to cast them.
                    // This logic also handles the case where both are null, which is not a valid argument
                    // to resolver.commonTClass.
                    if (Objects.equal(instances[field], botInstance))
                        continue;

                    TClass topClass = tclass(instances[field]);
                    TClass botClass = tclass(botInstance);

                    TClass common = resolver.commonTClass(topClass, botClass);
                    if (common == null) {
                        throw new AkibanInternalException("no common type found found between row " + (rownum-1)
                        + " and " + rownum + " at field " + field);
                    }
                    // The two rows have different TClasses at this index, so we'll need at least one of them to
                    // be casted. Only applies if both are non-null, though.
                    if ( (topClass != null) && (botClass != null) )
                        needCasts.set(field);

                    Boolean topIsNullable = (instances[field] == null) ? null : instances[field].nullability();
                    Boolean botIsNullable = (botInstance == null) ? null : botInstance.nullability();
                    if (topClass != common){
                        TInstance instance = (botClass == common) ? botInstance : common.instance();
                        instances[field] = instance;
                    }

                    // See if the top instance is not nullable but should be
                    if (instances[field] != null) {
                        Boolean isNullable;
                        if (topIsNullable == null)
                            isNullable = botIsNullable;
                        else if (botIsNullable == null)
                            isNullable = topIsNullable;
                        else
                            isNullable = topIsNullable || botIsNullable;
                        instances[field].setNullable(isNullable);
                    }
                }
            }

            // See if we need any casts
            if (!needCasts.isEmpty()) {
                for (List<ExpressionNode> row : rows) {
                    for (int field = 0; field < nfields; ++field) {
                        if (needCasts.get(field)) {
                            ExpressionNode orig = row.get(field);
                            ExpressionNode cast = castTo(orig, instances[field], folder);
                            row.set(field, cast);
                        }
                    }
                }
            }
            node.setTInstances(instances);
        }

        ExpressionNode handleCastExpression(CastExpression expression) {
            DataTypeDescriptor dtd = expression.getSQLtype();
            TInstance instance = TypesTranslation.toTInstance(dtd);
            expression.setPreptimeValue(new TPreptimeValue(instance));
            return finishCast(expression, folder);
        }

        ExpressionNode handleFunctionExpression(FunctionExpression expression) {
            List<ExpressionNode> operands = expression.getOperands();
            List<TPreptimeValue> operandClasses = new ArrayList<TPreptimeValue>(operands.size());
            for (ExpressionNode operand : operands)
                operandClasses.add(operand.getPreptimeValue());

            OverloadResult resolutionResult = resolver.get(expression.getFunction(), operandClasses);

            // cast operands
            for (int i = 0, operandsSize = operands.size(); i < operandsSize; i++) {
                TClass targetType = resolutionResult.getTypeClass(i);
                if (targetType != null) {
                    ExpressionNode operand = castTo(operands.get(i), targetType, folder);
                    operands.set(i, operand);
                }
            }

            TValidatedOverload overload = resolutionResult.getOverload();
            expression.setOverload(overload);

            final List<TPreptimeValue> operandValues = new ArrayList<TPreptimeValue>(operands.size());
            List<TInstance> operandInstances = new ArrayList<TInstance>(operands.size());
            boolean anyOperandsNullable = false;
            for (ExpressionNode operand : operands) {
                TPreptimeValue preptimeValue = operand.getPreptimeValue();
                operandValues.add(preptimeValue);
                operandInstances.add(preptimeValue.instance());
                if (Boolean.TRUE.equals(preptimeValue.instance().nullability()))
                    anyOperandsNullable = true;
            }

            TOverloadResult overloadResultStrategy = overload.resultStrategy();
            TInstance resultInstance;
            TInstance castTo;

            TPreptimeContext context = new TPreptimeContext(operandInstances, queryContext);
            switch (overloadResultStrategy.category()) {
            case CUSTOM:
                TInstance castSource = overloadResultStrategy.customRuleCastSource();
                if (castSource == null) {
                    castTo = null;
                    resultInstance = overloadResultStrategy.customRule().resultInstance(operandValues, context);
                }
                else {
                    castTo = overloadResultStrategy.customRule().resultInstance(operandValues, context);
                    resultInstance = castSource;
                }
                break;
            case FIXED:
                resultInstance = overloadResultStrategy.fixed();
                castTo = null;
                break;
            case PICKING:
                resultInstance = resolutionResult.getPickedInstance();
                castTo = null;
                break;
            default:
                throw new AssertionError(overloadResultStrategy.category());
            }
            context.setOutputType(resultInstance);
            if (resultInstance.nullability() == null) {
                resultInstance.setNullable(anyOperandsNullable);
            }
            overload.finishPreptimePhase(context);

            // Put the preptime value, possibly including nullness, into the expression. The constant folder
            // will use it.
            TPreptimeValue preptimeValue = overload.evaluateConstant(context, new LazyListBase<TPreptimeValue>() {
                @Override
                public TPreptimeValue get(int i) {
                    return operandValues.get(i);
                }

                @Override
                public int size() {
                    return operandValues.size();
                }
            });
            if (preptimeValue == null)
                preptimeValue = new TPreptimeValue(resultInstance);
            else
                assert resultInstance.equals(preptimeValue.instance())
                        : resultInstance + " != " + preptimeValue.instance();

            expression.setPreptimeValue(preptimeValue);
            if (castTo == null) {
                return expression;
            }
            else {
                return castTo(expression, castTo, folder);
            }
        }

        ExpressionNode handleIfElseExpression(IfElseExpression expression) {
            ExpressionNode thenExpr = expression.getThenExpression();
            ExpressionNode elseExpr = expression.getElseExpression();

            // constant-fold if the condition is constant
            PValueSource conditionVal = pval(expression.getTestCondition());
            if (conditionVal != null) {
                boolean conditionMet = conditionVal.getBoolean(false);
                return conditionMet ? thenExpr : elseExpr;
            }

            TInstance thenType = tinst(thenExpr);
            TInstance elseType = tinst(elseExpr);

            TClass commonClass = resolver.commonTClass(thenType.typeClass(), elseType.typeClass());
            if (commonClass == null)
                throw error("couldn't determine a type for CASE expression");
            thenExpr = castTo(thenExpr, commonClass, folder);
            elseExpr = castTo(elseExpr, commonClass, folder);
            TInstance resultInstance = commonClass.pickInstance(tinst(thenExpr), tinst(elseExpr));
            expression.setPreptimeValue(new TPreptimeValue(resultInstance));
            return expression;
        }

        ExpressionNode handleAggregateFunctionExpression(AggregateFunctionExpression expression) {
            ExpressionNode operand = expression.getOperand();
            TInstance resultType;
            if (operand == null) {
                TAggregator tAggregator = resolver.getAggregation(expression.getFunction(), null);
                resultType = tAggregator.resultType(null);
                expression.setPreptimeValue(new TPreptimeValue(resultType));
            }
            else {
                TClass inputTClass = tclass(operand);
                TAggregator tAggregator = resolver.getAggregation(expression.getFunction(), inputTClass);
                TClass aggrTypeClass = tAggregator.getTypeClass();
                if (aggrTypeClass != null && !aggrTypeClass.equals(inputTClass)) {
                    operand = castTo(operand, aggrTypeClass, folder);
                    expression.setOperand(operand);
                }
                resultType = tAggregator.resultType(operand.getPreptimeValue());
            }
            expression.setPreptimeValue(new TPreptimeValue(resultType));
            return expression;
        }

        ExpressionNode handleExistsCondition(ExistsCondition expression) {
            return boolExpr(expression);
        }

        ExpressionNode handleSubqueryValueExpression(SubqueryValueExpression expression) {
            TypedPlan typedSubquery = findTypedPlanNode(expression.getSubquery().getInput());
            TPreptimeValue tpv;
            assert typedSubquery.nFields() == 1 : typedSubquery;
            if (typedSubquery instanceof Project) {
                Project project = (Project) typedSubquery;
                List<ExpressionNode> projectFields = project.getFields();
                assert projectFields.size() == 1 : projectFields;
                tpv = projectFields.get(0).getPreptimeValue();
            }
            else {
                tpv = new TPreptimeValue(typedSubquery.getTypeAt(0));
            }
            expression.setPreptimeValue(tpv);
            return expression;
        }

        ExpressionNode handleSubqueryResultSetExpression(SubqueryResultSetExpression expression) {
            return boolExpr(expression);
        }

        ExpressionNode handleAnyCondition(AnyCondition expression) {
            return boolExpr(expression);
        }

        ExpressionNode handleComparisonCondition(ComparisonCondition expression) {
            ExpressionNode left = expression.getLeft();
            ExpressionNode right = expression.getRight();
            TClass leftTClass = tclass(left);
            TClass rightTClass = tclass(right);
            if (leftTClass != rightTClass) {
                boolean needCasts = true;
                if ( (left.getClass() == ColumnExpression.class)&& (right.getClass() == ConstantExpression.class)) {
                    // Left is a Column, right is a Constant. Ideally, we'd like to keep the Column as a Column,
                    // and not a CAST(Column AS _) -- otherwise, we can't use it in an index lookup.
                    // So, try to cast the const to the column's type. To do this, CAST(Const -> Column) must be
                    // indexFriendly, *and* casting this result back to the original Const type must equal the same
                    // const.
                    if (resolver.getRegistry().isIndexFriendly(leftTClass, rightTClass)) {
                        TInstance columnType = tinst(left);
                        TInstance constType = tinst(right);
                        TCast constToCol = resolver.getTCast(constType, columnType);
                        if (constToCol != null) {
                            TCast colToConst = resolver.getTCast(columnType, constType);
                            if (colToConst != null) {
                                TPreptimeValue constValue = right.getPreptimeValue();
                                PValueSource asColType = castValue(constToCol, constValue, columnType);
                                TPreptimeValue asColTypeTpv = (asColType == null)
                                        ? null
                                        : new TPreptimeValue(columnType, asColType);
                                PValueSource backToConstType = castValue(colToConst, asColTypeTpv, constType);
                                if (PValueSources.areEqual(constValue.value(), backToConstType)) {
                                    TPreptimeValue constTpv = new TPreptimeValue(columnType, asColType);
                                    ConstantExpression constCasted = new ConstantExpression(constTpv);
                                    expression.setRight(constCasted);
                                    assert columnType.equals(tinst(expression.getRight()));
                                    needCasts = false;
                                }
                            }
                        }
                    }
                }
                if (needCasts) {
                    TClass common = resolver.commonTClass(leftTClass, rightTClass);
                    if (common == null)
                        throw error("no common type found for comparison of " + expression);
                    left = castTo(left, common, folder);
                    right = castTo(right, common, folder);
                    expression.setLeft(left);
                    expression.setRight(right);
                }
            }

            return boolExpr(expression);
        }

        ExpressionNode handleColumnExpression(ColumnExpression expression) {
            Column column = expression.getColumn();
            ColumnSource columnSource = expression.getTable();
            if (column != null) {
                assert columnSource instanceof TableSource : columnSource;
                expression.setPreptimeValue(new TPreptimeValue(column.tInstance()));
            }
            else if (columnSource instanceof AggregateSource) {
                AggregateSource aggTable = (AggregateSource) columnSource;
                TPreptimeValue ptv = aggTable.getField(expression.getPosition()).getPreptimeValue();
                expression.setPreptimeValue(ptv);
            }
            else if (columnSource instanceof SubquerySource) {
                TPreptimeValue tpv;
                Subquery subquery = ((SubquerySource)columnSource).getSubquery();
                TypedPlan typedSubquery = findTypedPlanNode(subquery.getInput());
                if (typedSubquery != null) {
                    tpv = new TPreptimeValue(typedSubquery.getTypeAt(expression.getPosition()));
                }
                else {
                    logger.warn("no Project found for subquery: {}", columnSource);
                    tpv = new TPreptimeValue(TypesTranslation.toTInstance(expression.getSQLtype()));
                }
                expression.setPreptimeValue(tpv);
                return expression;
            }
            else if (columnSource instanceof NullSource) {
                expression.setPreptimeValue(new TPreptimeValue(null));
                return expression;
            }
            else if (columnSource instanceof Project) {
                Project pTable = (Project) columnSource;
                TPreptimeValue ptv = pTable.getFields().get(expression.getPosition()).getPreptimeValue();
                expression.setPreptimeValue(ptv);
            }
            else if (columnSource instanceof ExpressionsSource) {
                ExpressionsSource exprsTable = (ExpressionsSource) columnSource;
                List<List<ExpressionNode>> expressions = exprsTable.getExpressions();
                TPreptimeValue tpv;
                if (expressions.size() == 1) {
                    // get the TPV straight from the expression, since there's just one row
                    tpv = expressions.get(0).get(expression.getPosition()).getPreptimeValue();
                }
                else {
                    TInstance tInstance = exprsTable.getTypeAt(expression.getPosition());
                    tpv = new TPreptimeValue(tInstance);
                }
                expression.setPreptimeValue(tpv);
            }
            else {
                throw new AssertionError(columnSource + "(" + columnSource.getClass() + ")");
            }
            return expression;
        }

        ExpressionNode handleInListCondition(InListCondition expression) {
            return boolExpr(expression);
        }

        ExpressionNode handleParameterCondition(ParameterCondition expression) {
            return boolExpr(expression);
        }

        ExpressionNode handleParameterExpression(ParameterExpression expression) {
            DataTypeDescriptor sqlType = expression.getSQLtype();
            if (sqlType != null) {
                // TODO eventually we'll probably want to ignore this completely, and do all the type inference
                // from within the types3 framework. For now, use what we have.
                TInstance tinst = TypesTranslation.toTInstance(sqlType);
                expression.setPreptimeValue(new TPreptimeValue(tinst));
            }
            return expression;
        }

        ExpressionNode handleBooleanOperationExpression(BooleanOperationExpression expression) {
            return boolExpr(expression);
        }

        ExpressionNode handleBooleanConstantExpression(BooleanConstantExpression expression) {
            return boolExpr(expression);
        }

        ExpressionNode handleConstantExpression(ConstantExpression expression) {
            // will be lazily loaded as necessary
            return expression;
        }

        private static PValueSource pval(ExpressionNode expression) {
            return expression.getPreptimeValue().value();
        }

        private static RuntimeException error(String message) {
            throw new RuntimeException(message); // TODO what actual error type?
        }

    }

    private static PValueSource castValue(TCast cast, TPreptimeValue source, TInstance targetInstance) {
        if (source == null)
            return null;
        boolean targetsMatch = targetInstance.typeClass() == cast.targetClass();
        boolean sourcesMatch = source.instance().typeClass() == cast.sourceClass();
        if ( (!targetsMatch) || (!sourcesMatch) )
            throw new IllegalArgumentException("cast <" + cast + "> not applicable to CAST(" + source + " AS " + targetInstance);

        TExecutionContext context = new TExecutionContext(
                null,
                Collections.singletonList(source.instance()),
                targetInstance,
                null, // TODO
                ErrorHandlingMode.ERROR,
                ErrorHandlingMode.ERROR,
                ErrorHandlingMode.ERROR
        );
        PValue result = new PValue(targetInstance.typeClass().underlyingType());
        try {
            cast.evaluate(context, source.value(), result);
        } catch (Exception e) {
            if (logger.isTraceEnabled()) {
                logger.trace("while casting values " + source + " to " + targetInstance + " using " + cast, e);
            }
            result = null;
        }
        return result;
    }

    private static ExpressionNode boolExpr(ExpressionNode expression) {
        expression.setPreptimeValue(new TPreptimeValue(AkBool.INSTANCE.instance()));
        return expression;
    }

    static class TopLevelCastingVistor implements PlanVisitor {

        private List<? extends ColumnContainer> targetColumns;
        private NewFolder folder;

        TopLevelCastingVistor(NewFolder folder) {
            this.folder = folder;
        }

        public void apply(PlanNode plan) {
            plan.accept(this);
        }

        // PlanVisitor

        @Override
        public boolean visitEnter(PlanNode n) {
            // set up the targets
            if (n instanceof InsertStatement) {
                InsertStatement insert = (InsertStatement) n;
                setTargets(insert.getTargetColumns());
            }
            else if (n instanceof UpdateStatement) {
                UpdateStatement update = (UpdateStatement) n;
                setTargets(update.getUpdateColumns());
                for (UpdateColumn updateColumn : update.getUpdateColumns()) {
                    Column target = updateColumn.getColumn();
                    ExpressionNode value = updateColumn.getExpression();
                    ExpressionNode casted = castTo(value, target.tInstance().typeClass(), folder);
                    if (casted != value)
                        updateColumn.setExpression(casted);
                }
            }

            // use the targets
            if (targetColumns != null) {
                if (n instanceof Project)
                    handleProject((Project) n);
                else if (n instanceof ExpressionsSource)
                    handleExpressionSource((ExpressionsSource) n);
            }
            return true;
        }

        @Override
        public boolean visitLeave(PlanNode n) {
            return true;
        }

        private void handleExpressionSource(ExpressionsSource source) {
            for (List<ExpressionNode> row : source.getExpressions()) {
                castToTarget(row, source);
            }
        }

        private void castToTarget(List<ExpressionNode> row, TypedPlan plan) {
            for (int i = 0, ncols = row.size(); i < ncols; ++i) {
                Column target = targetColumns.get(i).getColumn();
                ExpressionNode column = row.get(i);
                ExpressionNode casted = castTo(column, target.tInstance().typeClass(), folder);
                if (column != casted) {
                    row.set(i, casted);
                    plan.setTypeAt(i, casted.getPreptimeValue());
                }
            }
        }

        private void handleProject(Project source) {
            castToTarget(source.getFields(), source);
        }

        @Override
        public boolean visit(PlanNode n) {
            return true;
        }

        private void setTargets(List<? extends ColumnContainer> targetColumns) {
            assert this.targetColumns == null : this.targetColumns;
            this.targetColumns = targetColumns;
        }
    }


    private static ExpressionNode castTo(ExpressionNode expression, TClass targetClass, NewFolder folder) {
        if (targetClass == tclass(expression))
            return expression;
        return castTo(expression, targetClass.instance(), folder);
    }

    private static ExpressionNode castTo(ExpressionNode expression, TInstance targetInstance, NewFolder folder) {
        // parameters and literal nulls have no type, so just set the type -- they'll be polymorphic about it.
        if (expression instanceof ParameterExpression) {
            expression.setPreptimeValue(new TPreptimeValue(targetInstance));
            return expression;
        }
        if (expression instanceof NullSource) {
            PValueSource nullSource = PValueSources.getNullSource(targetInstance.typeClass().underlyingType());
            expression.setPreptimeValue(new TPreptimeValue(targetInstance, nullSource));
            return expression;
        }

        if (targetInstance.equalsExcludingNullable(tinst(expression)))
            return expression;
        targetInstance.setNullable(expression.getSQLtype().isNullable());
        CastExpression castExpression
                = new CastExpression(expression, targetInstance.dataTypeDescriptor(), expression.getSQLsource());
        castExpression.setPreptimeValue(new TPreptimeValue(targetInstance));
        ExpressionNode result = finishCast(castExpression, folder);
        result = folder.foldConstants(result);
        return result;
    }

    private static ExpressionNode finishCast(CastExpression castNode, NewFolder folder) {
        // If we have something like CAST( (VALUE[n] of ExpressionsSource) to FOO ),
        // refactor it to VALUE[n] of ExpressionsSource2, where ExpressionsSource2 has columns at n cast to FOO.
        ExpressionNode inner = castNode.getOperand();
        if (inner instanceof ColumnExpression) {
            ColumnExpression columnNode = (ColumnExpression) inner;
            ColumnSource source = columnNode.getTable();
            if (source instanceof ExpressionsSource) {
                ExpressionsSource expressionsTable = (ExpressionsSource) source;
                List<List<ExpressionNode>> rows = expressionsTable.getExpressions();
                int pos = columnNode.getPosition();
                TInstance castType = castNode.getPreptimeValue().instance();
                for (int i = 0, nrows = rows.size(); i < nrows; ++i) {
                    List<ExpressionNode> row = rows.get(i);
                    ExpressionNode targetColumn = row.get(pos);
                    targetColumn = castTo(targetColumn, castType, folder);
                    row.set(pos, targetColumn);
                }
            }
        }
        return inner;
    }

    private static TClass tclass(ExpressionNode operand) {
        return tclass(tinst(operand));
    }

    private static TClass tclass(TInstance tInstance) {
        return (tInstance == null) ? null : tInstance.typeClass();
    }

    private static TInstance tinst(ExpressionNode node) {
        TPreptimeValue ptv = node.getPreptimeValue();
        return ptv == null ? null : ptv.instance();
    }
}
