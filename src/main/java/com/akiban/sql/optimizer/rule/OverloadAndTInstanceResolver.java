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
import com.akiban.server.t3expressions.T3RegistryService;
import com.akiban.server.t3expressions.TCastResolver;
import com.akiban.server.types.AkType;
import com.akiban.server.types3.ErrorHandlingMode;
import com.akiban.server.types3.LazyList;
import com.akiban.server.types3.LazyListBase;
import com.akiban.server.types3.TCast;
import com.akiban.server.types3.TClass;
import com.akiban.server.types3.TExecutionContext;
import com.akiban.server.types3.TInstance;
import com.akiban.server.types3.TOverloadResult;
import com.akiban.server.types3.TPreptimeContext;
import com.akiban.server.types3.TPreptimeValue;
import com.akiban.server.types3.aksql.aktypes.AkBool;
import com.akiban.server.types3.common.types.StringAttribute;
import com.akiban.server.types3.common.types.TString;
import com.akiban.server.types3.mcompat.mtypes.MString;
import com.akiban.server.types3.pvalue.PValue;
import com.akiban.server.types3.pvalue.PValueSource;
import com.akiban.server.types3.pvalue.PValueSources;
import com.akiban.server.types3.texpressions.TValidatedAggregator;
import com.akiban.server.types3.texpressions.TValidatedScalar;
import com.akiban.server.types3.texpressions.TValidatedOverload;
import com.akiban.sql.optimizer.TypesTranslation;
import com.akiban.sql.optimizer.plan.AggregateFunctionExpression;
import com.akiban.sql.optimizer.plan.AggregateSource;
import com.akiban.sql.optimizer.plan.AnyCondition;
import com.akiban.sql.optimizer.plan.BasePlanWithInput;
import com.akiban.sql.optimizer.plan.BooleanCastExpression;
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
import com.akiban.sql.optimizer.plan.FunctionCondition;
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
import com.akiban.sql.optimizer.plan.ResolvableExpression;
import com.akiban.sql.optimizer.plan.ResultSet;
import com.akiban.sql.optimizer.plan.ResultSet.ResultField;
import com.akiban.sql.optimizer.plan.Select;
import com.akiban.sql.optimizer.plan.Sort;
import com.akiban.sql.optimizer.plan.Subquery;
import com.akiban.sql.optimizer.plan.SubqueryResultSetExpression;
import com.akiban.sql.optimizer.plan.SubquerySource;
import com.akiban.sql.optimizer.plan.SubqueryValueExpression;
import com.akiban.sql.optimizer.plan.TableSource;
import com.akiban.sql.optimizer.plan.TypedPlan;
import com.akiban.sql.optimizer.plan.UpdateStatement;
import com.akiban.sql.optimizer.plan.UpdateStatement.UpdateColumn;
import com.akiban.sql.optimizer.rule.ConstantFolder.NewFolder;
import com.akiban.sql.optimizer.rule.PlanContext.WhiteboardMarker;
import com.akiban.sql.optimizer.rule.PlanContext.DefaultWhiteboardMarker;
import com.akiban.sql.types.DataTypeDescriptor;
import com.akiban.sql.types.CharacterTypeAttributes;
import com.akiban.util.SparseArray;
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
        ResolvingVisitor resolvingVisitor = new ResolvingVisitor(plan, folder);
        folder.initResolvingVisitor(resolvingVisitor);
        plan.putWhiteboard(RESOLVER_MARKER, resolvingVisitor);
        resolvingVisitor.resolve(plan.getPlan());
        new TopLevelCastingVisitor(folder, resolvingVisitor.parametersSync).apply(plan.getPlan());
        plan.getPlan().accept(ParameterCastInliner.instance);
    }

    public static final WhiteboardMarker<ExpressionRewriteVisitor> RESOLVER_MARKER = 
        new DefaultWhiteboardMarker<ExpressionRewriteVisitor>();

    public static ExpressionRewriteVisitor getResolver(PlanContext plan) {
        return plan.getWhiteboard(RESOLVER_MARKER);
    }

    static class ResolvingVisitor implements PlanVisitor, ExpressionRewriteVisitor {

        private NewFolder folder;
        private T3RegistryService registry;
        private QueryContext queryContext;
        private ParametersSync parametersSync;

        ResolvingVisitor(PlanContext context, NewFolder folder) {
            this.folder = folder;
            SchemaRulesContext src = (SchemaRulesContext)context.getRulesContext();
            registry = src.getT3Registry();
            parametersSync = new ParametersSync(registry.getCastsResolver());
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
                        || (n instanceof Select)
                        || (n instanceof Sort)
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
                if ((n.getSQLtype() != null) &&
                    (n.getSQLtype().getCharacterAttributes() != null) &&
                    (n.getSQLtype().getCharacterAttributes().getCollationDerivation() == 
                        CharacterTypeAttributes.CollationDerivation.EXPLICIT)) {
                    // Apply result of explicit COLLATE, which will otherwise get lost.
                    // No way to mutate the existing instance, so replace entire tpv.
                    tInstance = StringAttribute.copyWithCollation(tInstance, n.getSQLtype().getCharacterAttributes());
                    tpv = new TPreptimeValue(tInstance, tpv.value());
                    n.setPreptimeValue(tpv);
                }
                if (tInstance != null) {
                    if (tInstance.nullability() == null) {
                        assert n.getSQLtype() != null : "ExpressionNode.SQLType is incorrectly null";
                        tInstance.setNullable(n.getSQLtype().isNullable());
                    }
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
                    if (botInstance == null) {
                        // type is unknown (parameter or literal NULL), so it doesn't participate in determining a
                        // type, but a cast is needed.
                        needCasts.set(field);
                        continue;
                    }
                    // If the two are the same, we know we don't need to cast them.
                    // This logic also handles the case where both are null, which is not a valid argument
                    // to resolver.commonTClass.
                    if (Objects.equal(instances[field], botInstance))
                        continue;

                    TClass topClass = tclass(instances[field]);
                    TClass botClass = tclass(botInstance);

                    TClass commonTClass = registry.getCastsResolver().commonTClass(topClass, botClass);
                    if (commonTClass == null) {
                        throw new AkibanInternalException("no common type found found between row " + (rownum-1)
                        + " and " + rownum + " at field " + field);
                    }
                    // The two rows have different TClasses at this index, so we'll need at least one of them to
                    // be casted. Only applies if both are non-null, though.
                    if ( (topClass != null) && (botClass != null) )
                        needCasts.set(field);

                    Boolean topIsNullable = (instances[field] == null) ? null : instances[field].nullability();
                    Boolean botIsNullable = botInstance.nullability();
                    // need to set a new instances[field]. Rules:
                    // - if topClass and botClass are the same as common, use picking algorithm
                    // - else, if one of them == commonTClass, use topInstance or botInstance (whichever is == common)
                    // - else, use commonTClass.instance()
                    boolean topIsCommon = (topClass == commonTClass);
                    boolean botIsCommon = (botClass == commonTClass);
                    if (topIsCommon && botIsCommon) {
                        instances[field] = topClass.pickInstance(instances[field], botInstance);
                    }
                    else if (botIsCommon) {
                        instances[field] = botInstance;
                    }
                    else if (!topIsCommon) { // this of this as "else if (topIsBottom) { <noop> } else { ..."
                        instances[field] = commonTClass.instance();
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
                        if (needCasts.get(field) && instances[field] != null) {
                            ExpressionNode orig = row.get(field);
                            ExpressionNode cast = castTo(orig, instances[field], folder, parametersSync);
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
            return finishCast(expression, folder, parametersSync);
        }

        private <V extends TValidatedOverload> ExpressionNode resolve(
                ResolvableExpression<V> expression,
                List<ExpressionNode> operands,
                OverloadResolver<V> resolver,
                boolean createPreptimeContext)
        {
            List<TPreptimeValue> operandClasses = new ArrayList<TPreptimeValue>(operands.size());
            for (ExpressionNode operand : operands)
                operandClasses.add(operand.getPreptimeValue());

            OverloadResult<V> resolutionResult = resolver.get(expression.getFunction(), operandClasses);

            // cast operands
            for (int i = 0, operandsSize = operands.size(); i < operandsSize; i++) {
                TInstance targetType = resolutionResult.getTypeClass(i);
                if (targetType != null) {
                    ExpressionNode operand = castTo(operands.get(i), targetType, folder, parametersSync);
                    operands.set(i, operand);
                }
            }

            V overload = resolutionResult.getOverload();
            expression.setResolved(overload);

            final List<TPreptimeValue> operandValues = new ArrayList<TPreptimeValue>(operands.size());
            List<TInstance> operandInstances = new ArrayList<TInstance>(operands.size());
            boolean anyOperandsNullable = false;
            for (ExpressionNode operand : operands) {
                TPreptimeValue preptimeValue = operand.getPreptimeValue();
                operandValues.add(preptimeValue);
                operandInstances.add(preptimeValue.instance());
                if (Boolean.TRUE.equals(preptimeValue.isNullable()))
                    anyOperandsNullable = true;
            }

            TOverloadResult overloadResultStrategy = overload.resultStrategy();
            TInstance resultInstance;
            TInstance castTo;

            TPreptimeContext context;
            if (createPreptimeContext) {
                context = new TPreptimeContext(operandInstances, queryContext);
                expression.setPreptimeContext(context);
            }
            else {
                context = null;
            }
            switch (overloadResultStrategy.category()) {
            case CUSTOM:
                TInstance castSource = overloadResultStrategy.customRuleCastSource();
                if (context == null)
                    context = new TPreptimeContext(operandInstances, queryContext);
                expression.setPreptimeContext(context);
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
            resultInstance = resultInstance.copy(); // May change nullability.
            if (createPreptimeContext)
                context.setOutputType(resultInstance);

            if (resultInstance.nullability() == null) {
                resultInstance.setNullable(anyOperandsNullable);
            }

            expression.setPreptimeValue(new TPreptimeValue(resultInstance));

            ExpressionNode resultExpression;
            if (castTo == null) {
                resultExpression = expression;
            }
            else {
                resultExpression = castTo(expression, castTo, folder, parametersSync);
                resultInstance = castTo;
            }

            if (expression instanceof FunctionCondition) {
                // Didn't know whether function would return boolean or not earlier,
                // so just assumed it would.
                if (resultInstance.typeClass() != AkBool.INSTANCE) {
                    castTo = AkBool.INSTANCE.instance();
                    castTo.setNullable(resultInstance.nullability());
                    resultExpression = castTo(resultExpression, castTo, folder, parametersSync);
                    resultInstance = castTo;
                }
            }

            return resultExpression;
        }

        ExpressionNode handleFunctionExpression(FunctionExpression expression) {
            List<ExpressionNode> operands = expression.getOperands();
            ExpressionNode result = resolve(expression, operands, registry.getScalarsResolver(), true);

            TValidatedScalar overload = expression.getResolved();
            TPreptimeContext context = expression.getPreptimeContext();

            final List<TPreptimeValue> operandValues = new ArrayList<TPreptimeValue>(operands.size());
            for (ExpressionNode operand : operands) {
                TPreptimeValue preptimeValue = operand.getPreptimeValue();
                operandValues.add(preptimeValue);
            }
            overload.finishPreptimePhase(context);

            // Put the preptime value, possibly including nullness, into the expression. The constant folder
            // will use it.
            LazyList<TPreptimeValue> lazyInputs = new LazyListBase<TPreptimeValue>() {
                @Override
                public TPreptimeValue get(int i) {
                    return operandValues.get(i);
                }

                @Override
                public int size() {
                    return operandValues.size();
                }
            };

            TPreptimeValue constantTpv = overload.evaluateConstant(context, overload.filterInputs(lazyInputs));
            if (constantTpv != null) {
                TPreptimeValue oldTpv = expression.getPreptimeValue();
                assert oldTpv.instance().equals(constantTpv.instance())
                        : oldTpv.instance() + " != " + constantTpv.instance();
                expression.setPreptimeValue(constantTpv);
            }

            SparseArray<Object> values = context.getValues();
            if ((values != null) && !values.isEmpty())
                expression.setPreptimeValues(values);

            return result;
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

            TInstance commonInstance = commonInstance(registry.getCastsResolver(), tinst(thenExpr), tinst(elseExpr));
            if (commonInstance == null)
                return new ConstantExpression(null, AkType.NULL); // both types are unknown, so result is unknown

            thenExpr = castTo(thenExpr, commonInstance, folder, parametersSync);
            elseExpr = castTo(elseExpr, commonInstance, folder, parametersSync);

            expression.setThenExpression(thenExpr);
            expression.setElseExpression(elseExpr);

            expression.setPreptimeValue(new TPreptimeValue(commonInstance));
            return expression;
        }

        ExpressionNode handleAggregateFunctionExpression(AggregateFunctionExpression expression) {
            List<ExpressionNode> operands = new ArrayList<ExpressionNode>();
            ExpressionNode operand = expression.getOperand();
            if (operand != null)
                operands.add(operand);
            ExpressionNode result = resolve(expression, operands, registry.getAggregatesResolver(), false);
            if (operand != null)
                expression.setOperand(operands.get(0)); // in case the original operand was casted
            return result;
        }

        ExpressionNode handleExistsCondition(ExistsCondition expression) {
            return boolExpr(expression, true);
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
            TPreptimeValue tpv = new TPreptimeValue(TypesTranslation.toTInstance(expression.getSQLtype()));
            expression.setPreptimeValue(tpv);
            return expression;
        }

        ExpressionNode handleAnyCondition(AnyCondition expression) {
            return boolExpr(expression, true);
        }

        ExpressionNode handleComparisonCondition(ComparisonCondition expression) {
            ExpressionNode left = expression.getLeft();
            ExpressionNode right = expression.getRight();
            TInstance leftTInst = tinst(left);
            TInstance rightTInst = tinst(right);
            if (TClass.comparisonNeedsCasting(leftTInst, rightTInst)) {
                boolean needCasts = true;
                TCastResolver casts = registry.getCastsResolver();
                if ( (left.getClass() == ColumnExpression.class)&& (right.getClass() == ConstantExpression.class)) {
                    // Left is a Column, right is a Constant. Ideally, we'd like to keep the Column as a Column,
                    // and not a CAST(Column AS _) -- otherwise, we can't use it in an index lookup.
                    // So, try to cast the const to the column's type. To do this, CAST(Const -> Column) must be
                    // indexFriendly, *and* casting this result back to the original Const type must equal the same
                    // const.
                    if (casts.isIndexFriendly(tclass(leftTInst), tclass(rightTInst))) {
                        TInstance columnType = tinst(left);
                        TInstance constType = tinst(right);
                        TCast constToCol = casts.cast(constType, columnType);
                        if (constToCol != null) {
                            TCast colToConst = casts.cast(columnType, constType);
                            if (colToConst != null) {
                                TPreptimeValue constValue = right.getPreptimeValue();
                                PValueSource asColType = castValue(constToCol, constValue, columnType);
                                TPreptimeValue asColTypeTpv = (asColType == null)
                                        ? null
                                        : new TPreptimeValue(columnType, asColType);
                                PValueSource backToConstType = castValue(colToConst, asColTypeTpv, constType);
                                if (PValueSources.areEqual(constValue.value(), backToConstType, constType)) {
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
                    TInstance common = commonInstance(casts, left, right);
                    if (common == null) {
                        // TODO this means we have something like '? = ?' or '? = NULL'. What to do? Varchar for now?
                        common = MString.VARCHAR.instance();
                    }
                    left = castTo(left, common, folder, parametersSync);
                    right = castTo(right, common, folder, parametersSync);
                    expression.setLeft(left);
                    expression.setRight(right);
                }
            }

            return boolExpr(expression, isNullable(left) || isNullable(right));
        }

        private boolean isNullable(ExpressionNode node) {
            return tinst(node).nullability();
        }

        ExpressionNode handleColumnExpression(ColumnExpression expression) {
            Column column = expression.getColumn();
            ColumnSource columnSource = expression.getTable();
            if (column != null) {
                assert columnSource instanceof TableSource : columnSource;
                TInstance columnInstance = column.tInstance();
                if ((Boolean.FALSE == columnInstance.nullability()) &&
                    (expression.getSQLtype() != null) &&
                    (expression.getSQLtype().isNullable())) {
                    // With an outer join, the column can still be nullable.
                    columnInstance = columnInstance.copy();
                    columnInstance.setNullable(Boolean.TRUE);
                }
                expression.setPreptimeValue(new TPreptimeValue(columnInstance));
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
            boolean nullable = isNullable(expression.getOperand());
            if (!nullable) {
                List<ExpressionNode> expressions = expression.getExpressions();
                for (int i = 0, expressionsSize = expressions.size(); (!nullable) && i < expressionsSize; i++) {
                    ExpressionNode rhs = expressions.get(i);
                    nullable = isNullable(rhs);
                }
            }
            return boolExpr(expression, nullable);
        }

        ExpressionNode handleParameterCondition(ParameterCondition expression) {
            parametersSync.uninferred(expression);
            TInstance instance = AkBool.INSTANCE.instance();
            instance.setNullable(true);
            return castTo(expression, instance,
                          folder, parametersSync);
        }

        ExpressionNode handleParameterExpression(ParameterExpression expression) {
            parametersSync.uninferred(expression);
            return expression;
        }

        ExpressionNode handleBooleanOperationExpression(BooleanOperationExpression expression) {
            boolean isNullable;
            DataTypeDescriptor sqLtype = expression.getSQLtype();
            isNullable = sqLtype == null // TODO if no SQL type, assume nullable for now
                    || sqLtype.isNullable(); // TODO rely on the previous type computer for now
            return boolExpr(expression, isNullable);
        }

        ExpressionNode handleBooleanConstantExpression(BooleanConstantExpression expression) {
            return boolExpr(expression, expression.getValue() == null);
        }

        ExpressionNode handleConstantExpression(ConstantExpression expression) {
            // will be lazily loaded as necessary
            return expression;
        }

        private static PValueSource pval(ExpressionNode expression) {
            return expression.getPreptimeValue().value();
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

    private static ExpressionNode boolExpr(ExpressionNode expression, Boolean nullable) {
        TInstance instance = AkBool.INSTANCE.instance();
        instance.setNullable(nullable);
        expression.setPreptimeValue(new TPreptimeValue(instance));
        return expression;
    }

    static class TopLevelCastingVisitor implements PlanVisitor {

        private List<? extends ColumnContainer> targetColumns;
        private NewFolder folder;
        private ParametersSync parametersSync;

        TopLevelCastingVisitor(NewFolder folder, ParametersSync parametersSync) {
            this.folder = folder;
            this.parametersSync = parametersSync;
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
                    ExpressionNode casted = castTo(value, target.tInstance().typeClass(), folder, parametersSync);
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
                ExpressionNode casted = castTo(column, target.tInstance(), folder, parametersSync);
                row.set(i, casted);
                plan.setTypeAt(i, casted.getPreptimeValue());
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

    private static class ParameterCastInliner implements PlanVisitor, ExpressionRewriteVisitor {

        private static final ParameterCastInliner instance = new ParameterCastInliner();

        // ExpressionRewriteVisitor

        @Override
        public ExpressionNode visit(ExpressionNode n) {
            if (n instanceof CastExpression) {
                CastExpression cast = (CastExpression) n;
                ExpressionNode operand = cast.getOperand();
                if (operand instanceof ParameterExpression) {
                    TInstance castTarget = tinst(cast);
                    TInstance parameterType = tinst(operand);
                    if (castTarget.equals(parameterType))
                        n = operand;
                }
            }
            return n;
        }

        @Override
        public boolean visitChildrenFirst(ExpressionNode n) {
            return false;
        }

        // PlanVisitor

        @Override
        public boolean visitEnter(PlanNode n) {
            return true;
        }

        @Override
        public boolean visitLeave(PlanNode n) {
            return true;
        }

        @Override
        public boolean visit(PlanNode n) {
            return true;
        }
    }

    private static ExpressionNode castTo(ExpressionNode expression, TClass targetClass, NewFolder folder,
                                  ParametersSync parametersSync)
    {
        if (targetClass == tclass(expression))
            return expression;
        return castTo(expression, targetClass.instance(), folder, parametersSync);
    }

    private static ExpressionNode castTo(ExpressionNode expression, TInstance targetInstance, NewFolder folder,
                                  ParametersSync parametersSync)
    {
        // parameters and literal nulls have no type, so just set the type -- they'll be polymorphic about it.
        if (expression instanceof ParameterExpression) {
            targetInstance = targetInstance.copy();
            targetInstance.setNullable(true);
            CastExpression castExpression = 
                newCastExpression(expression, targetInstance);
            castExpression.setPreptimeValue(new TPreptimeValue(targetInstance));
            parametersSync.set(expression, targetInstance);
            return castExpression;
        }
        if (expression instanceof NullSource) {
            PValueSource nullSource = PValueSources.getNullSource(targetInstance.typeClass().underlyingType());
            expression.setPreptimeValue(new TPreptimeValue(targetInstance, nullSource));
            return expression;
        }

        if (equalForCast(targetInstance, tinst(expression)))
            return expression;
        DataTypeDescriptor sqlType = expression.getSQLtype();
        targetInstance = targetInstance.copy();
        targetInstance.setNullable(sqlType == null || sqlType.isNullable());
        CastExpression castExpression = 
            newCastExpression(expression, targetInstance);
        castExpression.setPreptimeValue(new TPreptimeValue(targetInstance));
        ExpressionNode result = finishCast(castExpression, folder, parametersSync);
        result = folder.foldConstants(result);
        return result;
    }
    
    private static boolean equalForCast(TInstance target, TInstance source) {
        if (source == null)
            return false;
        if (!target.typeClass().equals(source.typeClass()))
            return false;
        if (target.typeClass() instanceof TString) {
            // Operations between strings do not require that the
            // charsets / collations be the same.
            return (target.attribute(StringAttribute.LENGTH) == 
                    source.attribute(StringAttribute.LENGTH));
        }
        return target.equalsExcludingNullable(source);
    }

    private static CastExpression newCastExpression(ExpressionNode expression, TInstance targetInstance) {
        if (targetInstance.typeClass() == AkBool.INSTANCE)
            // Allow use as a condition.
            return new BooleanCastExpression(expression, targetInstance.dataTypeDescriptor(), expression.getSQLsource());
        else
            return new CastExpression(expression, targetInstance.dataTypeDescriptor(), expression.getSQLsource());
    }

    private static ExpressionNode finishCast(CastExpression castNode, NewFolder folder, ParametersSync parametersSync) {
        // If we have something like CAST( (VALUE[n] of ExpressionsSource) to FOO ),
        // refactor it to VALUE[n] of ExpressionsSource2, where ExpressionsSource2 has columns at n cast to FOO.
        ExpressionNode inner = castNode.getOperand();
        ExpressionNode result = castNode;
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
                    targetColumn = castTo(targetColumn, castType, folder, parametersSync);
                    row.set(pos, targetColumn);
                }
                result = columnNode;
                result.setPreptimeValue(castNode.getPreptimeValue());
                expressionsTable.getFieldTInstances()[pos] = castType;
            }
        }
        return result;
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

    private static TInstance commonInstance(TCastResolver resolver, ExpressionNode left, ExpressionNode right) {
        return commonInstance(resolver, tinst(left), tinst(right));
    }

    private static TInstance commonInstance(TCastResolver resolver, TInstance left, TInstance right) {
        if (left == null && right == null)
            return null;
        else if (left == null)
            return right;
        else if (right == null)
            return left;

        TClass leftTClass = left.typeClass();
        TClass rightTClass = right.typeClass();
        if (leftTClass == rightTClass)
            return leftTClass.pickInstance(left, right);
        TClass commonClass = resolver.commonTClass(leftTClass, rightTClass);
        if (commonClass == null)
            throw error("couldn't determine a type for CASE expression");
        if (commonClass == leftTClass)
            return left;
        if (commonClass == rightTClass)
            return right;
        return commonClass.instance();
    }

    private static RuntimeException error(String message) {
        throw new RuntimeException(message); // TODO what actual error type?
    }

    /**
     * Helper class for keeping various instances of the same parameter in sync, in terms of their TInstance. So for
     * instance, in an expression IF($0 == $1, $1, $0) we'd want both $0s to have the same TInstance, and ditto for
     * both $1s.
     */
    private static class ParametersSync {
        private TCastResolver resolver;
        private SparseArray<List<ExpressionNode>> instancesMap;

        private ParametersSync(TCastResolver resolver) {
            this.resolver = resolver;
            this.instancesMap = new SparseArray<List<ExpressionNode>>();
        }

        public void uninferred(ParameterExpression parameterExpression) {
            assert parameterExpression.getPreptimeValue() == null : parameterExpression;
            List<ExpressionNode> siblings = siblings(parameterExpression);
            if (siblings.isEmpty()) {
                parameterExpression.setPreptimeValue(new TPreptimeValue());
            }
            else {
                TPreptimeValue preptimeValue = siblings.get(0).getPreptimeValue();
                parameterExpression.setPreptimeValue(preptimeValue);
            }
            siblings.add(parameterExpression);
        }

        private List<ExpressionNode> siblings(ParameterExpression parameterNode) {
            int pos = parameterNode.getPosition();
            List<ExpressionNode> siblings = instancesMap.get(pos);
            if (siblings == null) {
                siblings = new ArrayList<ExpressionNode>(4); // guess at capacity. this should be plenty
                instancesMap.set(pos, siblings);
            }
            return siblings;
        }

        public void set(ExpressionNode node, TInstance tInstance) {
            List<ExpressionNode> siblings = siblings((ParameterExpression) node);
            TPreptimeValue sharedTpv = siblings.get(0).getPreptimeValue();
            TInstance previousInstance = sharedTpv.instance();
            tInstance = commonInstance(resolver, tInstance, previousInstance);
            sharedTpv.instance(tInstance);
        }
    }
}
