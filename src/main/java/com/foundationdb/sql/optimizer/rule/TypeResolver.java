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

import com.foundationdb.ais.model.Column;
import com.foundationdb.ais.model.ColumnContainer;
import com.foundationdb.ais.model.Routine;
import com.foundationdb.qp.operator.QueryContext;
import com.foundationdb.server.error.AkibanInternalException;
import com.foundationdb.server.error.SQLParserInternalException;
import com.foundationdb.server.types.service.OverloadResolver;
import com.foundationdb.server.types.service.OverloadResolver.OverloadResult;
import com.foundationdb.server.types.service.TypesRegistryService;
import com.foundationdb.server.types.service.TCastResolver;
import com.foundationdb.server.types.ErrorHandlingMode;
import com.foundationdb.server.types.LazyList;
import com.foundationdb.server.types.LazyListBase;
import com.foundationdb.server.types.TCast;
import com.foundationdb.server.types.TClass;
import com.foundationdb.server.types.TExecutionContext;
import com.foundationdb.server.types.TInstance;
import com.foundationdb.server.types.TKeyComparable;
import com.foundationdb.server.types.TOverloadResult;
import com.foundationdb.server.types.TPreptimeContext;
import com.foundationdb.server.types.TPreptimeValue;
import com.foundationdb.server.types.aksql.aktypes.AkBool;
import com.foundationdb.server.types.common.types.StringAttribute;
import com.foundationdb.server.types.common.types.TBinary;
import com.foundationdb.server.types.common.types.TString;
import com.foundationdb.server.types.common.types.TypesTranslator;
import com.foundationdb.server.types.value.Value;
import com.foundationdb.server.types.value.ValueSource;
import com.foundationdb.server.types.value.ValueSources;
import com.foundationdb.server.types.texpressions.TValidatedScalar;
import com.foundationdb.server.types.texpressions.TValidatedOverload;
import com.foundationdb.sql.StandardException;
import com.foundationdb.sql.optimizer.plan.*;
import com.foundationdb.sql.optimizer.plan.ResultSet.ResultField;
import com.foundationdb.sql.optimizer.plan.UpdateStatement.UpdateColumn;
import com.foundationdb.sql.optimizer.rule.ConstantFolder.Folder;
import com.foundationdb.sql.optimizer.rule.PlanContext.WhiteboardMarker;
import com.foundationdb.sql.optimizer.rule.PlanContext.DefaultWhiteboardMarker;
import com.foundationdb.sql.parser.ValueNode;
import com.foundationdb.sql.types.DataTypeDescriptor;
import com.foundationdb.sql.types.TypeId;
import com.foundationdb.sql.types.CharacterTypeAttributes;
import com.foundationdb.util.SparseArray;
import com.google.common.base.Objects;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.BitSet;
import java.util.Collections;
import java.util.List;

public final class TypeResolver extends BaseRule {
    private static final Logger logger = LoggerFactory.getLogger(TypeResolver.class);

    @Override
    protected Logger getLogger() {
        return logger;
    }

    @Override
    public void apply(PlanContext plan) {
        Folder folder = new Folder(plan);
        ResolvingVisitor resolvingVisitor = new ResolvingVisitor(plan, folder);
        folder.initResolvingVisitor(resolvingVisitor);
        plan.putWhiteboard(RESOLVER_MARKER, resolvingVisitor);
        resolvingVisitor.resolve(plan.getPlan());
        new TopLevelCaster(folder, resolvingVisitor.parametersSync).apply(plan.getPlan());
        plan.getPlan().accept(ParameterCastInliner.instance);
    }

    public static final WhiteboardMarker<ExpressionRewriteVisitor> RESOLVER_MARKER = 
        new DefaultWhiteboardMarker<>();

    public static ExpressionRewriteVisitor getResolver(PlanContext plan) {
        return plan.getWhiteboard(RESOLVER_MARKER);
    }

    public static class ResolvingVisitor implements PlanVisitor, ExpressionRewriteVisitor {

        private Folder folder;
        private TypesRegistryService registry;
        private TypesTranslator typesTranslator;
        private QueryContext queryContext;
        private ParametersSync parametersSync;

        public ResolvingVisitor(PlanContext context, Folder folder) {
            this.folder = folder;
            SchemaRulesContext src = (SchemaRulesContext)context.getRulesContext();
            registry = src.getTypesRegistry();
            typesTranslator = src.getTypesTranslator();
            parametersSync = new ParametersSync(registry.getCastsResolver());
            this.queryContext = context.getQueryContext();
        }

        public void resolve(PlanNode root) {
            root.accept(this);
            parametersSync.updateTypes(typesTranslator);
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
                updateResultFields(n, ((ResultSet)n).getFields());
            }
            else if (n instanceof DMLStatement) {
                updateResultFields(n, ((DMLStatement)n).getResultField());
            }
            else if (n instanceof ExpressionsSource) {
                handleExpressionsSource((ExpressionsSource)n);
            }
            else if( n instanceof SetPlanNode){
                updateSetNode((SetPlanNode)n);
            }
            return true;
        }

        private void updateResultFields(PlanNode n, List<ResultField> rsFields) {
            if (rsFields == null) return;
            TypedPlan typedInput = findTypedPlanNode(n);
            if (typedInput != null) {
                assert rsFields.size() == typedInput.nFields() : rsFields + " not applicable to " + typedInput;
                for (int i = 0, size = rsFields.size(); i < size; i++) {
                    ResultField rsField = rsFields.get(i);
                    rsField.setType(typedInput.getTypeAt(i));
                }
            }
            else {
                logger.warn("no Project node found for result fields: {}", n);
            }
        }

        private TypedPlan findTypedPlanNode(PlanNode n) {
            while (true) {
                if (n instanceof TypedPlan)
                    return (TypedPlan) n;
                if ( (n instanceof ResultSet)
                        || (n instanceof DMLStatement)
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
            else if (n instanceof RoutineExpression)
                n = handleRoutineExpression((RoutineExpression) n);
            else if (n instanceof ColumnDefaultExpression)
                n = handleColumnDefaultExpression((ColumnDefaultExpression) n);
            else
                logger.warn("unrecognized ExpressionNode subclass: {}", n.getClass());

            n = folder.foldConstants(n);
            // Set nullability of TInstance if it hasn't been given explicitly
            // At the same time, update the node's DataTypeDescriptor to match its TInstance
            TPreptimeValue tpv = n.getPreptimeValue();
            if (tpv != null) {
                TInstance type = tpv.type();
                if ((n.getSQLtype() != null) &&
                    (n.getSQLtype().getCharacterAttributes() != null) &&
                    (n.getSQLtype().getCharacterAttributes().getCollationDerivation() == 
                        CharacterTypeAttributes.CollationDerivation.EXPLICIT)) {
                    // Apply result of explicit COLLATE, which will otherwise get lost.
                    // No way to mutate the existing instance, so replace entire tpv.
                    type = StringAttribute.copyWithCollation(type, n.getSQLtype().getCharacterAttributes());
                    tpv = new TPreptimeValue(type, tpv.value());
                    n.setPreptimeValue(tpv);
                }
                if (type != null) {
                    DataTypeDescriptor newDtd = type.dataTypeDescriptor();
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
            BitSet widened = new BitSet(nfields);
            
            // First pass. Assume that instances[f] contains the TInstance of the top operand at field f. This could
            // be null, if that operand doesn't have a type; this is definitely true of the first row, but it can
            // also happen if an ExpressionNode is a constant NULL.
            for (int rownum = 0, expressionsSize = rows.size(); rownum < expressionsSize; rownum++) {
                List<ExpressionNode> row = rows.get(rownum);
                assert row.size() == nfields : "jagged rows: " + node;
                for (int field = 0; field < nfields; ++field) {
                    TInstance botInstance = type(row.get(field));
                    special:
                    if (botInstance == null) {
                        // type is unknown (parameter or literal NULL), so it doesn't participate in determining a
                        // type, but a cast is needed.
                        needCasts.set(field);
                        // If it is a parameter, it needs to be allowed to be wider than
                        // any of the existing values, while still consistent with them.
                        if (row.get(field) instanceof ParameterExpression) {
                            // Force use of commonTClass(existing,null) below to widen.
                            if (!widened.get(field)) {
                                widened.set(field);
                                break special;
                            }
                        }
                        continue;
                    }
                    else if (instances[field] == null) {
                        // Take type from first non-NULL, unless we have to widen,
                        // which commonTClass(null,expr) will take care of.
                        if (widened.get(field)) {
                            break special;
                        }
                        instances[field] = botInstance;
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
                    // be casted. Also the common class will be the widest comparable.
                    needCasts.set(field);
                    widened.set(field);

                    boolean eitherIsNullable;
                    if (botInstance == null)
                        eitherIsNullable = true;
                    else
                        eitherIsNullable = botInstance.nullability();
                    if ( (!eitherIsNullable) && (instances[field] != null)) {
                        // bottom is not nullable, and there is a top. See if it's nullable
                        eitherIsNullable = instances[field].nullability();
                    }
                    
                    // need to set a new instances[field]. Rules:
                    // - if topClass and botClass are the same as common, use picking algorithm
                    // - else, if one of them == commonTClass, use topInstance or botInstance (whichever is == common)
                    // - else, use commonTClass.instance()
                    boolean topIsCommon = (topClass == commonTClass);
                    boolean botIsCommon = (botClass == commonTClass);
                    if (topIsCommon && botIsCommon) {
                        // TODO: The special case here for TClass VARCHAR with mismatched charsets
                        // is a limitation of the TClass#pickInstance, as there is no current way
                        // to create a common TInstance for TString with difference charsets. 
                        if (commonTClass instanceof TString &&
                            botInstance.attribute(StringAttribute.CHARSET) != instances[field].attribute(StringAttribute.CHARSET)) {
                            ;
                        }
                        else {    
                            instances[field] = topClass.pickInstance(instances[field], botInstance);
                        }
                    }
                    else if (botIsCommon) {
                        instances[field] = botInstance;
                    }
                    else if (!topIsCommon) { // this of this as "else if (topIsBottom) { <noop> } else { ..."
                        instances[field] = commonTClass.instance(eitherIsNullable);
                    }

                    // See if the top instance is not nullable but should be
                    if (instances[field] != null) {
                        instances[field] = instances[field].withNullable(eitherIsNullable);
                    }
                }
            }

            // See if we need any casts
            if (!needCasts.isEmpty()) {
                for (int field = 0; field < nfields; field++) {
                    if (widened.get(field)) {
                        // A parameter should get a really wide VARCHAR so that it
                        // won't be truncated because of other non-parameters.
                        // Also make sure it's VARBINARY, as BINARY means pad, which
                        // we don't want here.
                        TClass tclass = TInstance.tClass(instances[field]);
                        if (tclass instanceof TString) {
                            if (((TString)tclass).getFixedLength() < 0) {
                                instances[field] = 
                                    typesTranslator.typeClassForString()
                                      .instance(Integer.MAX_VALUE,
                                                instances[field].attribute(StringAttribute.CHARSET),
                                                instances[field].attribute(StringAttribute.COLLATION),
                                                instances[field].nullability());
                            }
                        }
                        else if (tclass instanceof TBinary) {
                            if (((TBinary)tclass).getDefaultLength() < 0) {
                                instances[field] = 
                                    typesTranslator.typeClassForBinary()
                                      .instance(Integer.MAX_VALUE,
                                                instances[field].nullability());
                            }
                        }
                    }
                }
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
            TInstance type = typesTranslator.typeForSQLType(dtd);
            expression.setPreptimeValue(new TPreptimeValue(type));
            if (expression.getOperand() instanceof ParameterExpression) {
                parametersSync.set(expression.getOperand(), type);
            }
            return finishCast(expression, folder, parametersSync);
        }

        private <V extends TValidatedOverload> ExpressionNode resolve(
                ResolvableExpression<V> expression,
                List<ExpressionNode> operands,
                OverloadResolver<V> resolver,
                boolean createPreptimeContext)
        {
            List<TPreptimeValue> operandClasses = new ArrayList<>(operands.size());
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

            final List<TPreptimeValue> operandValues = new ArrayList<>(operands.size());
            List<TInstance> operandInstances = new ArrayList<>(operands.size());
            boolean anyOperandsNullable = false;
            for (ExpressionNode operand : operands) {
                TPreptimeValue preptimeValue = operand.getPreptimeValue();
                operandValues.add(preptimeValue);
                operandInstances.add(preptimeValue.type());
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
                TInstance castSource = overloadResultStrategy.customRuleCastSource(anyOperandsNullable);
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
                resultInstance = overloadResultStrategy.fixed(anyOperandsNullable);
                castTo = null;
                break;
            case PICKING:
                resultInstance = resolutionResult.getPickedInstance();
                castTo = null;
                break;
            default:
                throw new AssertionError(overloadResultStrategy.category());
            }
            if (createPreptimeContext)
                context.setOutputType(resultInstance);

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
                    castTo = AkBool.INSTANCE.instance(resultInstance.nullability());
                    resultExpression = castTo(resultExpression, castTo, folder, parametersSync);
                }
            }

            return resultExpression;
        }

        ExpressionNode handleFunctionExpression(FunctionExpression expression) {
            List<ExpressionNode> operands = expression.getOperands();
            ExpressionNode result = resolve(expression, operands, registry.getScalarsResolver(), true);

            TValidatedScalar overload = expression.getResolved();
            TPreptimeContext context = expression.getPreptimeContext();

            final List<TPreptimeValue> operandValues = new ArrayList<>(operands.size());
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
                assert oldTpv.type().equals(constantTpv.type())
                        : oldTpv.type() + " != " + constantTpv.type();
                expression.setPreptimeValue(constantTpv);
            }

            SparseArray<Object> values = context.getValues();
            if ((values != null) && !values.isEmpty())
                expression.setPreptimeValues(values);

            return result;
        }

        ExpressionNode handleIfElseExpression(IfElseExpression expression) {
            ConditionList conditions = expression.getTestConditions();
            ExpressionNode thenExpr = expression.getThenExpression();
            ExpressionNode elseExpr = expression.getElseExpression();

            // constant-fold if the condition is constant
            if (conditions.size() == 1) {
                ValueSource conditionVal = pval(conditions.get(0));
                if (conditionVal != null) {
                    boolean conditionMet = conditionVal.getBoolean(false);
                    return conditionMet ? thenExpr : elseExpr;
                }
            }

            TInstance commonInstance = commonInstance(registry.getCastsResolver(), type(thenExpr), type(elseExpr));
            if (commonInstance == null)
                return ConstantExpression.typedNull(null, null, null);

            thenExpr = castTo(thenExpr, commonInstance, folder, parametersSync);
            elseExpr = castTo(elseExpr, commonInstance, folder, parametersSync);

            expression.setThenExpression(thenExpr);
            expression.setElseExpression(elseExpr);

            expression.setPreptimeValue(new TPreptimeValue(commonInstance));
            return expression;
        }

        ExpressionNode handleAggregateFunctionExpression(AggregateFunctionExpression expression) {
            List<ExpressionNode> operands = new ArrayList<>();
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
            DataTypeDescriptor sqlType = expression.getSQLtype();
            if (sqlType.isRowMultiSet()) {
                setMissingRowMultiSetColumnTypes(sqlType, expression.getSubquery());
            }
            TPreptimeValue tpv = new TPreptimeValue(typesTranslator.typeForSQLType(sqlType));
            expression.setPreptimeValue(tpv);
            return expression;
        }

        // If a RowMultiSet column is a function expression, it won't have an SQL type
        // when the RowMultiSet type is built. Must get it now.
        static void setMissingRowMultiSetColumnTypes(DataTypeDescriptor sqlType,
                                                     Subquery subquery) {
            if (subquery.getInput() instanceof ResultSet) {
                List<ResultField> fields = ((ResultSet)subquery.getInput()).getFields();
                DataTypeDescriptor[] columnTypes = ((TypeId.RowMultiSetTypeId)sqlType.getTypeId()).getColumnTypes();
                for (int i = 0; i < columnTypes.length; i++) {
                    if (columnTypes[i] == null) {
                        // TInstance should have been computed earlier in walk.
                        columnTypes[i] = fields.get(i).getSQLtype();
                    }
                }
            }
        }

        ExpressionNode handleAnyCondition(AnyCondition expression) {
            return boolExpr(expression, true);
        }

        ExpressionNode handleComparisonCondition(ComparisonCondition expression) {
            ExpressionNode left = expression.getLeft();
            ExpressionNode right = expression.getRight();
            TInstance leftTInst = type(left);
            TInstance rightTInst = type(right);
            boolean nullable = isNullable(left) || isNullable(right);
            TKeyComparable keyComparable = registry.getKeyComparable(tclass(leftTInst), tclass(rightTInst));
            if (keyComparable != null) {
                expression.setKeyComparable(keyComparable);
            }
            else if (TClass.comparisonNeedsCasting(leftTInst, rightTInst)) {
                boolean needCasts = true;
                TCastResolver casts = registry.getCastsResolver();
                if ( (left.getClass() == ColumnExpression.class)&& (right.getClass() == ConstantExpression.class)) {
                    // Left is a Column, right is a Constant. Ideally, we'd like to keep the Column as a Column,
                    // and not a CAST(Column AS _) -- otherwise, we can't use it in an index lookup.
                    // So, try to cast the const to the column's type. To do this, CAST(Const -> Column) must be
                    // indexFriendly, *and* casting this result back to the original Const type must equal the same
                    // const.
                    if (rightTInst == null) {
                        // literal null, so a comparison always returns UNKNOWN
                        return new BooleanConstantExpression(null);
                    }
                    if (casts.isIndexFriendly(tclass(leftTInst), tclass(rightTInst))) {
                        TInstance columnType = type(left);
                        TInstance constType = type(right);
                        TCast constToCol = casts.cast(constType, columnType);
                        if (constToCol != null) {
                            TCast colToConst = casts.cast(columnType, constType);
                            if (colToConst != null) {
                                TPreptimeValue constValue = right.getPreptimeValue();
                                ValueSource asColType = castValue(constToCol, constValue, columnType);
                                TPreptimeValue asColTypeTpv = (asColType == null)
                                        ? null
                                        : new TPreptimeValue(columnType, asColType);
                                ValueSource backToConstType = castValue(colToConst, asColTypeTpv, constType);
                                if (ValueSources.areEqual(constValue.value(), backToConstType, constType)) {
                                    TPreptimeValue constTpv = new TPreptimeValue(columnType, asColType);
                                    ConstantExpression constCasted = new ConstantExpression(constTpv);
                                    expression.setRight(constCasted);
                                    assert columnType.equals(type(expression.getRight()));
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
                        common = typesTranslator.typeForString();
                    }
                    left = castTo(left, common, folder, parametersSync);
                    right = castTo(right, common, folder, parametersSync);
                    expression.setLeft(left);
                    expression.setRight(right);
                }
            }

            return boolExpr(expression, nullable);
        }

        private boolean isNullable(ExpressionNode node) {
            TInstance type = type(node);
            return type == null || type.nullability();
        }

        ExpressionNode handleColumnExpression(ColumnExpression expression) {
            Column column = expression.getColumn();
            ColumnSource columnSource = expression.getTable();
            if (column != null) {
                assert columnSource instanceof TableSource : columnSource;
                TInstance columnInstance = column.getType();
                if ((Boolean.FALSE == columnInstance.nullability()) &&
                    (expression.getSQLtype() != null) &&
                    (expression.getSQLtype().isNullable())) {
                    // With an outer join, the column can still be nullable.
                    columnInstance = columnInstance.withNullable(true);
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
                    tpv = new TPreptimeValue(typesTranslator.typeForSQLType(expression.getSQLtype()));
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
                    TInstance type = exprsTable.getTypeAt(expression.getPosition());
                    tpv = new TPreptimeValue(type);
                }
                expression.setPreptimeValue(tpv);
            }
            else if (columnSource instanceof CreateAs){
                expression.setPreptimeValue(new TPreptimeValue(null));
                return expression;
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
            TInstance type = AkBool.INSTANCE.instance(true);
            return castTo(expression, type,
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
            return boolExpr(expression, expression.isNullable());
        }

        ExpressionNode handleConstantExpression(ConstantExpression expression) {
            // will be lazily loaded as necessary
            return expression;
        }

        ExpressionNode handleRoutineExpression(RoutineExpression expression) {
            Routine routine = expression.getRoutine();
            List<ExpressionNode> operands = expression.getOperands();
            for (int i = 0; i < operands.size(); i++) {
                ExpressionNode operand = castTo(operands.get(i), routine.getParameters().get(i).getType(),
                                                folder, parametersSync);
                operands.set(i, operand);
            }
            TPreptimeValue tpv = new TPreptimeValue(routine.getReturnValue().getType());
            expression.setPreptimeValue(tpv);
            return expression;
        }

        ExpressionNode handleColumnDefaultExpression(ColumnDefaultExpression expression) {
            if (expression.getPreptimeValue() == null) {
                TPreptimeValue tpv = new TPreptimeValue(expression.getColumn().getType());
                expression.setPreptimeValue(tpv);
            }
            return expression;
        }

        private static ValueSource pval(ExpressionNode expression) {
            return expression.getPreptimeValue().value();
        }

        private void updateSetNode(SetPlanNode setPlan) {
            Project leftProject = getProject(setPlan.getLeft());
            Project rightProject= getProject(setPlan.getRight());
            Project topProject = (Project)setPlan.getOutput();
            ResultSet leftResult = (ResultSet)leftProject.getOutput();
            ResultSet rightResult = (ResultSet)rightProject.getOutput();
            List<ResultField> fields = new ArrayList<> (leftProject.nFields());

            for (int i= 0; i < leftProject.nFields(); i++) {
                ExpressionNode leftExpr = leftProject.getFields().get(i);
                ExpressionNode rightExpr= rightProject.getFields().get(i);
                DataTypeDescriptor leftType = leftExpr.getSQLtype();
                DataTypeDescriptor rightType = rightExpr.getSQLtype();

                DataTypeDescriptor projectType = null;
                // Case of SELECT null UNION SELECT null -> pick a type
                if (leftType == null && rightType == null)
                    projectType = new DataTypeDescriptor (TypeId.VARCHAR_ID, true);
                if (leftType == null)
                    projectType = rightType;
                else if (rightType == null)
                    projectType = leftType;
                else {
                    try {
                        projectType = leftType.getDominantType(rightType);
                    } catch (StandardException e) {
                        projectType = null;
                    }
                }
                TInstance projectInst = typesTranslator.typeForSQLType(projectType);
                ValueNode leftSource = leftExpr.getSQLsource();
                ValueNode rightSource = rightExpr.getSQLsource();

                CastExpression leftCast = new CastExpression(leftExpr, projectType, leftSource, projectInst);
                castProjectField(leftCast, folder, parametersSync, typesTranslator);
                leftProject.getFields().set(i, leftCast);

                CastExpression rightCast = new CastExpression (rightExpr, projectType, rightSource, projectInst);
                castProjectField(rightCast, folder, parametersSync, typesTranslator);
                rightProject.getFields().set(i, rightCast);

                ResultField leftField = leftResult.getFields().get(i);
                ResultField rightField = rightResult.getFields().get(i);
                String name = null;
                if (leftField.getName() != null && rightField.getName() != null)
                    name = leftField.getName();
                else if (leftField.getName() != null)
                    name = leftField.getName();
                else if (rightField.getName() != null)
                    name = rightField.getName();

                Column column = null;
                // If both side of the setPlan reference the same column, use it, else null
                if (leftField.getColumn() != null && rightField.getColumn() != null &&
                        leftField.getColumn() == rightField.getColumn())
                    column = leftField.getColumn();

                fields.add(new ResultField(name, projectType, column));
                fields.get(i).setType(typesTranslator.typeForSQLType(projectType));
            }

            setPlan.setResults(fields);

            // setPlan -> project -> ResultSet
            if (setPlan.getOutput().getOutput() instanceof ResultSet) {
                ResultSet rs = (ResultSet)setPlan.getOutput().getOutput();
                ResultSet newSet = new ResultSet (setPlan, fields);
                rs.getOutput().replaceInput(rs, newSet);
            }
        }


        private void castProjectField (CastExpression cast, Folder folder, ParametersSync parameterSync, TypesTranslator typesTranslator) {
            DataTypeDescriptor dtd = cast.getSQLtype();
            TInstance type = typesTranslator.typeForSQLType(dtd);
            cast.setPreptimeValue(new TPreptimeValue(type));
            TypeResolver.finishCast(cast, folder, parameterSync);
        }

        private Project getProject(PlanNode node) {
            PlanNode project = ((BasePlanWithInput)node).getInput();
            if (project instanceof Project)
                return (Project)project;


            else if (project instanceof SetPlanNode) {
                SetPlanNode setOperator = (SetPlanNode)project;
                project = getProject(((SetPlanNode)project).getLeft());
                Project oldProject = (Project)project;
                Project setProject = (Project) project.duplicate();
                setProject.replaceInput(oldProject.getInput(), setOperator);
                return setProject;
            }
            else if (!(project instanceof BasePlanWithInput)) 
                return null;
            project = ((BasePlanWithInput)project).getInput();
            if (project instanceof Project)
                return (Project)project;
            return null;
        }
    }

    private static ValueSource castValue(TCast cast, TPreptimeValue source, TInstance targetInstance) {
        if (source == null)
            return null;
        boolean targetsMatch = targetInstance.typeClass() == cast.targetClass();
        boolean sourcesMatch = source.type().typeClass() == cast.sourceClass();
        if ( (!targetsMatch) || (!sourcesMatch) )
            throw new IllegalArgumentException("cast <" + cast + "> not applicable to CAST(" + source + " AS " + targetInstance);

        TExecutionContext context = new TExecutionContext(
                null,
                Collections.singletonList(source.type()),
                targetInstance,
                null, // TODO
                ErrorHandlingMode.ERROR,
                ErrorHandlingMode.ERROR,
                ErrorHandlingMode.ERROR
        );
        Value result = new Value(targetInstance);
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

    private static ExpressionNode boolExpr(ExpressionNode expression, boolean nullable) {
        TInstance type = AkBool.INSTANCE.instance(nullable);
        ValueSource value = null;
        if (expression.getPreptimeValue() != null) {
            if (type.equals(expression.getPreptimeValue().type()))
                return expression;
            value = expression.getPreptimeValue().value();
        }
        expression.setPreptimeValue(new TPreptimeValue(type, value));
        return expression;
    }

    static class TopLevelCaster {

        private List<? extends ColumnContainer> targetColumns;
        private Folder folder;
        private ParametersSync parametersSync;

        TopLevelCaster(Folder folder, ParametersSync parametersSync) {
            this.folder = folder;
            this.parametersSync = parametersSync;
        }

        public void apply(PlanNode node) {
            while (targetColumns == null) {
                if (node instanceof InsertStatement) {
                    InsertStatement insert = (InsertStatement) node;
                    setTargets(insert.getTargetColumns());
                }
                else if (node instanceof UpdateStatement) {
                    UpdateStatement update = (UpdateStatement) node;
                    setTargets(update.getUpdateColumns());
                    for (UpdateColumn updateColumn : update.getUpdateColumns()) {
                        Column target = updateColumn.getColumn();
                        ExpressionNode value = updateColumn.getExpression();
                        ExpressionNode casted = castTo(value, target.getType(), folder, parametersSync);
                        if (casted != value) {
                            updateColumn.setExpression(casted);
                        }
                    }
                }
                if (node instanceof BasePlanWithInput)
                    node = ((BasePlanWithInput)node).getInput();
                else
                    break;
            }
            if (targetColumns != null) {
                if (node instanceof Project)
                    handleProject((Project) node);
                else if (node instanceof ExpressionsSource)
                    handleExpressionSource((ExpressionsSource) node);
            }
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
                ExpressionNode casted = castTo(column, target.getType(), folder, parametersSync);
                row.set(i, casted);
                plan.setTypeAt(i, casted.getPreptimeValue());
            }
        }

        private void handleProject(Project source) {
            castToTarget(source.getFields(), source);
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
                    TInstance castTarget = type(cast);
                    TInstance parameterType = type(operand);
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

    private static ExpressionNode castTo(ExpressionNode expression, TInstance targetInstance, Folder folder,
                                  ParametersSync parametersSync)
    {
        // parameters and literal nulls have no type, so just set the type -- they'll be polymorphic about it.
        if (expression instanceof ParameterExpression) {
            targetInstance = targetInstance.withNullable(true);
            CastExpression castExpression = 
                newCastExpression(expression, targetInstance);
            castExpression.setPreptimeValue(new TPreptimeValue(targetInstance));
            parametersSync.set(expression, targetInstance);
            return castExpression;
        }
        if (expression instanceof NullSource) {
            ValueSource nullSource = ValueSources.getNullSource(targetInstance);
            expression.setPreptimeValue(new TPreptimeValue(targetInstance, nullSource));
            return expression;
        }

        if (equalForCast(targetInstance, type(expression)))
            return expression;
        DataTypeDescriptor sqlType = expression.getSQLtype();
        targetInstance = targetInstance.withNullable(sqlType == null || sqlType.isNullable());
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
            return (target.attribute(StringAttribute.MAX_LENGTH) ==
                    source.attribute(StringAttribute.MAX_LENGTH));
        }
        return target.equalsExcludingNullable(source);
    }

    private static CastExpression newCastExpression(ExpressionNode expression, TInstance targetInstance) {
        if (targetInstance.typeClass() == AkBool.INSTANCE)
            // Allow use as a condition.
            return new BooleanCastExpression(expression, targetInstance.dataTypeDescriptor(), expression.getSQLsource(), targetInstance);
        else
            return new CastExpression(expression, targetInstance.dataTypeDescriptor(), expression.getSQLsource(), targetInstance);
    }

    protected static ExpressionNode finishCast(CastExpression castNode, Folder folder, ParametersSync parametersSync) {
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
                TInstance castType = castNode.getType();
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
            if(source instanceof CreateAs) {
                inner.setPreptimeValue(castNode.getPreptimeValue());
            }
        }
        return result;
    }

    private static TClass tclass(ExpressionNode operand) {
        return tclass(type(operand));
    }

    private static TClass tclass(TInstance type) {
        return (type == null) ? null : type.typeClass();
    }

    private static TInstance type(ExpressionNode node) {
        TPreptimeValue ptv = node.getPreptimeValue();
        return ptv == null ? null : ptv.type();
    }

    private static TInstance commonInstance(TCastResolver resolver, ExpressionNode left, ExpressionNode right) {
        return commonInstance(resolver, type(left), type(right));
    }

    public static TInstance commonInstance(TCastResolver resolver, TInstance left, TInstance right) {
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
        return commonClass.instance(left.nullability() || right.nullability());
    }

    private static RuntimeException error(String message) {
        throw new RuntimeException(message); // TODO what actual error type?
    }

    /**
     * Helper class for keeping various instances of the same parameter in sync, in terms of their TInstance. So for
     * instance, in an expression IF($0 == $1, $1, $0) we'd want both $0s to have the same TInstance, and ditto for
     * both $1s.
     */
    protected static class ParametersSync {
        private TCastResolver resolver;
        private SparseArray<List<ExpressionNode>> instancesMap;

        public ParametersSync(TCastResolver resolver) {
            this.resolver = resolver;
            this.instancesMap = new SparseArray<>();
        }

        public void uninferred(ParameterExpression parameterExpression) {
            //assert parameterExpression.getPreptimeValue() == null : parameterExpression;
            TPreptimeValue preptimeValue;
            List<ExpressionNode> siblings = siblings(parameterExpression);
            if (siblings.isEmpty()) {
                preptimeValue = new TPreptimeValue();
                if (parameterExpression.getSQLsource() != null)
                    // Start with type client intends to send, if any.
                    preptimeValue.type((TInstance) parameterExpression.getSQLsource().getUserData());
                parameterExpression.setPreptimeValue(new TPreptimeValue());
            }
            else {
                preptimeValue = siblings.get(0).getPreptimeValue();
            }
            parameterExpression.setPreptimeValue(preptimeValue);
            siblings.add(parameterExpression);
        }

        private List<ExpressionNode> siblings(ParameterExpression parameterNode) {
            int pos = parameterNode.getPosition();
            List<ExpressionNode> siblings = instancesMap.get(pos);
            if (siblings == null) {
                siblings = new ArrayList<>(4); // guess at capacity. this should be plenty
                instancesMap.set(pos, siblings);
            }
            return siblings;
        }

        public void set(ExpressionNode node, TInstance type) {
            List<ExpressionNode> siblings = siblings((ParameterExpression) node);
            TPreptimeValue sharedTpv = siblings.get(0).getPreptimeValue();
            TInstance previousInstance = sharedTpv.type();
            type = commonInstance(resolver, type, previousInstance);
            sharedTpv.type(type);
        }

        public void updateTypes(TypesTranslator typesTranslator) {
            int nparams = instancesMap.lastDefinedIndex();
            for (int i = 0; i < nparams; i++) {
                if (!instancesMap.isDefined(i)) continue;
                List<ExpressionNode> siblings = instancesMap.get(i);
                TPreptimeValue sharedTpv = siblings.get(0).getPreptimeValue();
                TInstance type = sharedTpv.type();
                DataTypeDescriptor sqlType = null;
                if (type == null) {
                    sqlType = siblings.get(0).getSQLtype();
                    if (sqlType != null)
                        type = typesTranslator.typeForSQLType(sqlType);
                    else
                        type = typesTranslator.typeClassForString().instance(true);
                    sharedTpv.type(type);
                }
                if (sqlType == null)
                    sqlType = type.dataTypeDescriptor();
                for (ExpressionNode param : siblings) {
                    param.setSQLtype(sqlType);
                    if (param.getSQLsource() != null) {
                        try {
                            param.getSQLsource().setType(sqlType);
                            param.getSQLsource().setUserData(type);
                        }
                        catch (StandardException ex) {
                            throw new SQLParserInternalException(ex);
                        }
                    }
                }
            }
        }
    }
}
