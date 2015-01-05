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

package com.foundationdb.server.test;

import com.foundationdb.ais.model.Column;
import com.foundationdb.qp.expression.IndexBound;
import com.foundationdb.qp.expression.IndexKeyRange;
import com.foundationdb.qp.operator.ExpressionGenerator;
import com.foundationdb.qp.operator.Operator;
import com.foundationdb.qp.operator.QueryBindings;
import com.foundationdb.qp.operator.QueryContext;
import com.foundationdb.qp.row.Row;
import com.foundationdb.qp.rowtype.IndexRowType;
import com.foundationdb.qp.rowtype.RowType;
import com.foundationdb.server.api.dml.ColumnSelector;
import com.foundationdb.server.explain.CompoundExplainer;
import com.foundationdb.server.explain.ExplainContext;
import com.foundationdb.server.types.service.TCastResolver;
import com.foundationdb.server.types.TCast;
import com.foundationdb.server.types.TClass;
import com.foundationdb.server.types.TInstance;
import com.foundationdb.server.types.TPreptimeValue;
import com.foundationdb.server.types.mcompat.mtypes.MString;
import com.foundationdb.server.types.value.Value;
import com.foundationdb.server.types.value.ValueSource;
import com.foundationdb.server.types.value.ValueSources;
import com.foundationdb.server.types.texpressions.Comparison;
import com.foundationdb.server.types.texpressions.AnySubqueryTExpression;
import com.foundationdb.server.types.texpressions.ExistsSubqueryTExpression;
import com.foundationdb.server.types.texpressions.ScalarSubqueryTExpression;
import com.foundationdb.server.types.texpressions.TCastExpression;
import com.foundationdb.server.types.texpressions.TComparisonExpression;
import com.foundationdb.server.types.texpressions.TEvaluatableExpression;
import com.foundationdb.server.types.texpressions.TPreparedBoundField;
import com.foundationdb.server.types.texpressions.TPreparedExpression;
import com.foundationdb.server.types.texpressions.TPreparedField;
import com.foundationdb.server.types.texpressions.TPreparedLiteral;
import com.foundationdb.server.types.texpressions.TPreparedParameter;
import com.foundationdb.sql.optimizer.rule.TypeResolver;

public final class ExpressionGenerators {
    public static ExpressionGenerator field(final Column column, final int position)
    {
        return new ExpressionGenerator() {
            @Override
            public TPreparedExpression getTPreparedExpression() {
                return new TPreparedField(column.getType(), position);
            }
        };
    }

    public static ExpressionGenerator compare(final ExpressionGenerator left, final Comparison comparison,
                                              final ExpressionGenerator right, final TCastResolver castResolver)
    {
        return new ExpressionGenerator() {
            @Override
            public TPreparedExpression getTPreparedExpression() {
                TPreparedExpression leftExpr = left.getTPreparedExpression();
                TPreparedExpression rightExpr = right.getTPreparedExpression();

                TInstance common = TypeResolver.commonInstance(
                        castResolver, leftExpr.resultType(), rightExpr.resultType());
                leftExpr = castTo(leftExpr, common, castResolver);
                rightExpr = castTo(rightExpr, common, castResolver);
                return new TComparisonExpression(leftExpr, comparison, rightExpr);
            }

            private TPreparedExpression castTo(TPreparedExpression expression, TInstance target, TCastResolver casts) {
                TClass inputTClass = expression.resultType().typeClass();
                TClass targetTClass = target.typeClass();
                if (targetTClass.equals(inputTClass))
                    return expression;
                TCast cast = casts.cast(inputTClass, targetTClass);
                return new TCastExpression(expression, cast, target);
            }
        };
    }

    public static ExpressionGenerator field(final RowType rowType, final int position)
    {
        return field(rowType, position, position);
    }

    public static ExpressionGenerator field(final RowType rowType, final int rowTypeIndex, final int fieldIndex)
    {
        return field(rowType.typeAt(rowTypeIndex), fieldIndex);
    }

    public static ExpressionGenerator field(final TInstance type, final int position)
    {
        return new ExpressionGenerator() {
            @Override
            public TPreparedExpression getTPreparedExpression() {
                return new TPreparedField(type, position);
            }
        };
    }

    public static ExpressionGenerator existsSubquery(final Operator innerPlan, final RowType outer, final RowType inner, final int bindingPos)
    {
        return new ExpressionGenerator() {
            @Override
            public TPreparedExpression getTPreparedExpression() {
                return new ExistsSubqueryTExpression(innerPlan, outer, inner, bindingPos);
            }
        };
    }

    public static ExpressionGenerator scalarSubquery(final Operator innerPlan, final ExpressionGenerator expression, final RowType outer, final RowType inner, final int pos) {
        return new ExpressionGenerator() {
            @Override
            public TPreparedExpression getTPreparedExpression() {
                return new ScalarSubqueryTExpression(innerPlan, expression.getTPreparedExpression(), outer, inner, pos);
            }
        };
    }

    public static ExpressionGenerator anySubquery(final Operator innerPlan, final ExpressionGenerator expression, final RowType outer, final RowType inner, final int pos) {
        return new ExpressionGenerator() {
            @Override
            public TPreparedExpression getTPreparedExpression() {
                return new AnySubqueryTExpression(innerPlan, expression.getTPreparedExpression(), outer, inner, pos);
            }
        };
    }

    public static IndexBound indexBound(Row row, ColumnSelector columnSelector)
    {
        return new IndexBound(row, columnSelector);
    }

    public static IndexKeyRange indexKeyRange(IndexRowType indexRowType, IndexBound lo, boolean loInclusive, IndexBound hi, boolean hiInclusive)
    {
        return IndexKeyRange.bounded(indexRowType, lo, loInclusive, hi, hiInclusive);
    }

    public static ExpressionGenerator literal(final Object value)
    {
        return new ExpressionGenerator() {
            @Override
            public TPreparedExpression getTPreparedExpression() {
                TPreptimeValue tpv = ValueSources.preptimeValueFromObject(value);
                
                //FromObjectValueSource valueSource = new FromObjectValueSource().setReflectively(value);
                //TPreptimeValue tpv = ValueSources.fromObject(value, valueSource.getConversionType());
                return new TPreparedLiteral(tpv.type(), tpv.value());
            }
        };
    }

    public static ExpressionGenerator literal (final Object value, final TInstance type) 
    {
        return new ExpressionGenerator() {
            @Override
            public TPreparedExpression getTPreparedExpression() {
                TPreptimeValue tpv = ValueSources.fromObject(value, type);
                return new TPreparedLiteral(tpv.type(), tpv.value());
            }
        };
    }

    public static ExpressionGenerator variable(final TInstance type, final int position)
    {
        return new ExpressionGenerator() {
            @Override
            public TPreparedExpression getTPreparedExpression() {
                return new TPreparedParameter(position, type); 
            }
        };
    }

    public static ExpressionGenerator boundField(final RowType rowType, final int rowPosition, final int fieldPosition)
    {
        return new ExpressionGenerator() {
            @Override
            public TPreparedExpression getTPreparedExpression() {
                return new TPreparedBoundField(rowType, rowPosition, fieldPosition);
            }
        };
    }

    public static ExpressionGenerator toUpper(final ExpressionGenerator input)
    {
        return new ExpressionGenerator() {
            @Override
            public TPreparedExpression getTPreparedExpression() {
                final TPreparedExpression expr = input.getTPreparedExpression();
                return new TPreparedExpression() {
                    
                    @Override
                    public TPreptimeValue evaluateConstant(QueryContext queryContext) {
                        throw new UnsupportedOperationException();
                    }

                    @Override
                    public TInstance resultType() {
                        return expr.resultType();
                    }

                    @Override
                    public TEvaluatableExpression build() {
                        final TEvaluatableExpression eval = expr.build();
                        return new TEvaluatableExpression() {
                            @Override
                            public ValueSource resultValue() {
                                return value;
                            }

                            @Override
                            public void evaluate() {
                                eval.evaluate();
                                ValueSource inSrc = eval.resultValue();
                                if (inSrc.isNull())
                                    value.putNull();
                                else
                                    value.putString(inSrc.getString(), null);
                            }

                            @Override
                            public void with(Row row) {
                                eval.with(row);
                            }

                            @Override
                            public void with(QueryContext context) {
                                eval.with(context);
                            }

                            @Override
                            public void with(QueryBindings bindings) {
                                eval.with(bindings);
                            }

                            private final Value value = new Value(MString.VARCHAR.instance(255, true));
                        };
                    }

                    @Override
                    public CompoundExplainer getExplainer(ExplainContext context) {
                        throw new UnsupportedOperationException();
                    }
                };
            }
        };
    }
}
