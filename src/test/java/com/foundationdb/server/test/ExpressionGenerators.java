/**
 * Copyright (C) 2009-2013 Akiban Technologies, Inc.
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
import com.foundationdb.qp.row.RowBase;
import com.foundationdb.qp.rowtype.IndexRowType;
import com.foundationdb.qp.rowtype.RowType;
import com.foundationdb.server.api.dml.ColumnSelector;
import com.foundationdb.server.explain.CompoundExplainer;
import com.foundationdb.server.explain.ExplainContext;
import com.foundationdb.server.expression.Expression;
import com.foundationdb.server.expression.std.BoundFieldExpression;
import com.foundationdb.server.expression.std.CaseConvertExpression;
import com.foundationdb.server.expression.std.ColumnExpression;
import com.foundationdb.server.expression.std.CompareExpression;
import com.foundationdb.server.expression.std.Comparison;
import com.foundationdb.server.expression.std.FieldExpression;
import com.foundationdb.server.expression.std.LiteralExpression;
import com.foundationdb.server.expression.std.VariableExpression;
import com.foundationdb.server.expression.subquery.AnySubqueryExpression;
import com.foundationdb.server.expression.subquery.ExistsSubqueryExpression;
import com.foundationdb.server.expression.subquery.ScalarSubqueryExpression;
import com.foundationdb.server.t3expressions.TCastResolver;
import com.foundationdb.server.types.AkType;
import com.foundationdb.server.types.FromObjectValueSource;
import com.foundationdb.server.types3.TCast;
import com.foundationdb.server.types3.TClass;
import com.foundationdb.server.types3.TInstance;
import com.foundationdb.server.types3.TPreptimeValue;
import com.foundationdb.server.types3.mcompat.mtypes.MString;
import com.foundationdb.server.types3.pvalue.PValue;
import com.foundationdb.server.types3.pvalue.PValueSource;
import com.foundationdb.server.types3.pvalue.PValueSources;
import com.foundationdb.server.types3.texpressions.AnySubqueryTExpression;
import com.foundationdb.server.types3.texpressions.ExistsSubqueryTExpression;
import com.foundationdb.server.types3.texpressions.ScalarSubqueryTExpression;
import com.foundationdb.server.types3.texpressions.TCastExpression;
import com.foundationdb.server.types3.texpressions.TComparisonExpression;
import com.foundationdb.server.types3.texpressions.TEvaluatableExpression;
import com.foundationdb.server.types3.texpressions.TPreparedBoundField;
import com.foundationdb.server.types3.texpressions.TPreparedExpression;
import com.foundationdb.server.types3.texpressions.TPreparedField;
import com.foundationdb.server.types3.texpressions.TPreparedLiteral;
import com.foundationdb.server.types3.texpressions.TPreparedParameter;
import com.foundationdb.sql.optimizer.rule.OverloadAndTInstanceResolver;

public final class ExpressionGenerators {
    public static ExpressionGenerator field(final Column column, final int position)
    {
        return new ExpressionGenerator() {
            @Override
            public Expression getExpression() {
                return new ColumnExpression(column, position);
            }

            @Override
            public TPreparedExpression getTPreparedExpression() {
                return new TPreparedField(column.tInstance(), position);
            }
        };
    }

    public static ExpressionGenerator compare(final ExpressionGenerator left, final Comparison comparison,
                                              final ExpressionGenerator right, final TCastResolver castResolver)
    {
        return new ExpressionGenerator() {
            @Override
            public Expression getExpression() {
                return new CompareExpression(left.getExpression(), comparison, right.getExpression());
            }

            @Override
            public TPreparedExpression getTPreparedExpression() {
                TPreparedExpression leftExpr = left.getTPreparedExpression();
                TPreparedExpression rightExpr = right.getTPreparedExpression();

                TInstance common = OverloadAndTInstanceResolver.commonInstance(
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
                return new TCastExpression(expression, cast, target, null);
            }
        };
    }

    public static ExpressionGenerator field(final RowType rowType, final int position)
    {
        return new ExpressionGenerator() {
            @Override
            public Expression getExpression() {
                return new FieldExpression(rowType, position);
            }

            @Override
            public TPreparedExpression getTPreparedExpression() {
                return new TPreparedField(rowType.typeInstanceAt(position), position);
            }
        };
    }

    public static ExpressionGenerator existsSubquery(final Operator innerPlan, final RowType outer, final RowType inner, final int bindingPos)
    {
        return new ExpressionGenerator() {
            @Override
            public Expression getExpression() {
                return new ExistsSubqueryExpression(innerPlan, outer, inner, 1);
            }

            @Override
            public TPreparedExpression getTPreparedExpression() {
                return new ExistsSubqueryTExpression(innerPlan, outer, inner, bindingPos);
            }
        };
    }

    public static ExpressionGenerator scalarSubquery(final Operator innerPlan, final ExpressionGenerator expression, final RowType outer, final RowType inner, final int pos) {
        return new ExpressionGenerator() {
            @Override
            public Expression getExpression() {
                return new ScalarSubqueryExpression(innerPlan, expression.getExpression(), outer, inner, 1);
            }

            @Override
            public TPreparedExpression getTPreparedExpression() {
                return new ScalarSubqueryTExpression(innerPlan, expression.getTPreparedExpression(), outer, inner, pos);
            }
        };
    }

    public static ExpressionGenerator anySubquery(final Operator innerPlan, final ExpressionGenerator expression, final RowType outer, final RowType inner, final int pos) {
        return new ExpressionGenerator() {
            @Override
            public Expression getExpression() {
                return new AnySubqueryExpression(innerPlan, expression.getExpression(), outer, inner, 1);
            }

            @Override
            public TPreparedExpression getTPreparedExpression() {
                return new AnySubqueryTExpression(innerPlan, expression.getTPreparedExpression(), outer, inner, pos);
            }
        };
    }

    public static IndexBound indexBound(RowBase row, ColumnSelector columnSelector)
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
            public Expression getExpression() {
                return new LiteralExpression(new FromObjectValueSource().setReflectively(value));
            }

            @Override
            public TPreparedExpression getTPreparedExpression() {
                FromObjectValueSource valueSource = new FromObjectValueSource().setReflectively(value);
                TPreptimeValue tpv = PValueSources.fromObject(value, valueSource.getConversionType());
                return new TPreparedLiteral(tpv.instance(), tpv.value());
            }
        };
    }

    public static ExpressionGenerator literal(final Object value, final AkType type)
    {
        return new ExpressionGenerator() {
            @Override
            public Expression getExpression() {
                return new LiteralExpression(new FromObjectValueSource().setExplicitly(value, type));
            }

            @Override
            public TPreparedExpression getTPreparedExpression() {
                TPreptimeValue tpv = PValueSources.fromObject(value, type);
                return new TPreparedLiteral(tpv.instance(), tpv.value());
            }
        };
    }

    public static ExpressionGenerator variable(final AkType type, final int position)
    {
        return new ExpressionGenerator() {
            @Override
            public Expression getExpression() {
                return new VariableExpression(type, position);
            }

            @Override
            public TPreparedExpression getTPreparedExpression() {
                assert false : "TODO";
                return new TPreparedParameter(position, null); // TODO
            }
        };
    }

    public static ExpressionGenerator boundField(final RowType rowType, final int rowPosition, final int fieldPosition)
    {
        return new ExpressionGenerator() {
            @Override
            public Expression getExpression() {
                return new BoundFieldExpression(rowPosition, new FieldExpression(rowType, fieldPosition));
            }

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
            public Expression getExpression() {
                return new CaseConvertExpression(input.getExpression(), CaseConvertExpression.ConversionType.TOUPPER);
            }

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
                            public PValueSource resultValue() {
                                return pValue;
                            }

                            @Override
                            public void evaluate() {
                                eval.evaluate();
                                PValueSource inSrc = eval.resultValue();
                                if (inSrc.isNull())
                                    pValue.putNull();
                                else
                                    pValue.putString(inSrc.getString(), null);
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

                            private final PValue pValue = new PValue(MString.VARCHAR.instance(255, true));
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
