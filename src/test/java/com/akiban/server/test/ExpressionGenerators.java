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

package com.akiban.server.test;

import com.akiban.ais.model.Column;
import com.akiban.qp.expression.IndexBound;
import com.akiban.qp.expression.IndexKeyRange;
import com.akiban.qp.operator.ExpressionGenerator;
import com.akiban.qp.operator.Operator;
import com.akiban.qp.operator.QueryContext;
import com.akiban.qp.row.Row;
import com.akiban.qp.row.RowBase;
import com.akiban.qp.rowtype.IndexRowType;
import com.akiban.qp.rowtype.RowType;
import com.akiban.server.api.dml.ColumnSelector;
import com.akiban.server.explain.CompoundExplainer;
import com.akiban.server.explain.ExplainContext;
import com.akiban.server.expression.Expression;
import com.akiban.server.expression.std.BoundFieldExpression;
import com.akiban.server.expression.std.CaseConvertExpression;
import com.akiban.server.expression.std.ColumnExpression;
import com.akiban.server.expression.std.CompareExpression;
import com.akiban.server.expression.std.Comparison;
import com.akiban.server.expression.std.FieldExpression;
import com.akiban.server.expression.std.LiteralExpression;
import com.akiban.server.expression.std.VariableExpression;
import com.akiban.server.expression.subquery.AnySubqueryExpression;
import com.akiban.server.expression.subquery.ExistsSubqueryExpression;
import com.akiban.server.expression.subquery.ScalarSubqueryExpression;
import com.akiban.server.t3expressions.TCastResolver;
import com.akiban.server.types.AkType;
import com.akiban.server.types.FromObjectValueSource;
import com.akiban.server.types3.TCast;
import com.akiban.server.types3.TClass;
import com.akiban.server.types3.TInstance;
import com.akiban.server.types3.TPreptimeValue;
import com.akiban.server.types3.pvalue.PUnderlying;
import com.akiban.server.types3.pvalue.PValue;
import com.akiban.server.types3.pvalue.PValueSource;
import com.akiban.server.types3.pvalue.PValueSources;
import com.akiban.server.types3.texpressions.AnySubqueryTExpression;
import com.akiban.server.types3.texpressions.ExistsSubqueryTExpression;
import com.akiban.server.types3.texpressions.ScalarSubqueryTExpression;
import com.akiban.server.types3.texpressions.TCastExpression;
import com.akiban.server.types3.texpressions.TComparisonExpression;
import com.akiban.server.types3.texpressions.TEvaluatableExpression;
import com.akiban.server.types3.texpressions.TPreparedBoundField;
import com.akiban.server.types3.texpressions.TPreparedExpression;
import com.akiban.server.types3.texpressions.TPreparedField;
import com.akiban.server.types3.texpressions.TPreparedLiteral;
import com.akiban.server.types3.texpressions.TPreparedParameter;
import com.akiban.sql.optimizer.rule.OverloadAndTInstanceResolver;

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

                            private final PValue pValue = new PValue(PUnderlying.STRING);
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
