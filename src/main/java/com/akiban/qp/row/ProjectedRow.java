
package com.akiban.qp.row;

import com.akiban.qp.operator.QueryContext;
import com.akiban.qp.rowtype.ProjectedRowType;
import com.akiban.qp.rowtype.RowType;
import com.akiban.server.Quote;
import com.akiban.server.expression.Expression;
import com.akiban.server.expression.ExpressionEvaluation;
import com.akiban.server.types.ValueSource;
import com.akiban.server.types.util.ValueHolder;
import com.akiban.server.types3.TInstance;
import com.akiban.server.types3.pvalue.PValueSource;
import com.akiban.server.types3.texpressions.TEvaluatableExpression;
import com.akiban.server.types3.texpressions.TPreparedExpression;
import com.akiban.util.AkibanAppender;

import com.google.common.base.Function;
import com.google.common.collect.Iterators;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class ProjectedRow extends AbstractRow
{
    // Object interface

    @Override
    public String toString()
    {
        StringBuilder buffer = new StringBuilder();
        AkibanAppender appender = AkibanAppender.of(buffer);
        buffer.append('(');
        boolean first = true;
        if (pEvaluatableExpressions != null) {
            for (int i = 0, pEvalsSize = pEvaluatableExpressions.size(); i < pEvalsSize; i++) {
                PValueSource evaluation = pvalue(i);
                TInstance instance = tInstances.get(i);
                if (first) {
                    first = false;
                } else {
                    buffer.append(", ");
                }
                instance.format(evaluation, appender);
            }
        }
        else {
            for (ExpressionEvaluation evaluation : evaluations) {
                if (first) {
                    first = false;
                } else {
                    buffer.append(", ");
                }
                evaluation.eval().appendAsString(appender, Quote.NONE);
            }
        }
        buffer.append(')');
        return buffer.toString();
    }

    // Row interface

    @Override
    public RowType rowType()
    {
        return rowType;
    }

    @Override
    public ValueSource eval(int i) {
        ValueHolder holder = holders[i];
        if (holder == null) {
            holders[i] = holder = new ValueHolder();
            holder.copyFrom(evaluations.get(i).eval());
        }
        return holder;
    }

    @Override
    public PValueSource pvalue(int index) {
        TEvaluatableExpression evaluatableExpression = pEvaluatableExpressions.get(index);
        if (!evaluated[index]) {
            evaluatableExpression.with(row);
            evaluatableExpression.with(context);
            evaluatableExpression.evaluate();
            evaluated[index] = true;
        }
        return evaluatableExpression.resultValue();
    }

    @Override
    public HKey hKey()
    {
        return null;
    }

    // AbstractRow interface


    @Override
    protected void beforeAcquire() {
        row.acquire();
    }

    @Override
    public void afterRelease()
    {
        row.release();
    }

    // ProjectedRow interface

    public ProjectedRow(ProjectedRowType rowType,
                        Row row,
                        QueryContext context,
                        List<? extends Expression> expressions,
                        List<TEvaluatableExpression> pEvaluatableExprs,
                        List<? extends TInstance> tInstances)
    {
        this.context = context;
        this.rowType = rowType;
        this.row = row;
        this.evaluations = createEvaluations(expressions, row, context);
        this.pEvaluatableExpressions = pEvaluatableExprs;
        if (pEvaluatableExpressions == null)
            evaluated = null;
        else
            evaluated = new boolean[pEvaluatableExpressions.size()];
        this.tInstances = tInstances;
        this.holders = expressions == null ? null : new ValueHolder[expressions.size()];
    }

    public Iterator<ValueSource> getValueSources()
    {
        if (evaluations == null)
            return null;
        else
        {
            int size = evaluations.size();
            List<ValueSource> ret = new ArrayList<>(size);
            for (int i = 0; i < size; ++i)
                ret.add(eval(i));
            return ret.iterator();
        }
    }
    
    public Iterator<PValueSource> getPValueSources()
    {
        if (pEvaluatableExpressions == null)
            return null;
        else
        {
            int size = pEvaluatableExpressions.size();
            List<PValueSource> ret = new ArrayList<>(size);
            for (int i = 0; i < size; ++i)
                ret.add(pvalue(i));
            return ret.iterator();
        }
    }
    // For use by this class

    private List<ExpressionEvaluation> createEvaluations(List<? extends Expression> expressions,
                                                         Row row, QueryContext context)
    {
        if (expressions == null)
            return null;
        int n = expressions.size();
        List<ExpressionEvaluation> result = new ArrayList<>(n);
        for (int i = 0; i < n; i++) {
            ExpressionEvaluation evaluation = expressions.get(i).evaluation();
            evaluation.of(context);
            evaluation.of(row);
            result.add(evaluation);
        }
        return result;
    }

    public static List<TEvaluatableExpression> createTEvaluatableExpressions
        (List<? extends TPreparedExpression> pExpressions)
    {
        if (pExpressions == null)
            return null;
        int n = pExpressions.size();
        List<TEvaluatableExpression> result = new ArrayList<>(n);
        for (int i = 0; i < n; i++) {
            TEvaluatableExpression eval = pExpressions.get(i).build();
            result.add(eval);
        }
        return result;
    }


    // Object state

    private final QueryContext context;
    private final ProjectedRowType rowType;
    private final Row row;
    private final List<ExpressionEvaluation> evaluations;
    private final List<TEvaluatableExpression> pEvaluatableExpressions;
    private final boolean[] evaluated;
    private final List<? extends TInstance> tInstances;
    private final ValueHolder[] holders;
}
