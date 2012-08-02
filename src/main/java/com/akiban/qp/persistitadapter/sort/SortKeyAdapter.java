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

package com.akiban.qp.persistitadapter.sort;

import com.akiban.ais.model.Column;
import com.akiban.qp.expression.BoundExpressions;
import com.akiban.qp.operator.API.Ordering;
import com.akiban.qp.operator.QueryContext;
import com.akiban.qp.row.Row;
import com.akiban.server.collation.AkCollator;
import com.akiban.server.expression.std.Comparison;
import com.akiban.server.types.AkType;
import com.akiban.server.types3.TInstance;

abstract class SortKeyAdapter<S, E> {
    public abstract AkType[] createAkTypes(int size);
    public abstract AkCollator[] createAkCollators(int size);
    public abstract TInstance[] createTInstances(int size);
    public abstract void setColumnMetadata(Column column, int f, AkType[] akTypes, AkCollator[] collators,
                                           TInstance[] tInstances);

    public abstract void checkConstraints(BoundExpressions loExpressions,
                                          BoundExpressions hiExpressions,
                                          int f,
                                          AkCollator[] collators);

    public abstract S[] createSourceArray(int size);

    public abstract S get(BoundExpressions boundExpressions, int f);
    public abstract SortKeyTarget<S> createTarget();

    public abstract SortKeySource<S> createSource(TInstance tInstance);
    public abstract long compare(TInstance tInstance, S one, S two);
    public abstract E createComparison(TInstance tInstance, AkCollator collator, S one, Comparison comparison, S two);
    public abstract boolean evaluateComparison(E comparison, QueryContext queryContext);
    public boolean areEqual(TInstance tInstance, AkCollator collator, S one, S two, QueryContext queryContext) {
        E expr = createComparison(tInstance, collator, one, Comparison.EQ, two);
        return evaluateComparison(expr, queryContext);
    }

    public void checkConstraints(BoundExpressions loExpressions, BoundExpressions hiExpressions, TInstance[] instances,
                                 int f)
    {
        S loValueSource = get(loExpressions, f);
        S hiValueSource = get(hiExpressions, f);
        if (isNull(loValueSource) && isNull(hiValueSource)) {
            // OK, they're equal
        } else if (isNull(loValueSource) || isNull(hiValueSource)) {
            throw new IllegalArgumentException(String.format("lo: %s, hi: %s", loValueSource, hiValueSource));
        } else {
            TInstance tInstance = (instances == null) ? null : instances[f];
            long comparison = compare(tInstance, loValueSource, hiValueSource);
            if (comparison != 0) {
                throw new IllegalArgumentException();
            }
        }
    }

    public abstract boolean isNull(S source);

    public abstract S eval(Row row, int field);

    public abstract void setOrderingMetadata(int orderingIndex, Ordering ordering, int tInstancesOffset,
                                             TInstance[] tInstances);
}
