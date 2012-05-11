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

package com.akiban.sql.optimizer.plan;

import java.util.ArrayList;
import java.util.Collection;

/** A conjunction of boolean conditions used for WHERE / HAVING / ON / ...
 */
public class ConditionList extends ArrayList<ConditionExpression>
{
    public ConditionList() {
        super();
    }

    public ConditionList(int size) {
        super(size);
    }

    public ConditionList(Collection<? extends ConditionExpression> list) {
        super(list);
    }

    public boolean accept(ExpressionVisitor v) {
        for (ConditionExpression condition : this) {
            if (!condition.accept(v)) {
                return false;
            }
        }
        return true;
    }

    public void accept(ExpressionRewriteVisitor v) {
        for (int i = 0; i < size(); i++) {
            set(i, (ConditionExpression)get(i).accept(v));
        }
    }

    public ConditionList duplicate(DuplicateMap map) {
        ConditionList copy = new ConditionList(size());
        for (ConditionExpression cond : this) {
            copy.add((ConditionExpression)cond.duplicate(map));
        }
        return copy;
    }

}
