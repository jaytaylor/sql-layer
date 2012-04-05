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

package com.akiban.ais.model.staticgrouping;

import com.akiban.ais.model.TableName;

public final class JoinDescriptionBuilder
{
    interface Callback {
        JoinDescription created(JoinDescriptionBuilder builder,
                                       TableName parent, String firstParentColumn,
                                       TableName child, String firstChildColumn);
    }
    private final TableName parent;
    private final TableName joinChild;
    private final Callback callback;
    private JoinDescription join;

    JoinDescriptionBuilder(TableName parent, TableName child, Callback callback)
    {
        this.parent = parent;
        this.joinChild = child;
        this.callback = callback;
    }

    public JoinDescriptionBuilder column(String parent, String child) {
        if (join == null) {
            join = callback.created(this, this.parent, parent, this.joinChild, child);
        }
        else {
            join.addJoinColumn(parent, child);
        }
        return this;
    }

    @Override
    public String toString() {
        StringBuilder builder = new StringBuilder(JoinDescriptionBuilder.class.getName()).append('[');
        if (join == null) {
            builder.append("unbuilt join ").append(joinChild.escaped());
        }
        else {
            builder.append("valid join ").append(join);
        }
        builder.append(" has parent ").append(parent).append(']');
        return builder.toString();
    }
}
