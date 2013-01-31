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

package com.akiban.qp.operator;

import com.akiban.ais.model.UserTable;
import com.akiban.qp.exec.Plannable;
import com.akiban.qp.rowtype.RowType;
import com.akiban.util.tap.InOutTap;
import com.akiban.util.tap.Tap;

import java.util.Collections;
import java.util.List;
import java.util.Set;

public abstract class Operator implements Plannable
{
    // Object interface

    @Override
    public String toString()
    {
        return getName();
    }

    // Operator interface

    @Override
    public String getName()
    {
        return getClass().getSimpleName();
    }

    // I'm not sure I like having this as part of the interface. On one hand, operators like Flatten create new
    // RowTypes and it's handy to get access to those new RowTypes. On the other hand, not all operators do this,
    // and it's conceivable we'll have to invent an operator for which this doesn't make sense, e.g., it creates
    // multiple RowTypes.
    public RowType rowType()
    {
        throw new UnsupportedOperationException(getClass().getName());
    }

    /**
     * Find the derived types created by this operator and its inputs. A <i>derived type</i> is a type generated
     * by an operator, and as such, does not correspond to an AIS UserTable or Index.
     * @param derivedTypes Derived types created by this operator or input operators are added to derivedTypes.
     */
    public void findDerivedTypes(Set<RowType> derivedTypes)
    {
    }

    @Override
    public List<Operator> getInputOperators()
    {
        return Collections.emptyList();
    }

    protected abstract Cursor cursor(QueryContext context);

    @Override
    public String describePlan()
    {
        return toString();
    }

    @Override
    public final String describePlan(Operator inputOperator)
    {
        StringBuilder buffer = new StringBuilder();
        buffer.append(inputOperator.describePlan());
        buffer.append(NL);
        buffer.append(toString());
        return buffer.toString();
    }

    // For use by subclasses

    protected int ordinal(UserTable table)
    {
        return (table.rowDef()).getOrdinal();
    }

    // Class state

    protected static final String NL = System.getProperty("line.separator");
    public static final InOutTap OPERATOR_TAP = Tap.createRecursiveTimer("operator: root");
}
