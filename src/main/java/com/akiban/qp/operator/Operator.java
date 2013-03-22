
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
