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

package com.akiban.server.explain.format;

import com.akiban.server.explain.*;
import com.akiban.server.explain.Type.GeneralType;

import java.util.ArrayList;
import java.util.List;

public class DefaultFormatter
{
    private boolean verbose = true;
    private int numSubqueries = 0;
    private List<CompoundExplainer> subqueries = new ArrayList<CompoundExplainer>();
    private StringBuilder sb = new StringBuilder();
    private List<String> rows = new ArrayList<String>();
    
    public DefaultFormatter(boolean verbose)
    {
        this.verbose = verbose;
    }

    public List<String> describeToList(Explainer explainer)
    {
        describe(explainer);
        for (int i = 1; i <= numSubqueries; i++)
        {
            newRow();
            sb.append("SUBQUERY ").append(i).append(':');
            newRow();
            describeOperator(subqueries.get(i-1), 0);
        }
        newRow();
        return rows;
    }
    
    private void describe(Explainer explainer) {
        describe(explainer, sb, false, null);
    }

    protected void describe(Explainer explainer, StringBuilder sb, boolean needsParens, String parentName) {
        if (explainer.hasAttributes())
        {
            CompoundExplainer opEx = (CompoundExplainer) explainer;
            switch (explainer.getType().generalType())
            {
            case OPERATOR:
                describeOperator(opEx, 0);
                break;
            case ROWTYPE:
                describeRowType(opEx);
                break;
            default:
                describeExpression(opEx, needsParens, parentName);
            }
        }
        else
        {
            PrimitiveExplainer primEx = (PrimitiveExplainer) explainer;
            describePrimitive(primEx);
        }
    }

    protected void describeExpression(CompoundExplainer explainer, boolean needsParens, String parentName) {
        
        Attributes atts = explainer.get();
        String name = atts.get(Label.NAME).get(0).get().toString();
        
        if (atts.containsKey(Label.INFIX_REPRESENTATION))
        {
            Explainer leftExplainer = atts.valuePairs().get(0).getValue();
            Explainer rightExplainer = atts.valuePairs().get(1).getValue();
            if (name.equals(parentName) && atts.containsKey(Label.ASSOCIATIVE))
            {
                if(atts.get(Label.ASSOCIATIVE).get(0).equals(PrimitiveExplainer.getInstance(true)))
                    needsParens = false;
            }
            if (needsParens)
                sb.append("(");
            describe(leftExplainer, sb, true, name);
            sb.append(" ").append(atts.get(Label.INFIX_REPRESENTATION).get(0).get()).append(" ");
            describe(rightExplainer, sb, true, name);
            if (needsParens)
                sb.append(")");
        }
        else if (explainer.getType().equals(Type.LITERAL))
        {
            sb.append(atts.get(Label.OPERAND).get(0).get());
        }
        else if (explainer.getType().equals(Type.SUBQUERY))
        {
            sb.append("SUBQUERY ").append(++numSubqueries);
            subqueries.add((CompoundExplainer)atts.get(Label.OPERAND).get(0));
        }
        else if (name.startsWith("CAST"))
        {
            sb.append(name.substring(0, 4)).append("(");
            describe(atts.get(Label.OPERAND).get(0));
            sb.append(" AS ").append(atts.get(Label.OUTPUT_TYPE).get(0).get()).append(")");
        }
        else if (name.equals("Bound"))
        {
            sb.append(name).append("(").append(atts.get(Label.BINDING_POSITION).get(0).get()).append(",");
            describe(atts.get(Label.OPERAND).get(0));
            sb.append(")");
        }
        else if (name.equals("Variable"))
            sb.append(name).append("(pos=").append(atts.get(Label.BINDING_POSITION).get(0).get()).append(")");
        else if (name.equals("Field"))
        {
            if (atts.containsKey(Label.COLUMN_NAME))
                sb.append(atts.get(Label.COLUMN_NAME).get(0).get());
            else
                sb.append(name).append('(').append(atts.get(Label.BINDING_POSITION).get(0).get()).append(')');
        }
        else
        {
            sb.append(name).append('(');
            if (atts.containsKey(Label.OPERAND))
            {
                for (Explainer entry : atts.get(Label.OPERAND))
                {
                    describe(entry);
                    sb.append(", ");
                }
                sb.setLength(sb.length()-2);
            }
            sb.append(')');
        }
    }

    protected void describePrimitive(PrimitiveExplainer explainer) {

        if (explainer.getType()==Type.STRING)
        {
            sb.append('\'').append(explainer.get()).append('\'');
        }
        /*else if (explainer.getClass().isArray())
        {
        * do we need functionality to describePrimitive each element of the array separately?
        }*/
        else
        {
            sb.append(explainer.get());
        }
        
    }

    protected void describeOperator(CompoundExplainer explainer, int depth) {
        
        Attributes atts = explainer.get();
        Type type = explainer.getType();
        
        String name = atts.get(Label.NAME).get(0).get().toString();
        
        
        if (!verbose)
        {
            sb.append(name.substring(0, name.indexOf('_')));
            switch (type)
            {
                case LOOKUP_OPERATOR:
                    sb.append('(');
                    if (atts.containsKey(Label.TABLE_CORRELATION))
                    {
                        for (Explainer table : atts.get(Label.TABLE_CORRELATION))
                            sb.append(table.get()).append(", ");
                        sb.setLength(sb.length()-2);
                    }
                    else
                    {
                        for (Explainer table : atts.get(Label.ANCESTOR_TYPE))
                            sb.append(table.get()).append(", ");
                        if (!atts.get(Label.ANCESTOR_TYPE).isEmpty())
                            sb.setLength(sb.length() - 2);
                    }
                    sb.append(')');
                    break;
                case COUNT_OPERATOR:
                    sb.append("(*)");
                    break;
                case PHYSICAL_OPERATOR:
                    sb.append('(').append(atts.get(Label.BRIEF)).append(')');
                    break;
                case DUI:
                    if (name.equals("Delete_Default"))
                        if (atts.containsKey(Label.TABLE_CORRELATION))
                            sb.append("FROM ").append(atts.get(Label.TABLE_CORRELATION).get(0).get());
                        else if (atts.containsKey(Label.TABLE_TYPE))
                            sb.append("FROM ").append(atts.get(Label.TABLE_TYPE).get(0).get());
                    else if (name.equals("Insert_Default"))
                        if (atts.containsKey(Label.TABLE_CORRELATION))
                            sb.append("INTO ").append(atts.get(Label.TABLE_CORRELATION).get(0).get());
                        else if (atts.containsKey(Label.TABLE_TYPE))
                            sb.append("INTO ").append(atts.get(Label.TABLE_TYPE).get(0).get());
                    break;
                case SCAN_OPERATOR:
                    if (name.equals("IndexScan_Default"))
                        sb.append(atts.get(Label.INDEX).get(0).get());
                    break;
                default:
                    // Nothing needed, as most operators display nothing in brief mode
            }
        }
        else
        {
            sb.append(name).append("(");
            switch (type) 
            {
                case SELECT_HKEY:
                    describe(atts.get(Label.PREDICATE).get(0));
                    break;
                case PROJECT:
                    for (Explainer projection : atts.get(Label.PROJECTION))
                    {
                        describe(projection);
                        sb.append(", ");
                    }
                    sb.setLength(sb.length()-2);
                    break;
                case SCAN_OPERATOR:
                    if (name.equals("ValuesScan_Default"))
                    {
                        if (atts.containsKey(Label.EXPRESSIONS))
                        {
                            for (Explainer row : atts.get(Label.EXPRESSIONS))
                                sb.append(row.get()).append(", ");
                            sb.setLength(sb.length() - 2);
                        }
                    }
                    else if (name.equals("GroupScan_Default"))
                        sb.append(atts.get(Label.SCAN_OPTION).get(0).get()).append(" on ").append(atts.get(Label.GROUP_TABLE).get(0).get());
                    else if (name.equals("IndexScan_Default"))
                    {
                        sb.append(atts.get(Label.INDEX).get(0).get());
                        if (atts.containsKey(Label.COLUMN_NAME))
                        {
                            int i = 0;
                            if (atts.containsKey(Label.EQUAL_COMPARAND))
                                for (Explainer comparand : atts.get(Label.EQUAL_COMPARAND))
                                    sb.append(", ").append(atts.get(Label.COLUMN_NAME).get(i++).get()).append(" = ").append(comparand.get());
                            for (boolean first = true; i < atts.get(Label.COLUMN_NAME).size(); i++)
                            {
                                sb.append(", ").append(atts.get(Label.COLUMN_NAME).get(i).get());
                                if (first)
                                {
                                    Object hi, lo;
                                    if (atts.containsKey(Label.HIGH_COMPARAND))
                                    {
                                        hi = atts.get(Label.HIGH_COMPARAND).get(0).get();
                                        if (atts.containsKey(Label.LOW_COMPARAND))
                                        {
                                            lo = atts.get(Label.LOW_COMPARAND).get(0).get();
                                            if (atts.get(Label.HIGH_COMPARAND).get(1).get().equals("INCLUSIVE"))
                                                if (atts.get(Label.LOW_COMPARAND).get(1).get().equals("INCLUSIVE"))
                                                    sb.append(" BETWEEN ").append(lo).append(" AND ").append(hi);
                                                else
                                                    sb.append(" <= ").append(hi).append(" AND ").append(" > ").append(lo);
                                            else
                                                sb.append(" < ").append(hi).append(" AND ").append(atts.get(Label.LOW_COMPARAND).get(1).get().equals("INCLUSIVE") ? " >= " : " > ").append(lo);
                                        }
                                        else
                                            sb.append(atts.get(Label.HIGH_COMPARAND).get(1).get().equals("INCLUSIVE") ? " <= " : " < ").append(hi);
                                    }
                                    else if (atts.containsKey(Label.LOW_COMPARAND))
                                        sb.append(atts.get(Label.LOW_COMPARAND).get(1).get().equals("INCLUSIVE") ? " >= " : " > ").append(atts.get(Label.LOW_COMPARAND).get(0).get());
                                    first = false;
                                }
                            }
                        }
                    }
                    break;
                case LOOKUP_OPERATOR:
                    if (name.equals("AncestorLookup_Default"))
                    {
                        sb.append(atts.get(Label.GROUP_TABLE).get(0).get()).append(" -> ");
                        if (atts.containsKey(Label.TABLE_CORRELATION))
                        {
                            for (Explainer table : atts.get(Label.TABLE_CORRELATION))
                                sb.append(table.get()).append(", ");
                            sb.setLength(sb.length()-2);
                        }
                        else
                        {
                            for (Explainer table : atts.get(Label.ANCESTOR_TYPE))
                                sb.append(table.get()).append(", ");
                            if (!atts.get(Label.ANCESTOR_TYPE).isEmpty())
                            {
                                sb.setLength(sb.length() - 2);
                            }
                        }
                    }
                    else if (name.equals("AncestorLookup_Nested"))
                    {
                        if (atts.containsKey(Label.BINDING_POSITION))
                            sb.append(atts.get(Label.BINDING_POSITION).get(0).get());
                        else
                            sb.append(atts.get(Label.GROUP_TABLE).get(0).get());
                        sb.append(" -> ");
                        if (atts.containsKey(Label.TABLE_CORRELATION))
                        {
                            for (Explainer table : atts.get(Label.TABLE_CORRELATION))
                                sb.append(table.get()).append(", ");
                            sb.setLength(sb.length()-2);
                        }
                        else
                        {
                            for (Explainer table : atts.get(Label.ANCESTOR_TYPE))
                                sb.append(table.get()).append(", ");
                            if (!atts.get(Label.ANCESTOR_TYPE).isEmpty())
                            {
                                sb.setLength(sb.length() - 2);
                            }
                        }
                    }
                    else if (name.equals("BranchLookup_Default"))
                    {
                        sb.append(atts.get(Label.GROUP_TABLE).get(0).get()).append(" -> ");
                        if (atts.containsKey(Label.TABLE_CORRELATION))
                        {
                            for (Explainer table : atts.get(Label.TABLE_CORRELATION))
                                sb.append(table.get()).append(", ");
                            sb.setLength(sb.length()-2);
                        }
                        else
                            sb.append(atts.get(Label.OUTPUT_TYPE).get(0).get());
                        sb.append(" (via ").append(atts.get(Label.ANCESTOR_TYPE).get(0).get()).append(')');
                    }
                    else if (name.equals("BranchLookup_Nested"))
                    {
                        if (atts.containsKey(Label.BINDING_POSITION))
                            sb.append(atts.get(Label.BINDING_POSITION).get(0).get());
                        else
                            sb.append(atts.get(Label.GROUP_TABLE).get(0).get());
                        sb.append(" -> ");
                        if (atts.containsKey(Label.TABLE_CORRELATION))
                        {
                            for (Explainer table : atts.get(Label.TABLE_CORRELATION))
                                sb.append(table.get()).append(", ");
                            sb.setLength(sb.length()-2);
                        }
                        else
                            sb.append(atts.get(Label.OUTPUT_TYPE).get(0).get());
                        sb.append(" (via ").append(atts.get(Label.ANCESTOR_TYPE).get(0).get()).append(")");
                    }
                    break;
                case COUNT_OPERATOR:
                    sb.append("*");
                    if (name.equals("Count_TableStatus"));
                        sb.append(" FROM ").append(atts.get(Label.INPUT_TYPE).get(0).get());
                    break;
                case FILTER: // Doesn't seem to be in any of the tests
                    for (Explainer rowtype : atts.get(Label.KEEP_TYPE))
                    {
                        describe(rowtype);
                        sb.append(", ");
                    }
                    sb.setLength(sb.length()-2);
                    break;
                case FLATTEN_OPERATOR:
                    describe(atts.get(Label.PARENT_TYPE).get(0));
                    sb.append(" ").append(atts.get(Label.JOIN_OPTION).get(0).get()).append(" ");
                    describe(atts.get(Label.CHILD_TYPE).get(0));
                    break;
                case ORDERED:
                    sb.append("skip ");
                    describe(atts.get(Label.LEFT).get(0));
                    sb.append(" left, skip ");
                    describe(atts.get(Label.RIGHT).get(0));
                    sb.append(" right, compare ");
                    describe(atts.get(Label.NUM_COMPARE).get(0));
                    if (name.equals("HKeyUnion"))
                    {
                        sb.append(", shorten to ");
                        describe(atts.get(Label.OUTPUT_TYPE).get(0));
                    }
                    else if (name.equals("Intersect_Ordered"))
                    {
                        sb.append(", USING ");
                        describe(atts.get(Label.JOIN_OPTION).get(0));
                    }
                    break;
                case IF_EMPTY:
                    for (Explainer expression : atts.get(Label.OPERAND))
                    {
                        describe(expression);
                        sb.append(", ");
                    }
                    if (!atts.valuePairs().isEmpty())
                    {
                        sb.setLength(sb.length() - 2);
                    }
                    break;
                case LIMIT_OPERATOR:
                    describe(atts.get(Label.LIMIT).get(0));
                    break;
                case NESTED_LOOPS:
                    if (name.equals("Map_NestedLoops"))
                    {
                        if(atts.containsKey(Label.TABLE_CORRELATION))
                            sb.append(atts.get(Label.TABLE_CORRELATION).get(0).get());
                        else
                            sb.append("loop_").append(atts.get(Label.BINDING_POSITION).get(0).get());
                    }
                    else if (name.equals("Product_NestedLoops"))
                    {
                        describe(atts.get(Label.OUTER_TYPE).get(0));
                        sb.append(" x ");
                        describe(atts.get(Label.INNER_TYPE).get(0));
                    }
                    break;
                case SORT:
                    int i = 0;
                    for (Explainer ex : atts.get(Label.EXPRESSIONS))
                    {
                        describe(ex);
                        sb.append(' ').append(atts.get(Label.ORDERING).get(i++).get()).append(", ");
                    }
                    if (atts.containsKey(Label.LIMIT))
                    {
                        sb.append("LIMIT ").append(atts.get(Label.LIMIT).get(0).get()).append(", ");
                    }
                    sb.append(atts.get(Label.SORT_OPTION).get(0).get());
                    break;
                case DUI:
                    if (name.equals("Delete_Default"))
                    {
                        if (atts.containsKey(Label.TABLE_CORRELATION))
                            sb.append("FROM ").append(atts.get(Label.TABLE_CORRELATION).get(0).get());
                        else if (atts.containsKey(Label.TABLE_TYPE))
                            sb.append("FROM ").append(atts.get(Label.TABLE_TYPE).get(0).get());
                    }
                    else if (name.equals("Insert_Default"))
                    {
                        if (atts.containsKey(Label.TABLE_CORRELATION))
                            sb.append("INTO ").append(atts.get(Label.TABLE_CORRELATION).get(0).get());
                        else if (atts.containsKey(Label.TABLE_TYPE))
                            sb.append("INTO ").append(atts.get(Label.TABLE_TYPE).get(0).get());
                        if (atts.containsKey(Label.COLUMN_NAME))
                        {
                            sb.append('(');
                            for (Explainer ex : atts.get(Label.COLUMN_NAME))
                                sb.append(ex.get()).append(", ");
                            sb.setLength(sb.length()-2);
                            sb.append(')');
                        }
                    }
                    else if (name.equals("Update_Default"))
                    {
                        if (atts.containsKey(Label.TABLE_CORRELATION))
                            sb.append(atts.get(Label.TABLE_CORRELATION).get(0).get());
                        else if (atts.containsKey(Label.TABLE_TYPE))
                            sb.append(atts.get(Label.TABLE_TYPE).get(0).get());
                        if (atts.containsKey(Label.COLUMN_NAME))
                        {
                            sb.append(" SET ");
                            for (int j = 0; j < Math.min(atts.get(Label.COLUMN_NAME).size(), atts.get(Label.EXPRESSIONS).size()); j++)
                            {
                                sb.append(atts.get(Label.COLUMN_NAME).get(j).get()).append(" = ");
                                describe(atts.get(Label.EXPRESSIONS).get(j));
                                sb.append(", ");
                            }
                            sb.setLength(sb.length()-2);
                        }
                    }
                    break;
                case PHYSICAL_OPERATOR:
                    if (atts.containsKey(Label.GROUPING_OPTION))
                        sb.append(atts.get(Label.GROUPING_OPTION).get(0).get()).append(": ");
                    for (Explainer ex : atts.get(Label.AGGREGATORS))
                    {
                        sb.append(ex.get());
                        sb.append(", ");
                    }
                    sb.setLength(sb.length()-2);
                    break;
                case BLOOM_FILTER:
                    if (name.equals("Select_BloomFilter") && atts.containsKey(Label.BLOOM_FILTER))
                    {
                        sb.append(atts.get(Label.BLOOM_FILTER).get(0).get());
                    }
                    else if (name.equals("Using_BloomFilter"))
                    {
                        sb.append(atts.get(Label.BINDING_POSITION).get(0).get());
                        if (atts.containsKey(Label.EXPRESSIONS))
                            for (Explainer ex : atts.get(Label.EXPRESSIONS))
                                sb.append(", ").append(ex.get());
                    }
                    break;
                case DISTINCT:
                case UNION_ALL:
                case UNION:
                    break;
                default:
                    throw new UnsupportedOperationException("Formatter does not recognize " + type.name());
            }
            sb.append(")");
        }
        if (atts.containsKey(Label.INPUT_OPERATOR))
        {
            for (Explainer input : atts.get(Label.INPUT_OPERATOR))
            {
                newRow();
                for (int i = 0; i <= depth; i++)
                {
                    sb.append("  ");
                }
                describeOperator((CompoundExplainer) input, depth + 1);
            }
        }
    }
    
    private void newRow()
    {
        rows.add(sb.toString());
        sb.delete(0, sb.length());
    }

    private void describeRowType(CompoundExplainer opEx) {
        Attributes atts = opEx.get();
        if (atts.containsKey(Label.PARENT_TYPE) && atts.containsKey((Label.CHILD_TYPE)))
        {
            describe(atts.get(Label.PARENT_TYPE).get(0));
            sb.append(" - ");
            describe(atts.get(Label.CHILD_TYPE).get(0));
        }
        else
            sb.append(atts.get(Label.NAME).get(0).get());
    }
}