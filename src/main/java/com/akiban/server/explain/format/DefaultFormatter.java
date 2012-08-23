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
    
    public DefaultFormatter(boolean verbose) {
        this.verbose = verbose;
    }

    public List<String> describeToList(Explainer explainer) {
        describe(explainer);
        for (int i = 1; i <= numSubqueries; i++) {
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
        switch (explainer.getType().generalType()) {
        case SCALAR_VALUE:
            describePrimitive((PrimitiveExplainer)explainer);
            break;
        case OPERATOR:
            describeOperator((CompoundExplainer)explainer, 0);
            break;
        case ROWTYPE:
            describeRowType((CompoundExplainer)explainer);
            break;
        default:
            describeExpression((CompoundExplainer)explainer, needsParens, parentName);
        }
    }

    protected void describeExpression(CompoundExplainer explainer, boolean needsParens, String parentName) {
        Attributes atts = explainer.get();
        String name = atts.getValue(Label.NAME).toString();
        
        if (atts.containsKey(Label.INFIX_REPRESENTATION)) {
            Explainer leftExplainer = atts.valuePairs().get(0).getValue();
            Explainer rightExplainer = atts.valuePairs().get(1).getValue();
            if (name.equals(parentName) && atts.containsKey(Label.ASSOCIATIVE)) {
                if (Boolean.TRUE.equals(atts.getValue(Label.ASSOCIATIVE)))
                    needsParens = false;
            }
            if (needsParens)
                sb.append("(");
            describe(leftExplainer, sb, true, name);
            sb.append(" ").append(atts.getValue(Label.INFIX_REPRESENTATION)).append(" ");
            describe(rightExplainer, sb, true, name);
            if (needsParens)
                sb.append(")");
        }
        else if (explainer.getType().equals(Type.LITERAL)) {
            sb.append(atts.getValue(Label.OPERAND));
        }
        else if (explainer.getType().equals(Type.SUBQUERY)) {
            sb.append("SUBQUERY ").append(++numSubqueries);
            subqueries.add((CompoundExplainer)atts.getAttribute(Label.OPERAND));
        }
        else if (name.startsWith("CAST")) {
            sb.append(name.substring(0, 4)).append("(");
            describe(atts.getAttribute(Label.OPERAND));
            sb.append(" AS ").append(atts.getValue(Label.OUTPUT_TYPE)).append(")");
        }
        else if (name.equals("Bound")) {
            if (atts.containsKey(Label.COLUMN_NAME))
                sb.append(atts.getValue(Label.COLUMN_NAME));
            else {
                sb.append(name).append("(").append(atts.getValue(Label.BINDING_POSITION)).append(",");
                describe(atts.getAttribute(Label.OPERAND));
                sb.append(")");
            }
        }
        else if (name.equals("Variable")) {
            int pos = ((Number)atts.getValue(Label.BINDING_POSITION)).intValue();
            sb.append("$").append(pos+1);
        }
        else if (name.equals("Field")) {
            if (atts.containsKey(Label.COLUMN_NAME))
                sb.append(atts.getValue(Label.COLUMN_NAME));
            else
                sb.append(name).append('(').append(atts.getValue(Label.BINDING_POSITION)).append(')');
        }
        else {
            sb.append(name).append('(');
            if (atts.containsKey(Label.OPERAND)) {
                for (Explainer entry : atts.get(Label.OPERAND)) {
                    describe(entry);
                    sb.append(", ");
                }
                sb.setLength(sb.length()-2);
            }
            sb.append(')');
        }
    }

    protected void describePrimitive(PrimitiveExplainer explainer) {

        if (explainer.getType()==Type.STRING) {
            sb.append('\'').append(explainer.get()).append('\'');
        }
        else {
            sb.append(explainer.get());
        }
    }

    protected void describeOperator(CompoundExplainer explainer, int depth) {
        
        Attributes atts = explainer.get();
        Type type = explainer.getType();
        String name = atts.getValue(Label.NAME).toString();
        
        if (!verbose) {
            sb.append(name.substring(0, name.indexOf('_')));
            switch (type) {
            case LOOKUP_OPERATOR:
                sb.append('(');
                if (atts.containsKey(Label.TABLE_CORRELATION)) {
                    for (Explainer table : atts.get(Label.TABLE_CORRELATION))
                        sb.append(table.get()).append(", ");
                    sb.setLength(sb.length()-2);
                }
                else {
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
                if (name.equals("Delete_Default")) {
                    if (atts.containsKey(Label.TABLE_CORRELATION))
                        sb.append("FROM ").append(atts.getValue(Label.TABLE_CORRELATION));
                    else if (atts.containsKey(Label.TABLE_TYPE))
                        sb.append("FROM ").append(atts.getValue(Label.TABLE_TYPE));
                }
                else if (name.equals("Insert_Default")) {
                    if (atts.containsKey(Label.TABLE_CORRELATION))
                        sb.append("INTO ").append(atts.getValue(Label.TABLE_CORRELATION));
                    else if (atts.containsKey(Label.TABLE_TYPE))
                        sb.append("INTO ").append(atts.getValue(Label.TABLE_TYPE));
                }
                break;
            case SCAN_OPERATOR:
                if (name.equals("IndexScan_Default"))
                    sb.append(atts.getValue(Label.INDEX));
                break;
            default:
                // Nothing needed, as most operators display nothing in brief mode
            }
        }
        else {
            sb.append(name).append("(");
            switch (type) {
            case SELECT_HKEY:
                describe(atts.getAttribute(Label.PREDICATE));
                break;
            case PROJECT:
                for (Explainer projection : atts.get(Label.PROJECTION)) {
                    describe(projection);
                    sb.append(", ");
                }
                sb.setLength(sb.length()-2);
                break;
            case SCAN_OPERATOR:
                if (name.equals("ValuesScan_Default")) {
                    if (atts.containsKey(Label.EXPRESSIONS)) {
                        for (Explainer row : atts.get(Label.EXPRESSIONS))
                            sb.append(row.get()).append(", ");
                        sb.setLength(sb.length() - 2);
                    }
                }
                else if (name.equals("GroupScan_Default")) {
                    sb.append(atts.getValue(Label.SCAN_OPTION)).append(" on ").append(atts.getValue(Label.GROUP_TABLE));
                }
                else if (name.equals("IndexScan_Default")) {
                    sb.append(atts.getValue(Label.INDEX));
                    if (atts.containsKey(Label.COLUMN_NAME)) {
                        int i = 0;
                        if (atts.containsKey(Label.EQUAL_COMPARAND))
                            for (Explainer comparand : atts.get(Label.EQUAL_COMPARAND))
                                sb.append(", ").append(atts.get(Label.COLUMN_NAME).get(i++).get()).append(" = ").append(comparand.get());
                        for (boolean first = true; i < atts.get(Label.COLUMN_NAME).size(); i++) {
                            sb.append(", ").append(atts.get(Label.COLUMN_NAME).get(i).get());
                            if (first) {
                                Object hi, lo;
                                if (atts.containsKey(Label.HIGH_COMPARAND)) {
                                    hi = atts.getValue(Label.HIGH_COMPARAND);
                                    if (atts.containsKey(Label.LOW_COMPARAND)) {
                                        lo = atts.getValue(Label.LOW_COMPARAND);
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
                                    sb.append(atts.get(Label.LOW_COMPARAND).get(1).get().equals("INCLUSIVE") ? " >= " : " > ").append(atts.getValue(Label.LOW_COMPARAND));
                                first = false;
                            }
                        }
                    }
                }
                break;
            case LOOKUP_OPERATOR:
                if (name.equals("AncestorLookup_Default")) {
                    sb.append(atts.getValue(Label.GROUP_TABLE)).append(" -> ");
                    if (atts.containsKey(Label.TABLE_CORRELATION)) {
                        for (Explainer table : atts.get(Label.TABLE_CORRELATION))
                            sb.append(table.get()).append(", ");
                        sb.setLength(sb.length()-2);
                    }
                    else {
                        for (Explainer table : atts.get(Label.ANCESTOR_TYPE))
                            sb.append(table.get()).append(", ");
                        if (!atts.get(Label.ANCESTOR_TYPE).isEmpty()) {
                            sb.setLength(sb.length() - 2);
                        }
                    }
                }
                else if (name.equals("AncestorLookup_Nested")) {
                    if (atts.containsKey(Label.BINDING_POSITION))
                        sb.append(atts.getValue(Label.BINDING_POSITION));
                    else
                        sb.append(atts.getValue(Label.GROUP_TABLE));
                    sb.append(" -> ");
                    if (atts.containsKey(Label.TABLE_CORRELATION)) {
                        for (Explainer table : atts.get(Label.TABLE_CORRELATION))
                            sb.append(table.get()).append(", ");
                        sb.setLength(sb.length()-2);
                    }
                    else {
                        for (Explainer table : atts.get(Label.ANCESTOR_TYPE))
                            sb.append(table.get()).append(", ");
                        if (!atts.get(Label.ANCESTOR_TYPE).isEmpty()) {
                            sb.setLength(sb.length() - 2);
                        }
                    }
                }
                else if (name.equals("BranchLookup_Default")) {
                    sb.append(atts.getValue(Label.GROUP_TABLE)).append(" -> ");
                    if (atts.containsKey(Label.TABLE_CORRELATION)) {
                        for (Explainer table : atts.get(Label.TABLE_CORRELATION))
                            sb.append(table.get()).append(", ");
                        sb.setLength(sb.length()-2);
                    }
                    else
                        sb.append(atts.getValue(Label.OUTPUT_TYPE));
                    sb.append(" (via ").append(atts.getValue(Label.ANCESTOR_TYPE)).append(')');
                }
                else if (name.equals("BranchLookup_Nested")) {
                    if (atts.containsKey(Label.BINDING_POSITION))
                        sb.append(atts.getValue(Label.BINDING_POSITION));
                    else
                        sb.append(atts.getValue(Label.GROUP_TABLE));
                    sb.append(" -> ");
                    if (atts.containsKey(Label.TABLE_CORRELATION)) {
                        for (Explainer table : atts.get(Label.TABLE_CORRELATION))
                            sb.append(table.get()).append(", ");
                        sb.setLength(sb.length()-2);
                    }
                    else
                        sb.append(atts.getValue(Label.OUTPUT_TYPE));
                    sb.append(" (via ").append(atts.getValue(Label.ANCESTOR_TYPE)).append(")");
                }
                break;
            case COUNT_OPERATOR:
                sb.append("*");
                if (name.equals("Count_TableStatus"));
                sb.append(" FROM ").append(atts.getValue(Label.INPUT_TYPE));
                break;
            case FILTER: // Doesn't seem to be in any of the tests
                for (Explainer rowtype : atts.get(Label.KEEP_TYPE)) {
                    describe(rowtype);
                    sb.append(", ");
                }
                sb.setLength(sb.length()-2);
                break;
            case FLATTEN_OPERATOR:
                describe(atts.getAttribute(Label.PARENT_TYPE));
                sb.append(" ").append(atts.getValue(Label.JOIN_OPTION)).append(" ");
                describe(atts.getAttribute(Label.CHILD_TYPE));
                break;
            case ORDERED:
                sb.append("skip ");
                describe(atts.getAttribute(Label.LEFT));
                sb.append(" left, skip ");
                describe(atts.getAttribute(Label.RIGHT));
                sb.append(" right, compare ");
                describe(atts.getAttribute(Label.NUM_COMPARE));
                if (name.equals("HKeyUnion")) {
                    sb.append(", shorten to ");
                    describe(atts.getAttribute(Label.OUTPUT_TYPE));
                }
                else if (name.equals("Intersect_Ordered")) {
                    sb.append(", USING ");
                    describe(atts.getAttribute(Label.JOIN_OPTION));
                }
                break;
            case IF_EMPTY:
                for (Explainer expression : atts.get(Label.OPERAND)) {
                    describe(expression);
                    sb.append(", ");
                }
                if (!atts.valuePairs().isEmpty()) {
                    sb.setLength(sb.length() - 2);
                }
                break;
            case LIMIT_OPERATOR:
                describe(atts.getAttribute(Label.LIMIT));
                break;
            case NESTED_LOOPS:
                if (name.equals("Map_NestedLoops")) {
                    if(atts.containsKey(Label.TABLE_CORRELATION))
                        sb.append(atts.getValue(Label.TABLE_CORRELATION));
                    else
                        sb.append("loop_").append(atts.getValue(Label.BINDING_POSITION));
                }
                else if (name.equals("Product_NestedLoops")) {
                    describe(atts.getAttribute(Label.OUTER_TYPE));
                    sb.append(" x ");
                    describe(atts.getAttribute(Label.INNER_TYPE));
                }
                break;
            case SORT:
                int i = 0;
                for (Explainer ex : atts.get(Label.EXPRESSIONS)) {
                    describe(ex);
                    sb.append(' ').append(atts.get(Label.ORDERING).get(i++).get()).append(", ");
                }
                if (atts.containsKey(Label.LIMIT)) {
                    sb.append("LIMIT ").append(atts.getValue(Label.LIMIT)).append(", ");
                }
                sb.append(atts.getValue(Label.SORT_OPTION));
                break;
            case DUI:
                if (name.equals("Delete_Default")) {
                    if (atts.containsKey(Label.TABLE_CORRELATION))
                        sb.append("FROM ").append(atts.getValue(Label.TABLE_CORRELATION));
                    else if (atts.containsKey(Label.TABLE_TYPE))
                        sb.append("FROM ").append(atts.getValue(Label.TABLE_TYPE));
                }
                else if (name.equals("Insert_Default")) {
                    if (atts.containsKey(Label.TABLE_CORRELATION))
                        sb.append("INTO ").append(atts.getValue(Label.TABLE_CORRELATION));
                    else if (atts.containsKey(Label.TABLE_TYPE))
                        sb.append("INTO ").append(atts.getValue(Label.TABLE_TYPE));
                    if (atts.containsKey(Label.COLUMN_NAME)) {
                        sb.append('(');
                        for (Explainer ex : atts.get(Label.COLUMN_NAME))
                            sb.append(ex.get()).append(", ");
                        sb.setLength(sb.length()-2);
                        sb.append(')');
                    }
                }
                else if (name.equals("Update_Default")) {
                    if (atts.containsKey(Label.TABLE_CORRELATION))
                        sb.append(atts.getValue(Label.TABLE_CORRELATION));
                    else if (atts.containsKey(Label.TABLE_TYPE))
                        sb.append(atts.getValue(Label.TABLE_TYPE));
                    if (atts.containsKey(Label.COLUMN_NAME)) {
                        sb.append(" SET ");
                        for (int j = 0; j < Math.min(atts.get(Label.COLUMN_NAME).size(), atts.get(Label.EXPRESSIONS).size()); j++) {
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
                    sb.append(atts.getValue(Label.GROUPING_OPTION)).append(": ");
                for (Explainer ex : atts.get(Label.AGGREGATORS)) {
                    sb.append(ex.get());
                    sb.append(", ");
                }
                sb.setLength(sb.length()-2);
                break;
            case BLOOM_FILTER:
                if (name.equals("Select_BloomFilter") && atts.containsKey(Label.BLOOM_FILTER)) {
                    sb.append(atts.getValue(Label.BLOOM_FILTER));
                }
                else if (name.equals("Using_BloomFilter")) {
                    sb.append(atts.getValue(Label.BINDING_POSITION));
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
        if (atts.containsKey(Label.INPUT_OPERATOR)) {
            for (Explainer input : atts.get(Label.INPUT_OPERATOR)) {
                newRow();
                for (int i = 0; i <= depth; i++) {
                    sb.append("  ");
                }
                describeOperator((CompoundExplainer) input, depth + 1);
            }
        }
    }
    
    private void newRow() {
        rows.add(sb.toString());
        sb.delete(0, sb.length());
    }

    private void describeRowType(CompoundExplainer opEx) {
        Attributes atts = opEx.get();
        if (atts.containsKey(Label.PARENT_TYPE) && atts.containsKey((Label.CHILD_TYPE))) {
            describe(atts.getAttribute(Label.PARENT_TYPE));
            sb.append(" - ");
            describe(atts.getAttribute(Label.CHILD_TYPE));
        }
        else {
            sb.append(atts.getValue(Label.NAME));
        }
    }
}