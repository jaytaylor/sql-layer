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

// NOTE: Should only depend on explain objects and standard Java.
// Should theoretically be able to run outside of server.

import com.akiban.server.explain.*;

import java.util.ArrayList;
import java.util.List;

public class DefaultFormatter
{
    private String defaultSchemaName;
    private boolean verbose;
    private int numSubqueries = 0;
    private List<CompoundExplainer> subqueries = new ArrayList<CompoundExplainer>();
    private StringBuilder sb = new StringBuilder();
    private List<String> rows = new ArrayList<String>();
    
    public DefaultFormatter(String defaultSchemaName, boolean verbose) {
        this.defaultSchemaName = defaultSchemaName;
        this.verbose = verbose;
    }

    public List<String> format(Explainer explainer) {
        append(explainer);
        for (int i = 0; i < numSubqueries; i++) {
            newRow();
            appendSubqueryBody(subqueries.get(i), i+1);
        }
        newRow();
        return rows;
    }
    
    protected void append(Explainer explainer) {
        append(explainer, false, null);
    }

    protected void append(Explainer explainer, boolean needsParens, String parentName) {
        switch (explainer.getType().generalType()) {
        case SCALAR_VALUE:
            appendPrimitive((PrimitiveExplainer)explainer);
            break;
        case OPERATOR:
            appendOperator((CompoundExplainer)explainer, 0);
            break;
        case ROWTYPE:
            appendRowType((CompoundExplainer)explainer);
            break;
        case ROW:
            appendRow((CompoundExplainer)explainer);
            break;
        default:
            appendExpression((CompoundExplainer)explainer, needsParens, parentName);
        }
    }

    protected void appendPrimitive(PrimitiveExplainer explainer) {
        sb.append(explainer.get());
    }

    protected void appendExpression(CompoundExplainer explainer, boolean needsParens, String parentName) {
        switch (explainer.getType()) {
        case FIELD:
            appendField(explainer);
            break;
        case FUNCTION:
        case BINARY_OPERATOR:
            appendFunction(explainer, needsParens, parentName);
            break;
        case SUBQUERY:
            appendSubquery(explainer);
            break;
        case LITERAL:
            appendLiteral(explainer);
            break;
        case VARIABLE:
            appendVariable(explainer);
            break;
        }
    }

    protected void appendFunction(CompoundExplainer explainer, boolean needsParens, String parentName) {
        Attributes atts = explainer.get();
        String name = (String)atts.getValue(Label.NAME);
        
        if (atts.containsKey(Label.INFIX_REPRESENTATION)) {
            Explainer leftExplainer = atts.valuePairs().get(0).getValue();
            Explainer rightExplainer = atts.valuePairs().get(1).getValue();
            if (name.equals(parentName) && atts.containsKey(Label.ASSOCIATIVE)) {
                if (Boolean.TRUE.equals(atts.getValue(Label.ASSOCIATIVE)))
                    needsParens = false;
            }
            if (needsParens)
                sb.append('(');
            append(leftExplainer, true, name);
            sb.append(' ').append(atts.getValue(Label.INFIX_REPRESENTATION)).append(' ');
            append(rightExplainer, true, name);
            if (needsParens)
                sb.append(')');
        }
        else if (name.startsWith("CAST")) {
            sb.append(name.substring(0, 4)).append('(');
            append(atts.getAttribute(Label.OPERAND));
            sb.append(" AS ").append(atts.getValue(Label.OUTPUT_TYPE)).append(')');
        }
        else {
            sb.append(name).append('(');
            if (atts.containsKey(Label.OPERAND)) {
                for (Explainer entry : atts.get(Label.OPERAND)) {
                    append(entry);
                    sb.append(", ");
                }
                sb.setLength(sb.length()-2);
            }
            sb.append(')');
        }
    }

    protected void appendField(CompoundExplainer explainer) {
        Attributes atts = explainer.get();
        boolean started = false;
        if (atts.containsKey(Label.TABLE_CORRELATION)) {
            sb.append(atts.getValue(Label.TABLE_CORRELATION));
            started = true;
        }
        else if (atts.containsKey(Label.TABLE_NAME)) {
            appendTableName(atts);
            started = true;
        }
        if (atts.containsKey(Label.COLUMN_NAME)) {
            if (started) sb.append('.');
            sb.append(atts.getValue(Label.COLUMN_NAME));
        }
        else if (started) {
            sb.append('[').append(atts.getValue(Label.POSITION)).append(']');
        }
        else {
            sb.append(atts.getValue(Label.NAME)).append('(');
            if (atts.containsKey(Label.BINDING_POSITION)) {
                sb.append(atts.getValue(Label.BINDING_POSITION)).append(", ");
            }
            sb.append(atts.getValue(Label.POSITION)).append(')');
        }
    }

    protected void appendSubquery(CompoundExplainer explainer) {
        Attributes atts = explainer.get();
        sb.append(atts.getValue(Label.NAME)).append('(');
        sb.append("SUBQUERY ").append(++numSubqueries).append(')');
        subqueries.add(explainer);
    }

    protected void appendSubqueryBody(CompoundExplainer explainer, int n) {
        Attributes atts = explainer.get();
        String name = (String)atts.getValue(Label.NAME);
        sb.append("SUBQUERY ").append(n).append(": ").append(name).append('(');
        if (atts.containsKey(Label.EXPRESSIONS)) {
            for (Explainer ex : atts.get(Label.EXPRESSIONS)) {
                append(ex);
                sb.append(", ");
            }
            sb.setLength(sb.length() - 2);
        }
        sb.append(')');
        newRow();
        sb.append("  ");
        appendOperator((CompoundExplainer)atts.getAttribute(Label.OPERAND), 1);
    }

    protected void appendLiteral(CompoundExplainer explainer) {
        Attributes atts = explainer.get();
        sb.append(atts.getValue(Label.OPERAND));
    }

    protected void appendVariable(CompoundExplainer explainer) {
        Attributes atts = explainer.get();
        int pos = ((Number)atts.getValue(Label.BINDING_POSITION)).intValue();
        sb.append("$").append(pos+1);
    }

    protected void appendOperator(CompoundExplainer explainer, int depth) {
        Attributes atts = explainer.get();
        String name = (String)atts.getValue(Label.NAME);
        sb.append(verbose ? name : name.substring(0, name.indexOf('_'))).append('(');
        switch (explainer.getType()) {
        case SELECT_HKEY:
            appendSelectOperator(name, atts);
            break;
        case PROJECT:
            appendProjectOperator(name, atts);
            break;
        case SCAN_OPERATOR:
            appendScanOperator(name, atts);
            break;
        case LOOKUP_OPERATOR:
            appendLookupOperator(name, atts);
            break;
        case COUNT_OPERATOR:
            appendCountOperator(name, atts);
            break;
        case FILTER:
            appendFilterOperator(name, atts);
            break;
        case FLATTEN_OPERATOR:
            appendFlattenOperator(name, atts);
            break;
        case ORDERED:
            appendOrderedOperator(name, atts);
            break;
        case IF_EMPTY:
            appendIfEmptyOperator(name, atts);
            break;
        case LIMIT_OPERATOR:
            appendLimitOperator(name, atts);
            break;
        case NESTED_LOOPS:
            appendNestedLoopsOperator(name, atts);
            break;
        case SORT:
            appendSortOperator(name, atts);
            break;
        case DUI:
            appendDUIOperator(name, atts);
            break;
        case AGGREGATE:
            appendAggregateOperator(name, atts);
            break;
        case BLOOM_FILTER:
            appendBloomFilterOperator(name, atts);
            break;
        case DISTINCT:
            appendDistinctOperator(name, atts);
            break;
        case UNION_ALL:
        case UNION:
            appendUnionOperator(name, atts);
            break;
        default:
            throw new UnsupportedOperationException("Formatter does not recognize " + 
                                                    explainer.getType());
        }
        sb.append(')');
        if (atts.containsKey(Label.INPUT_OPERATOR)) {
            for (Explainer input : atts.get(Label.INPUT_OPERATOR)) {
                newRow();
                for (int i = 0; i <= depth; i++) {
                    sb.append("  ");
                }
                appendOperator((CompoundExplainer)input, depth + 1);
            }
        }
    }            
        
    protected void appendSelectOperator(String name, Attributes atts) {
        if (verbose) {
            append(atts.getAttribute(Label.PREDICATE));
        }
    }

    protected void appendProjectOperator(String name, Attributes atts) {
        if (verbose) {
            for (Explainer projection : atts.get(Label.PROJECTION)) {
                append(projection);
                sb.append(", ");
            }
            sb.setLength(sb.length() - 2);
        }
    }

    protected void appendScanOperator(String name, Attributes atts) {
        if (name.equals("IndexScan_Default")) {
            appendIndexScanOperator(atts);
        }
        if (name.equals("ValuesScan_Default")) {
            if (verbose) {
                if (atts.containsKey(Label.EXPRESSIONS)) {
                    for (Explainer row : atts.get(Label.EXPRESSIONS)) {
                        append(row);
                        sb.append(", ");
                    }
                    sb.setLength(sb.length() - 2);
                }
            }
        }
        else if (name.equals("GroupScan_Default")) {
            if (verbose) {
                String opt = (String)atts.getValue(Label.SCAN_OPTION);
                if (!opt.equals("full scan"))
                    sb.append(opt).append(" on ");
            }
            appendTableName(atts);
        }
    }

    protected void appendIndexScanOperator(Attributes atts) {
        append(atts.getAttribute(Label.INDEX));
        if (verbose) {
            if (atts.containsKey(Label.INDEX_KIND) &&
                "SPATIAL".equals(atts.getValue(Label.INDEX_KIND))) {
                sb.append(", (");
                int ndims = atts.get(Label.LOW_COMPARAND).size();
                for (int i = 0; i < ndims; i++) {
                    append(atts.get(Label.COLUMN_NAME).get(i));
                    sb.append(", ");
                }
                sb.setLength(sb.length() - 2);
                sb.append(')');
                if (!atts.containsKey(Label.HIGH_COMPARAND)) {
                    sb.append(" ZNEAR(");
                    for (Explainer ex : atts.get(Label.LOW_COMPARAND)) {
                        append(ex);
                        sb.append(", ");
                    }
                    sb.setLength(sb.length() - 2);
                    sb.append(')');
                }
                else {
                    sb.append(" BETWEEN (");
                    for (Explainer ex : atts.get(Label.LOW_COMPARAND)) {
                        append(ex);
                        sb.append(", ");
                    }
                    sb.setLength(sb.length() - 2);
                    sb.append(") AND (");
                    for (Explainer ex : atts.get(Label.HIGH_COMPARAND)) {
                        append(ex);
                        sb.append(", ");
                    }
                    sb.setLength(sb.length() - 2);
                    sb.append(')');
                }
            }
            else {
                int ncols = atts.get(Label.COLUMN_NAME).size();
                int nequals = 0;
                if (atts.containsKey(Label.EQUAL_COMPARAND))
                    nequals = atts.get(Label.EQUAL_COMPARAND).size();
                if (atts.containsKey(Label.USED_COLUMNS)) {
                    // Don't display non-key columns if not used.
                    ncols = ((Number)atts.getValue(Label.USED_COLUMNS)).intValue();
                    int nconds = nequals;
                    if (atts.containsKey(Label.LOW_COMPARAND) ||
                        atts.containsKey(Label.HIGH_COMPARAND))
                        nconds++;
                    if (ncols < nconds)
                        ncols = nconds;
                }
                int norders = atts.get(Label.ORDERING).size();
                if (atts.containsKey(Label.ORDER_EFFECTIVENESS) &&
                    "NONE".equals(atts.getValue(Label.ORDER_EFFECTIVENESS))) {
                    // No need to display ordering if not used.
                    norders = 0;
                }
                while (norders > nequals+1) {
                    if (!atts.get(Label.ORDERING).get(norders-1).equals(atts.get(Label.ORDERING).get(norders-2)))
                        break;
                    norders--;
                }
                String indexSchema = (String)((CompoundExplainer)atts.getAttribute(Label.INDEX)).get().getValue(Label.TABLE_SCHEMA);
                if ("".equals(indexSchema))
                    indexSchema = null; // Group index.
                String indexTable = (String)((CompoundExplainer)atts.getAttribute(Label.INDEX)).get().getValue(Label.TABLE_NAME);
                for (int i = 0; i < ncols; i++) {
                    sb.append(", ");
                    String columnSchema = (String)atts.get(Label.TABLE_SCHEMA).get(i).get();
                    String columnTable = (String)atts.get(Label.TABLE_NAME).get(i).get();
                    if ((indexSchema != null) &&
                        !indexSchema.equals(columnSchema))
                        sb.append(columnSchema).append('.').append(columnTable).append('.');
                    else if ((indexSchema == null) ||
                             !indexTable.equals(columnTable))
                        sb.append(columnTable).append('.');
                    append(atts.get(Label.COLUMN_NAME).get(i));
                    if (i < nequals) {
                        Explainer comparand = atts.get(Label.EQUAL_COMPARAND).get(i);
                        if (isLiteralNull(comparand))
                            sb.append(" IS NULL");
                        else {
                            sb.append(" = ");
                            append(comparand);
                        }
                    }
                    else {
                        if (i == nequals) {
                            Explainer lo = null, hi = null;
                            boolean loInc = false, hiInc = false;
                            if (atts.containsKey(Label.LOW_COMPARAND)) {
                                lo = atts.get(Label.LOW_COMPARAND).get(0);
                                loInc = (Boolean)atts.get(Label.LOW_COMPARAND).get(1).get();
                                if (!loInc && isLiteralNull(lo)) lo = null;
                            }
                            if (atts.containsKey(Label.HIGH_COMPARAND)) {
                                hi = atts.get(Label.HIGH_COMPARAND).get(0);
                                hiInc = (Boolean)atts.get(Label.HIGH_COMPARAND).get(1).get();
                                if (!hiInc && isLiteralNull(hi)) hi = null;
                            }
                            if (loInc && hiInc) {
                                sb.append(" BETWEEN ");
                                append(lo);
                                sb.append(" AND ");
                                append(hi);
                            }
                            else {
                                if (lo != null) {
                                    sb.append((loInc) ? " >= " : " > ");
                                    append(lo);
                                }
                                if (hi != null) {
                                    if (lo != null) sb.append(" AND");
                                    sb.append((hiInc) ? " <= " : " < ");
                                    append(hi);
                                }
                            }
                        }
                        if (i < norders) {
                            sb.append(" ");
                            append(atts.get(Label.ORDERING).get(i));
                        }
                    }
                }
            }
        }
    }

    private static boolean isLiteralNull(Explainer explainer) {
        return ((explainer.getType() == Type.LITERAL) &&
                "NULL".equals(((CompoundExplainer)explainer).get().getValue(Label.OPERAND)));
    }

    protected void appendLookupOperator(String name, Attributes atts) {
        if (verbose) {
            append(atts.getAttribute(Label.INPUT_TYPE));
            sb.append(" -> ");
        }
        for (Explainer table : atts.get(Label.OUTPUT_TYPE)) {
            append(table);
            sb.append(", ");
        }
        sb.setLength(sb.length() - 2);
        if (verbose && atts.containsKey(Label.ANCESTOR_TYPE)) {
            sb.append(" (via ");
            append(atts.getAttribute(Label.ANCESTOR_TYPE));
            sb.append(')');
        }
    }

    protected void appendCountOperator(String name, Attributes atts) {
        sb.append("*");
        if (verbose && name.equals("Count_TableStatus")) {
            sb.append(" FROM ");
            append(atts.getAttribute(Label.INPUT_TYPE));
        }
    }

    protected void appendFilterOperator(String name, Attributes atts) {
        if (verbose) {
            for (Explainer table : atts.get(Label.KEEP_TYPE)) {
                append(table);
                sb.append(", ");
            }
            sb.setLength(sb.length()-2);
        }
    }

    protected void appendFlattenOperator(String name, Attributes atts) {
        if (verbose) {
            append(atts.getAttribute(Label.PARENT_TYPE));
            sb.append(' ').append(atts.getValue(Label.JOIN_OPTION)).append(' ');
            append(atts.getAttribute(Label.CHILD_TYPE));
        }
    }

    protected void appendOrderedOperator(String name, Attributes atts) {
        if (verbose) {
            sb.append("skip ");
            append(atts.getAttribute(Label.LEFT));
            sb.append(" left, skip ");
            append(atts.getAttribute(Label.RIGHT));
            sb.append(" right, compare ");
            append(atts.getAttribute(Label.NUM_COMPARE));
            if (name.equals("HKeyUnion")) {
                sb.append(", shorten to ");
                append(atts.getAttribute(Label.OUTPUT_TYPE));
            }
            else if (name.equals("Intersect_Ordered")) {
                String join = (String)atts.getValue(Label.JOIN_OPTION);
                if (!"INNER".equals(join)) {
                    sb.append(", USING ").append(join);
                }
            }
        }
    }

    protected void appendIfEmptyOperator(String name, Attributes atts) {
        if (verbose) {
            for (Explainer expression : atts.get(Label.OPERAND)) {
                append(expression);
                sb.append(", ");
            }
            if (!atts.valuePairs().isEmpty()) {
                sb.setLength(sb.length() - 2);
            }
        }
    }

    protected void appendLimitOperator(String name, Attributes atts) {
        if (verbose) {
            append(atts.getAttribute(Label.LIMIT));
        }
    }

    protected void appendNestedLoopsOperator(String name, Attributes atts) {
        if (verbose) {
            if (name.equals("Map_NestedLoops")) {
                // Label the loop?
            }
            else if (name.equals("Product_NestedLoops")) {
                append(atts.getAttribute(Label.OUTER_TYPE));
                sb.append(" x ");
                append(atts.getAttribute(Label.INNER_TYPE));
            }
        }
    }

    protected void appendSortOperator(String name, Attributes atts) {
        if (verbose) {
            int i = 0;
            for (Explainer ex : atts.get(Label.EXPRESSIONS)) {
                append(ex);
                sb.append(' ').append(atts.get(Label.ORDERING).get(i++).get()).append(", ");
            }
            if (atts.containsKey(Label.LIMIT)) {
                sb.append("LIMIT ").append(atts.getValue(Label.LIMIT)).append(", ");
            }
            String opt = (String)atts.getValue(Label.SORT_OPTION);
            if (opt.equals("PRESERVE_DUPLICATES"))
                sb.setLength(sb.length() - 2);
            else
                sb.append(opt.replace('_', ' '));
        }
    }

    protected void appendDUIOperator(String name, Attributes atts) {
        if (name.equals("Delete_Default")) {
            if (atts.containsKey(Label.TABLE_NAME)) {
                sb.append("FROM ");
                appendTableName(atts);
            }
            else if (atts.containsKey(Label.TABLE_TYPE)) {
                sb.append("FROM ");
                append(atts.getAttribute(Label.TABLE_TYPE));
            }
        }
        else if (name.equals("Insert_Default") || 
                    name.equals("Insert_Returning")) {
            if (atts.containsKey(Label.TABLE_NAME)) {
                sb.append("INTO ");
                appendTableName(atts);
            }
            else if (atts.containsKey(Label.TABLE_TYPE)) {
                sb.append("INTO ");
                append(atts.getAttribute(Label.TABLE_TYPE));
            }
            if (verbose) {
                if (atts.containsKey(Label.COLUMN_NAME)) {
                    sb.append('(');
                    for (Explainer ex : atts.get(Label.COLUMN_NAME))
                        sb.append(ex.get()).append(", ");
                    sb.setLength(sb.length()-2);
                    sb.append(')');
                }
            }
        }
        else if (name.equals("Update_Default")) {
            if (atts.containsKey(Label.TABLE_NAME)) {
                appendTableName(atts);
            }
            else if (atts.containsKey(Label.TABLE_TYPE)) {
                append(atts.getAttribute(Label.TABLE_TYPE));
            }
            if (verbose) {
                if (atts.containsKey(Label.COLUMN_NAME)) {
                    sb.append(" SET ");
                    int ncols = Math.min(atts.get(Label.COLUMN_NAME).size(), 
                                         atts.get(Label.EXPRESSIONS).size());
                    for (int j = 0; j < ncols; j++) {
                        sb.append(atts.get(Label.COLUMN_NAME).get(j).get());
                        sb.append(" = ");
                        append(atts.get(Label.EXPRESSIONS).get(j));
                        sb.append(", ");
                    }
                    sb.setLength(sb.length()-2);
                }
            }
        }
    }

    protected void appendAggregateOperator(String name, Attributes atts) {
        int nkeys = ((Number)atts.getValue(Label.GROUPING_OPTION)).intValue();
        List<Explainer> aggrs = atts.get(Label.AGGREGATORS);
        if (!verbose) {
            if (nkeys > 0) {
                sb.append("group by ").append(nkeys);
                if (aggrs != null) sb.append(", ");
            }
            if (aggrs != null) {
                sb.append("aggregate ").append(aggrs.size());
            }
        }
        else {
            if (nkeys > 0) {
                sb.append("GROUP BY ");
                if (!appendProjectColumns(atts, nkeys)) {
                    // Fallback is just count.
                    sb.append(nkeys).append(" field");
                    if (nkeys > 1) sb.append("s");
                }
            }
            if (aggrs != null) {
                if (nkeys > 0) sb.append(": ");
                for (Explainer aggr : aggrs) {
                    append(aggr);
                    sb.append(", ");
                }
                sb.setLength(sb.length() - 2);
            }
        }
    }

    protected void appendBloomFilterOperator(String name, Attributes atts) {
        if (verbose) {
            if (name.equals("Using_BloomFilter")) {
                appendProjectColumns(atts, -1);
            }            
            else if (name.equals("Select_BloomFilter")) {
                if (atts.containsKey(Label.EXPRESSIONS)) {
                    for (Explainer ex : atts.get(Label.EXPRESSIONS)) {
                        append(ex);
                        sb.append(", ");
                    }
                    sb.setLength(sb.length() - 2);
                }
            }
        }
    }

    // If all the inputs are simple columns, display their names.
    protected boolean appendProjectColumns(Attributes atts, int nfields) {
        int olen = sb.length();
        Attributes inputOperator = ((CompoundExplainer)atts.get(Label.INPUT_OPERATOR).get(0)).get();
        boolean allcols = false;
        if (inputOperator.getValue(Label.NAME).equals("Project_Default")) {
            allcols = true;
            List<Explainer> fields = inputOperator.get(Label.PROJECTION);
            if (nfields < 0) nfields = fields.size();
            for (int i = 0; i < nfields; i++) {
                CompoundExplainer field = (CompoundExplainer)fields.get(i);
                if (field.getType() == Type.FIELD) {
                    Attributes kattr = field.get();
                    if (kattr.containsKey(Label.COLUMN_NAME)) {
                        sb.append(kattr.getValue(Label.COLUMN_NAME)).append(", ");
                        continue;
                    }
                }
                allcols = false;
                break;
            }
        }
        sb.setLength(allcols ? sb.length() - 2 : olen);
        return allcols;
    }

    protected void appendDistinctOperator(String name, Attributes atts) {
    }

    protected void appendUnionOperator(String name, Attributes atts) {
    }

    protected void appendRow(CompoundExplainer rEx) {
        Attributes atts = rEx.get();
        sb.append('[');
        if (atts.containsKey(Label.EXPRESSIONS)) {
            for (Explainer value : atts.get(Label.EXPRESSIONS)) {
                append(value);
                sb.append(", ");
            }
            sb.setLength(sb.length() - 2);
        }
        sb.append(']');
    }

    protected void appendRowType(CompoundExplainer rtEx) {
        Attributes atts = rtEx.get();
        if (atts.containsKey(Label.PARENT_TYPE) && 
            atts.containsKey((Label.CHILD_TYPE))) {
            append(atts.getAttribute(Label.PARENT_TYPE));
            sb.append(" - ");
            append(atts.getAttribute(Label.CHILD_TYPE));
        }
        else if (atts.containsKey(Label.LEFT_TYPE) && 
                 atts.containsKey((Label.RIGHT_TYPE))) {
            append(atts.getAttribute(Label.LEFT_TYPE));
            sb.append(" x ");
            append(atts.getAttribute(Label.RIGHT_TYPE));
        }
        else if (atts.containsKey(Label.INDEX_NAME)) {
            sb.append("Index(");
            appendTableName(atts);
            sb.append('.').append(atts.getValue(Label.INDEX_NAME)).append(')');
        }
        else if (atts.containsKey(Label.TABLE_NAME)) {
            appendTableName(atts);
        }
        else {
            sb.append(atts.getValue(Label.NAME));
        }
    }

    protected void appendTableName(Attributes atts) {
        if (atts.containsKey(Label.TABLE_SCHEMA)) {
            String name = atts.getValue(Label.TABLE_SCHEMA).toString();
            if (!name.equals(defaultSchemaName))
                sb.append(name).append('.');
        }
        sb.append(atts.getValue(Label.TABLE_NAME));
    }

    protected void newRow() {
        rows.add(sb.toString());
        sb.delete(0, sb.length());
    }
}
