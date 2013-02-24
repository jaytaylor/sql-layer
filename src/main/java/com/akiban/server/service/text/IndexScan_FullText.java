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

package com.akiban.server.service.text;

import com.akiban.qp.operator.Cursor;
import com.akiban.qp.operator.Operator;
import com.akiban.qp.operator.QueryContext;
import com.akiban.server.explain.*;

import org.apache.lucene.search.Query;

public class IndexScan_FullText extends Operator
{
    private final FullTextIndexService service;
    private final String index;
    private final QueryGenerator queryGenerator;
    private final int limit;

    /** Return <code>Query</code>, which might be fixed from compile
     * time, parsed from a string, or built up from expressions.
     */
    public interface QueryGenerator extends Explainable {
        public Query getQuery(FullTextIndexService service, QueryContext context, String indexName);
    }

    public static QueryGenerator staticQuery(final Query query) {
        return new QueryGenerator() {
                @Override
                public Query getQuery(FullTextIndexService service, QueryContext context, String indexName) {
                    return query;
                }

                @Override
                public CompoundExplainer getExplainer(ExplainContext context) {
                    CompoundExplainer explainer = new CompoundExplainer(Type.LITERAL);
                    explainer.addAttribute(Label.OPERAND, PrimitiveExplainer.getInstance(query.toString()));
                    return explainer;
                }

                @Override
                public String toString() {
                    return query.toString();
                }
            };
    }

    public static QueryGenerator parseQuery(final String query) {
        return new QueryGenerator() {
                @Override
                public Query getQuery(FullTextIndexService service, QueryContext context, String indexName) {
                    return service.parseQuery(context, indexName, query);
                }

                @Override
                public CompoundExplainer getExplainer(ExplainContext context) {
                    CompoundExplainer explainer = new CompoundExplainer(Type.LITERAL);
                    explainer.addAttribute(Label.OPERAND, PrimitiveExplainer.getInstance(query));
                    return explainer;
                }

                @Override
                public String toString() {
                    return query;
                }
            };
    }

    public IndexScan_FullText(FullTextIndexService service, String index, 
                              QueryGenerator queryGenerator, int limit) {
        this.service = service;
        this.index = index;
        this.queryGenerator = queryGenerator;
        this.limit = limit;
    }

    @Override
    protected Cursor cursor(QueryContext context) {
        Query query = queryGenerator.getQuery(service, context, index);
        return service.searchIndex(context, index, query, limit);
    }

    @Override
    public CompoundExplainer getExplainer(ExplainContext context)
    {
        Attributes atts = new Attributes();
        atts.put(Label.NAME, PrimitiveExplainer.getInstance(getName()));
        atts.put(Label.INDEX, PrimitiveExplainer.getInstance(index));
        atts.put(Label.INDEX_KIND, PrimitiveExplainer.getInstance("FULL_TEXT"));
        atts.put(Label.PREDICATE, queryGenerator.getExplainer(context));
        atts.put(Label.LIMIT, PrimitiveExplainer.getInstance(limit));
        if (context.hasExtraInfo(this))
            atts.putAll(context.getExtraInfo(this).get()); 
        return new CompoundExplainer(Type.SCAN_OPERATOR, atts);
    }

    @Override
    public String toString() {
        StringBuilder str = new StringBuilder(getName());
        str.append("(").append(index);
        str.append(" ").append(queryGenerator);
        str.append(" LIMIT ");
        str.append(limit);
        str.append(")");
        return str.toString();
    }

}
