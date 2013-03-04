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

import com.akiban.ais.model.FullTextIndex;
import com.akiban.ais.model.IndexColumn;
import com.akiban.ais.model.IndexName;
import com.akiban.qp.operator.QueryContext;
import com.akiban.qp.rowtype.RowType;
import com.akiban.server.explain.*;
import com.akiban.server.types3.texpressions.TEvaluatableExpression;
import com.akiban.server.types3.texpressions.TPreparedExpression;

import org.apache.lucene.index.Term;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;

import java.util.List;

public class FullTextQueryBuilder
{
    protected final IndexName indexName;
    protected final FullTextIndexService service;
    protected final QueryContext buildContext;

    /** Construct directly for testing. */
    public FullTextQueryBuilder(IndexName indexName, FullTextIndexService service) {
        this.indexName = indexName;
        this.service = service;
        this.buildContext = null;
    }

    public FullTextQueryBuilder(FullTextIndex index, QueryContext buildContext) {
        this.indexName = index.getIndexName();
        this.service = buildContext.getServiceManager().getServiceByClass(FullTextIndexService.class);
        this.buildContext = buildContext;
    }

    static class Constant implements FullTextQueryExpression {
        private final Query query;

        public Constant(Query query) {
            this.query = query;
        }

        @Override
        public Query getQuery(QueryContext context) {
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
    }
    
    public FullTextQueryExpression staticQuery(Query query) {
        return new Constant(query);
    }

    /** A string in Lucene query syntax. */
    public FullTextQueryExpression parseQuery(final String query) {
        return parseQuery(null, query);
    }

    public FullTextQueryExpression parseQuery(IndexColumn defaultField,
                                              final String query) {
        final String fieldName = (defaultField == null) ? null : defaultField.getColumn().getName();
        if (buildContext != null) {
            return new Constant(service.parseQuery(buildContext, indexName, fieldName, query));
        }
        return new FullTextQueryExpression() {
                @Override
                public Query getQuery(QueryContext context) {
                    return service.parseQuery(context, indexName, fieldName, query);
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
    
    public FullTextQueryExpression parseQuery(IndexColumn defaultField,
                                              final TPreparedExpression qexpr) {
        final String fieldName = (defaultField == null) ? null : defaultField.getColumn().getName();
        return new FullTextQueryExpression() {
                @Override
                public Query getQuery(QueryContext context) {
                    TEvaluatableExpression qeval = qexpr.build();
                    qeval.with(context);
                    qeval.evaluate();
                    if (qeval.resultValue().isNull())
                        return null;
                    String query = qeval.resultValue().getString();
                    return service.parseQuery(context, indexName, fieldName, query);
                }

                @Override
                public CompoundExplainer getExplainer(ExplainContext context) {
                    CompoundExplainer explainer = new CompoundExplainer(Type.FIELD);
                    explainer.addAttribute(Label.OPERAND, qexpr.getExplainer(context));
                    return explainer;
                }

                @Override
                public String toString() {
                    return qexpr.toString();
                }
            };
    }
    
    public FullTextQueryExpression matchQuery(IndexColumn field, String key) {
        String fieldName = field.getColumn().getName();
        return new Constant(new TermQuery(new Term(fieldName, key)));
    }

    public FullTextQueryExpression matchQuery(IndexColumn field,
                                              final TPreparedExpression qexpr) {
        final String fieldName = field.getColumn().getName();
        return new FullTextQueryExpression() {
                @Override
                public Query getQuery(QueryContext context) {
                    TEvaluatableExpression qeval = qexpr.build();
                    qeval.with(context);
                    qeval.evaluate();
                    if (qeval.resultValue().isNull())
                        return null;
                    String query = qeval.resultValue().getString();
                    return new TermQuery(new Term(fieldName, query));
                }

                @Override
                public CompoundExplainer getExplainer(ExplainContext context) {
                    CompoundExplainer explainer = new CompoundExplainer(Type.FIELD);
                    explainer.addAttribute(Label.OPERAND, qexpr.getExplainer(context));
                    return explainer;
                }

                @Override
                public String toString() {
                    return qexpr.toString();
                }
            };
    }
    
    public enum BooleanType { SHOULD, MUST, NOT };

    public FullTextQueryExpression booleanQuery(final List<FullTextQueryExpression> queries,
                                                final List<BooleanType> types) {
        boolean isConstant = true;
        for (FullTextQueryExpression query : queries) {
            if (!(query instanceof Constant)) {
                isConstant = false;
                break;
            }
        }
        FullTextQueryExpression result = 
            new FullTextQueryExpression() {
                @Override
                public Query getQuery(QueryContext context) {
                    BooleanQuery query = new BooleanQuery();
                    for (int i = 0; i < queries.size(); i++) {
                        BooleanClause.Occur occur;
                        switch (types.get(i)) {
                        case MUST:
                            occur = BooleanClause.Occur.MUST;
                            break;
                        case NOT:
                            occur = BooleanClause.Occur.MUST_NOT;
                            break;
                        case SHOULD:
                            occur = BooleanClause.Occur.SHOULD;
                            break;
                        default:
                            throw new IllegalArgumentException(types.get(i).toString());
                        }
                        query.add(queries.get(i).getQuery(context), occur);
                    }
                    return query;
                }

                @Override
                public CompoundExplainer getExplainer(ExplainContext context) {
                    CompoundExplainer explainer = new CompoundExplainer(Type.FIELD);
                    for (FullTextQueryExpression query : queries) {
                        explainer.addAttribute(Label.OPERAND, query.getExplainer(context));
                    }
                    return explainer;
                }

                @Override
                public String toString() {
                    return queries.toString();
                }
            };
        if (isConstant) {
            return new Constant(result.getQuery(buildContext));
        }
        else {
            return result;
        }
    }

    public IndexScan_FullText scanOperator(String query, int limit) {
        return scanOperator(parseQuery(query), limit);
    }
    
    public IndexScan_FullText scanOperator(FullTextQueryExpression query, int limit) {
        RowType rowType = null;
        if (buildContext != null)
            rowType = service.searchRowType(buildContext.getSession(), indexName);
        return new IndexScan_FullText(indexName, query, limit, rowType);
    }

}
