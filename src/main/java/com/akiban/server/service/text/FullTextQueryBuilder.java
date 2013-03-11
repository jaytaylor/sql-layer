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

import com.akiban.ais.model.AkibanInformationSchema;
import com.akiban.ais.model.FullTextIndex;
import com.akiban.ais.model.IndexColumn;
import com.akiban.ais.model.IndexName;
import com.akiban.qp.operator.QueryContext;
import com.akiban.qp.rowtype.RowType;
import com.akiban.qp.rowtype.Schema;
import com.akiban.server.collation.AkCollator;
import com.akiban.server.error.AkibanInternalException;
import com.akiban.server.explain.*;
import com.akiban.server.service.ServiceManager;
import com.akiban.server.service.session.Session;
import com.akiban.server.types3.texpressions.TEvaluatableExpression;
import com.akiban.server.types3.texpressions.TPreparedExpression;

import org.apache.lucene.index.Term;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;

import java.io.File;
import java.util.List;

public class FullTextQueryBuilder
{
    protected final IndexName indexName;
    protected final FullTextIndexInfos infos;
    protected final QueryContext buildContext;

    /** Construct directly for non-SQL and testing. */
    public FullTextQueryBuilder(IndexName indexName, FullTextIndexService service) {
        this.indexName = indexName;
        this.infos = service;
        this.buildContext = null;
    }

    /** Construct for given index and context */
    public FullTextQueryBuilder(FullTextIndex index, AkibanInformationSchema ais,
                                QueryContext buildContext) {
        this.indexName = index.getIndexName();
        ServiceManager serviceManager = null;
        if (buildContext != null) {
            try {
                serviceManager = buildContext.getServiceManager();
            }
            catch (UnsupportedOperationException ex) {
            }
        }
        if (serviceManager != null) {
            this.infos = serviceManager.getServiceByClass(FullTextIndexService.class);
        }
        else {
            this.infos = new TestFullTextIndexInfos(ais);
        }
        this.buildContext = buildContext;
    }

    /** For testing without services running (or even stored AIS). */
    static class TestFullTextIndexInfos extends FullTextIndexInfosImpl {
        private final AkibanInformationSchema ais;
        private final File dummyPath = new File("."); // Does not matter.

        public TestFullTextIndexInfos(AkibanInformationSchema ais) {
            this.ais = ais;
        }

        @Override
        protected AkibanInformationSchema getAIS(Session session) {
            return ais;
        }

        @Override
        protected File getIndexPath() {
            return dummyPath; 
        }
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
            return new Constant(infos.parseQuery(buildContext, indexName, fieldName, query));
        }
        return new FullTextQueryExpression() {
                @Override
                public Query getQuery(QueryContext context) {
                    return infos.parseQuery(context, indexName, fieldName, query);
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
                    return infos.parseQuery(context, indexName, fieldName, query);
                }

                @Override
                public CompoundExplainer getExplainer(ExplainContext context) {
                    CompoundExplainer explainer = new CompoundExplainer(Type.FUNCTION);
                    explainer.addAttribute(Label.NAME, PrimitiveExplainer.getInstance("PARSE"));
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
        String fieldName = checkFieldForMatch(field);
        return new Constant(new TermQuery(new Term(fieldName, key)));
    }

    public FullTextQueryExpression matchQuery(IndexColumn field,
                                              final TPreparedExpression qexpr) {
        final String fieldName = checkFieldForMatch(field);
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
                    CompoundExplainer explainer = new CompoundExplainer(Type.FUNCTION);
                    explainer.addAttribute(Label.NAME, PrimitiveExplainer.getInstance("TERM"));
                    explainer.addAttribute(Label.OPERAND, qexpr.getExplainer(context));
                    return explainer;
                }

                @Override
                public String toString() {
                    return qexpr.toString();
                }
            };
    }
    
    protected String checkFieldForMatch(IndexColumn field) {
        String fieldName = field.getColumn().getName();
        AkCollator collator = field.getColumn().getCollator();
        if ((collator != null) && !collator.isCaseSensitive()) {
            throw new AkibanInternalException("Building a term for field that may need analysis: " + fieldName);
        }
        return fieldName;
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
                    CompoundExplainer explainer = new CompoundExplainer(Type.FUNCTION);
                    explainer.addAttribute(Label.NAME, PrimitiveExplainer.getInstance("AND"));
                    for (FullTextQueryExpression query : queries) {
                        explainer.get().put(Label.OPERAND, query.getExplainer(context));
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
            rowType = infos.searchRowType(buildContext.getSession(), indexName);
        return new IndexScan_FullText(indexName, query, limit, rowType);
    }

}
