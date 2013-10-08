/**
 * Copyright (C) 2009-2013 FoundationDB, LLC
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

package com.foundationdb.server.service.text;

import com.foundationdb.ais.model.AkibanInformationSchema;
import com.foundationdb.ais.model.FullTextIndex;
import com.foundationdb.ais.model.IndexColumn;
import com.foundationdb.ais.model.IndexName;
import com.foundationdb.qp.operator.QueryBindings;
import com.foundationdb.qp.operator.QueryContext;
import com.foundationdb.qp.rowtype.RowType;
import com.foundationdb.server.collation.AkCollator;
import com.foundationdb.server.error.AkibanInternalException;
import com.foundationdb.server.explain.*;
import com.foundationdb.server.service.ServiceManager;
import com.foundationdb.server.service.session.Session;
import com.foundationdb.server.types.texpressions.TEvaluatableExpression;
import com.foundationdb.server.types.texpressions.TPreparedExpression;

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
        public boolean needsBindings() {
            return false;
        }

        @Override
        public Query getQuery(QueryContext context, QueryBindings bindings) {
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
                public boolean needsBindings() {
                    return false;
                }

                @Override
                public Query getQuery(QueryContext context, QueryBindings bindings) {
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
                public boolean needsBindings() {
                    return true;
                }

                @Override
                public Query getQuery(QueryContext context, QueryBindings bindings) {
                    TEvaluatableExpression qeval = qexpr.build();
                    qeval.with(context);
                    qeval.with(bindings);
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
                public boolean needsBindings() {
                    return true;
                }

                @Override
                public Query getQuery(QueryContext context, QueryBindings bindings) {
                    TEvaluatableExpression qeval = qexpr.build();
                    qeval.with(context);
                    qeval.with(bindings);
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
                public boolean needsBindings() {
                    for (FullTextQueryExpression query : queries) {
                        if (query.needsBindings()) {
                            return true;
                        }
                    }
                    return false;
                }

                @Override
                public Query getQuery(QueryContext context, QueryBindings bindings) {
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
                        query.add(queries.get(i).getQuery(context, bindings), occur);
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
            return new Constant(result.getQuery(buildContext, null));
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
