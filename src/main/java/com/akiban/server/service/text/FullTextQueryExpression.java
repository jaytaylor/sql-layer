
package com.akiban.server.service.text;

import com.akiban.qp.operator.QueryContext;
import com.akiban.server.explain.Explainable;

import org.apache.lucene.search.Query;

/** Return <code>Query</code>, which might be fixed from compile time,
 * parsed from a string, or built up from expressions.
 */
public interface FullTextQueryExpression extends Explainable {
    public Query getQuery(QueryContext context);
}
