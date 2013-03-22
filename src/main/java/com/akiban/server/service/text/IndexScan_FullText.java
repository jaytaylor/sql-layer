
package com.akiban.server.service.text;

import com.akiban.ais.model.IndexName;
import com.akiban.qp.operator.Cursor;
import com.akiban.qp.operator.Operator;
import com.akiban.qp.operator.QueryContext;
import com.akiban.qp.rowtype.RowType;
import com.akiban.server.explain.*;

import org.apache.lucene.search.Query;

public class IndexScan_FullText extends Operator
{
    private final IndexName index;
    private final FullTextQueryExpression queryExpression;
    private final int limit;
    private final RowType rowType;

    public IndexScan_FullText(IndexName index, 
                              FullTextQueryExpression queryExpression, 
                              int limit,
                              RowType rowType) {
        this.index = index;
        this.queryExpression = queryExpression;
        this.limit = limit;
        this.rowType = rowType;
    }

    @Override
    public RowType rowType() {
        if (rowType != null)
            return rowType;
        else
            return super.rowType(); // Only when testing and not needed.
    }

    @Override
    protected Cursor cursor(QueryContext context) {
        Query query = queryExpression.getQuery(context);
        FullTextIndexService service = context.getServiceManager().getServiceByClass(FullTextIndexService.class);
        return service.searchIndex(context, index, query, limit);
    }
    
    @Override
    public CompoundExplainer getExplainer(ExplainContext context)
    {
        Attributes atts = new Attributes();
        atts.put(Label.NAME, PrimitiveExplainer.getInstance(getName()));
        atts.put(Label.INDEX, PrimitiveExplainer.getInstance(index.toString()));
        atts.put(Label.INDEX_KIND, PrimitiveExplainer.getInstance("FULL_TEXT"));
        atts.put(Label.PREDICATE, queryExpression.getExplainer(context));
        if (limit > 0)
            atts.put(Label.LIMIT, PrimitiveExplainer.getInstance(limit));
        if (context.hasExtraInfo(this))
            atts.putAll(context.getExtraInfo(this).get()); 
        return new CompoundExplainer(Type.SCAN_OPERATOR, atts);
    }

    @Override
    public String toString() {
        StringBuilder str = new StringBuilder(getName());
        str.append("(").append(index);
        str.append(" ").append(queryExpression);
        if (limit > 0) {
            str.append(" LIMIT ").append(limit);
        }
        str.append(")");
        return str.toString();
    }

}
