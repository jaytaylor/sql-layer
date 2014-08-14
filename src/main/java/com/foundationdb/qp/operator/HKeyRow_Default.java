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

package com.foundationdb.qp.operator;

import com.foundationdb.ais.model.HKeyColumn;
import com.foundationdb.ais.model.HKeySegment;
import com.foundationdb.qp.storeadapter.PersistitHKey;
import com.foundationdb.qp.row.HKeyRow;
import com.foundationdb.qp.row.ProjectedRow;
import com.foundationdb.qp.row.Row;
import com.foundationdb.qp.rowtype.HKeyRowType;
import com.foundationdb.qp.rowtype.RowType;
import com.foundationdb.qp.util.HKeyCache;
import com.foundationdb.server.PersistitKeyValueTarget;
import com.foundationdb.server.explain.*;
import com.foundationdb.server.types.texpressions.TEvaluatableExpression;
import com.foundationdb.server.types.texpressions.TPreparedExpression;
import com.foundationdb.util.ArgumentValidation;
import com.foundationdb.util.tap.InOutTap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

/**
 <h1>Overview</h1>

 HKeyRow_Default builds an HKey entirely from evaluated expressions.

 <h1>Arguments</h1>

 <li><b>RowType rowType:</b> Type of HKey rows to be built. Must be non-null.
 <li><b>List<Expression> expressions:</b> Expressions computing key fields.

 <h1>Behavior</h1>

  An HKey is constructed by evaluating the given expressions (once).

 <h1>Output</h1>

  A single HKey row.

  <h1>Assumptions</h1>

  None.

  <h1>Performance</h1>

  HKeyRow_Default does no IO. It therefore compares favorably with an IndexScan of the PRIMARY index, when the complete key is known.

  <h1>Memory Requirements</h1>

    None.
 */


class HKeyRow_Default extends Operator
{
    // Object interface

    @Override
    public String toString() {
        return String.format("hkey for %s (%s)", 
                             rowType.hKey().table().getName(), 
                             expressions.toString());
    }

    // Operator interface

    @Override
    protected Cursor cursor(QueryContext context, QueryBindingsCursor bindingsCursor) {
        return new Execution(context, bindingsCursor);
    }

    // HKeyRow_Default interface

    public HKeyRow_Default(RowType rowType, List<? extends TPreparedExpression> expressions) {
        ArgumentValidation.notNull("rowType", rowType);
        ArgumentValidation.isTrue("invalid row type", rowType instanceof HKeyRowType);
        this.rowType = (HKeyRowType)rowType;

        ArgumentValidation.notEmpty("expressions", expressions);
        ArgumentValidation.isSame("expressions length", expressions.size(),
                                  "hkey field count", this.rowType.nFields());
        this.expressions = expressions;
    }

    // Class state
    
    private static final InOutTap TAP_OPEN = OPERATOR_TAP.createSubsidiaryTap("operator: HKeyRow_Default open");
    private static final InOutTap TAP_NEXT = OPERATOR_TAP.createSubsidiaryTap("operator: HKeyRow_Default next");
    private static final Logger LOG = LoggerFactory.getLogger(HKeyRow_Default.class);

    // Object state

    protected final HKeyRowType rowType;
    private final List<? extends TPreparedExpression> expressions;

    @Override
    public CompoundExplainer getExplainer(ExplainContext context) {
        Attributes att = new Attributes();
        
        att.put(Label.NAME, PrimitiveExplainer.getInstance(getName()));
        att.put(Label.OUTPUT_TYPE, rowType.getExplainer(context));
        for (TPreparedExpression ex : expressions)
            att.put(Label.PROJECTION, ex.getExplainer(context));
        if (context.hasExtraInfo(this))
            att.putAll(context.getExtraInfo(this).get());
        return new CompoundExplainer(Type.HKEY_OPERATOR, att);
    }

    // Inner classes

    private class Execution extends LeafCursor {
        // Cursor interface
        
        @Override
        public void open() {
            TAP_OPEN.in();
            try {
                CursorLifecycle.checkIdle(this);
                idle = false;
            } finally {
                TAP_OPEN.out();
            }
        }

        @Override
        public Row next() {
            if (TAP_NEXT_ENABLED) {
                TAP_NEXT.in();
            }
            try {
                if (CURSOR_LIFECYCLE_ENABLED) {
                    CursorLifecycle.checkIdleOrActive(this);
                }
                if (idle) {
                    return null;
                }
                checkQueryCancelation();
                Row row = buildHKeyRow();
                if (LOG_EXECUTION) {
                    LOG.debug("HKeyRow_Default: yield {}", row);
                }
                idle = true;
                return row;
            } finally {
                if (TAP_NEXT_ENABLED) {
                    TAP_NEXT.out();
                }
            }
        }

        @Override
        public void close() {
            CursorLifecycle.checkIdleOrActive(this);
            idle = true;
        }

        @Override
        public void destroy() {
            close();
            evalExprs = null;
        }

        @Override
        public boolean isIdle() {
            return !isDestroyed() && idle;
        }

        @Override
        public boolean isActive() {
            return !isDestroyed() && !idle;
        }

        @Override
        public boolean isDestroyed() {
            return (evalExprs ==  null);
        }

        // For use by this class

        private HKeyRow buildHKeyRow() {
            StoreAdapter store = adapter(rowType.hKey().table());
            PersistitHKey hkey = (PersistitHKey)store.newHKey(rowType.hKey());
            target.attach(hkey.key());
            int index = 0;
            for (HKeySegment segment : rowType.hKey().segments()) {
                hkey.extendWithOrdinal(segment.table().getOrdinal());
                for (HKeyColumn column : segment.columns()) {
                    TEvaluatableExpression evalExpr = evalExprs.get(index++);
                    evalExpr.with(context);
                    evalExpr.with(bindings);
                    evalExpr.evaluate();
                    column.column().getType().writeCollating(evalExpr.resultValue(), target);
                }
            }
            assert (index == rowType.nFields());
            return new HKeyRow(rowType, hkey, new HKeyCache<>(store));
        }

        // Execution interface

        Execution(QueryContext context, QueryBindingsCursor bindingsCursor) {
            super(context, bindingsCursor);
            evalExprs = ProjectedRow.createTEvaluatableExpressions(expressions);
        }

        // Object state
        private boolean idle = true;
        private List<TEvaluatableExpression> evalExprs = null;
        private final PersistitKeyValueTarget target = new PersistitKeyValueTarget(rowType);
    }
}
