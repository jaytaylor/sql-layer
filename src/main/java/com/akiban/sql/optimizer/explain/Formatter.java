/**
 * Copyright (C) 2011 Akiban Technologies Inc. This program is free software:
 * you can redistribute it and/or modify it under the terms of the GNU Affero
 * General Public License, version 3, as published by the Free Software
 * Foundation.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS
 * FOR A PARTICULAR PURPOSE. See the GNU Affero General Public License for more
 * details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see http://www.gnu.org/licenses.
 */
package com.akiban.sql.optimizer.explain;

import com.akiban.sql.optimizer.explain.std.ExpressionExplainer;
import java.util.Map;

public class Formatter {

    String describe(Explainer explainer) {
        StringBuilder sb = new StringBuilder("");
        describe(explainer, sb);
        return sb.toString();
    }

    void describe(Explainer explainer, StringBuilder sb) {
        if (explainer.hasAttributes())
        {
            OperationExplainer opEx = (OperationExplainer) explainer;
            describeOperation(opEx, sb);
        }
        else
        {
            PrimitiveExplainer primEx = (PrimitiveExplainer) explainer;
            describePrimitive(primEx, sb);
        }
    }

    void describeExpression(ExpressionExplainer explainer, StringBuilder sb) {
        // TODO
    }

    void describeOperation(OperationExplainer explainer, StringBuilder sb) {
        
        Attributes atts = (Attributes) explainer.get().clone();
        
        if (explainer.get().containsKey(Label.INFIX))
        {
            sb.append("(");
            describe(atts.valuePairs().get(0).getValue(), sb);
            sb.append(" ");
            sb.append(atts.get(Label.NAME).get(0).get());
            sb.append(" ");
            describe(atts.valuePairs().get(1).getValue(), sb);
            sb.append(")");
        }
        else
        {
            sb.append(atts.get(Label.NAME).get(0).get());
            atts.remove(Label.NAME);
            sb.append("(");
            for (Map.Entry<Label, Explainer> entry : atts.valuePairs())
            {
                describe(entry.getValue(), sb);
                sb.append(", ");
            }
            sb.setLength(sb.length()-2);
            sb.append(")");
        }
    }

    void describePrimitive(PrimitiveExplainer explainer, StringBuilder sb) {
        if (explainer.getType()==Type.STRING)
        {
            sb.append("\"");
            sb.append(explainer.get());
            sb.append("\"");
        }
        else
        {
            sb.append(explainer.get());
        }
    }
}
