/**
 * Copyright (C) 2011 Akiban Technologies Inc.
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License, version 3,
 * as published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see http://www.gnu.org/licenses.
 */


package com.akiban.sql.optimizer.explain.std;

import com.akiban.sql.optimizer.explain.Attributes;
import com.akiban.sql.optimizer.explain.Explainer;
import com.akiban.sql.optimizer.explain.Formatter;
import com.akiban.sql.optimizer.explain.Label;
import com.akiban.sql.optimizer.explain.OperationExplainer;
import com.akiban.sql.optimizer.explain.PrimitiveExplainer;
import java.util.Map.Entry;

/**
 * 
 * implements a simple Format that would print all the Explainer in an
 * indenting-fashion. 
 * 
 */
public class TreeFormat // used to implement Format.java, now is outdated.
{

    public String describe(Explainer explainer)
    {
        StringBuilder bd = new StringBuilder();
        doFormat(explainer, bd.append("\n"), 0);
        
        return bd.append("\n").toString();
    }
    
    private static void doFormat (Explainer ex, StringBuilder bd, int level)
    {
        if (ex.hasAttributes())
        {
             OperationExplainer opEx = (OperationExplainer)ex;
             Attributes atts = (Attributes) opEx.get().clone();
             
             // print name
             bd.append(atts.get(Label.NAME).get(0).get()); 
             atts.remove(Label.NAME);
             ++level;
             for (Entry<Label, Explainer> entry : atts.valuePairs())
             {
                 bd.append("\n");
                 for (int i = 0; i < level; ++i)
                     bd.append("--");
                 bd.append(entry.getKey()).append(": ");
                 doFormat(entry.getValue(), bd, level +1);
             }
        }
        else
            bd.append(((PrimitiveExplainer)ex).get());
    }
    
}
