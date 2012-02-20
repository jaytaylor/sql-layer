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

package com.akiban.sql.optimizer.explain;

import java.util.*;

public class Attributes extends EnumMap<Label, Set<Explainer>>
{       
    public Attributes()
    {
        super(Label.class);
    }            
    
    public boolean put (Label label, Explainer ex)
    {
        Set<Explainer> l = get(label);
        if (l == null)
        {
            l = new HashSet<Explainer>();
            put(label, l);
        }
        l.add(ex);
        return true;
    }
        
    public List<Entry<Label, Explainer>> valuePairs()
    {
        List<Entry<Label, Explainer>> pairs = new ArrayList<Entry<Label,Explainer>>();
        
        for (Entry<Label, Set<Explainer>> entry : entrySet())                
            for (Explainer ex : entry.getValue())            
                pairs.add(new ValuePair(entry.getKey(), ex, this));                            
        return pairs;
    }
    
    private static class ValuePair implements Entry<Label, Explainer>
    {
        private Label key;
        private Explainer value;
        private Attributes map;
        
        protected ValuePair (Label k, Explainer v, Attributes m)
        {
            key = k;
            value = v;
            map = m;
        }
        
        @Override
        public Label getKey()
        {
            return key;
        }

        @Override
        public Explainer getValue()
        {
            return value;
        }

        @Override
        public Explainer setValue(Explainer value)
        {
            Explainer old = this.value;            
            Set<Explainer> s = map.get(key);
            this.value = value;
            s.remove(old);
            s.add(value);      
            return old;
        }
        
    }
}
