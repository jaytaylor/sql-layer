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

package com.foundationdb.server.explain;

import java.util.*;

public class Attributes extends EnumMap<Label, List<Explainer>>
{       
    public Attributes()
    {
        super(Label.class);
    }            
    
    public Explainer getAttribute(Label label) {
        List<Explainer> l = get(label);
        if (l == null)
            return null;
        assert (l.size() == 1) : l;
        return l.get(0);
    }

    public Object getValue(Label label) {
        Explainer explainer = getAttribute(label);
        if (explainer == null)
            return null;
        return explainer.get();
    }

    public boolean put(Label label, Explainer ex)
    {
        List<Explainer> l = get(label);
        if (l == null)
        {
            l = new ArrayList<>();
            put(label, l);
        }
        l.add(ex);
        return true;
    }
        
    public List<Entry<Label, Explainer>> valuePairs()
    {
        List<Entry<Label, Explainer>> pairs = new ArrayList<>();
        
        for (Entry<Label, List<Explainer>> entry : entrySet())                
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
            List<Explainer> s = map.get(key);
            this.value = value;
            s.remove(old);
            s.add(value);      
            return old;
        }
        
    }
}
