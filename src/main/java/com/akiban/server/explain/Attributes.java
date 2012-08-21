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

package com.akiban.server.explain;

import java.util.*;

public class Attributes extends EnumMap<Label, List<Explainer>>
{       
    public Attributes()
    {
        super(Label.class);
    }            
    
    public boolean put (Label label, Explainer ex)
    {
        List<Explainer> l = get(label);
        if (l == null)
        {
            l = new ArrayList<Explainer>();
            put(label, l);
        }
        l.add(ex);
        return true;
    }
        
    public List<Entry<Label, Explainer>> valuePairs()
    {
        List<Entry<Label, Explainer>> pairs = new ArrayList<Entry<Label,Explainer>>();
        
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
