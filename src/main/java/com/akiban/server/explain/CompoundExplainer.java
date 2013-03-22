
package com.akiban.server.explain;

public class CompoundExplainer extends Explainer 
{
    private final Type type; 
    private Attributes states;
        
    public CompoundExplainer(Type type)
    {
        this(type, new Attributes());
    }

    public CompoundExplainer(Type type, Attributes states)
    {
        this.type = type;
        this.states = states;
    }   
    
    @Override
    public Type getType()
    {
        return type;
    }

    @Override
    public Attributes get()
    {
        return states;
    }
    
    public final boolean addAttribute(Label label, Explainer ex)
    {
        if (states.containsKey(label)) return false;
        states.put(label, ex);
        return true;
    }    
}
