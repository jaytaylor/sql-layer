
package com.akiban.server.explain;

public abstract class Explainer 
{
    public abstract Type getType();
    
    /**
     * 
     * @return a map of this object's attributes if it's an OperationExplainer
     *         a primitive object (Integer, Double, etc ...), otherwise.
     */
    public abstract Object get();
    
    @Override
    public final boolean equals (Object o)
    {
        if (o != null && o instanceof Explainer)
        {
            Explainer other = (Explainer)o;
            return (getType().equals(other.getType()) &&
                    get().equals(other.get()));
        }
        else
            return false;
    }
    
    @Override
    public final int hashCode ()
    {
        return getType().hashCode() + get().hashCode();
    }
}
