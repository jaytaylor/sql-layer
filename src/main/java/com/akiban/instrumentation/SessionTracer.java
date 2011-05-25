package com.akiban.instrumentation;

public interface SessionTracer {
    
    public void beginEvent(String eventName);
    
    public void endEvent(String eventName);
    
    public Event getEvent(String eventName);
    
    public Object[] getCurrentEvents();
    
    public void setTraceLevel(int level);
    
    public int getTraceLevel();
    
    public void enable();
    
    public void disable();
    
    public boolean isEnabled();

}
