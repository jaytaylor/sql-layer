package com.akiban.instrumentation;

import java.util.Date;

public interface SessionTracer {
    
    public void beginEvent(String eventName);
    
    public void endEvent();
    
    public Event getEvent(String eventName);
    
    public Object[] getCurrentEvents();
    
    public void setTraceLevel(int level);
    
    public int getTraceLevel();
    
    public void enable();
    
    public void disable();
    
    public boolean isEnabled();
    
    public String getCurrentStatement();
    
    public String getRemoteAddress();
    
    public Date getStartTime();
        
    public long getProcessingTime();

}
