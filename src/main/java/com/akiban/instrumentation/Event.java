package com.akiban.instrumentation;

public interface Event {
    
    public void start();
    
    public void stop();
    
    public void reset();
    
    public long getLastDuration();
    
    public String getName();
    
    public void enable();
    
    public void disable();
    
    public boolean isEnabled();

}
