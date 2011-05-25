package com.akiban.instrumentation;

import com.akiban.util.Tap;

public class EventImpl implements Event {

    
    public EventImpl(String name, int sessionId) {
        this.name = name;
        this.sessionId = sessionId;
        this.tapName = this.name + ":" + this.sessionId;
        this.enabled = false;
        this.eventTap = Tap.add(new Tap.PerThread(this.tapName, Tap.TimeAndCount.class));
    }
    
    // Event interface

    @Override
    public void start() {
        eventTap.in();
    }

    @Override
    public void stop() {
        eventTap.out();
    }

    @Override
    public void reset() {
        eventTap.reset();
    }

    @Override
    public long getLastDuration() {
        return eventTap.getDuration();
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    public void enable() { 
        reset();
        Tap.setEnabled(tapName, true);
        enabled = true;
    }

    @Override
    public void disable() {
        Tap.setEnabled(tapName, false);
        enabled = false;
    }

    @Override
    public boolean isEnabled() {
        return enabled;
    }
    
    // state

    private String name;
    private int sessionId;
    private String tapName;
    private boolean enabled;
    private final Tap eventTap;
    
}
