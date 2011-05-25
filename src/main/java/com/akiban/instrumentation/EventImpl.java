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
    
    public EventImpl(String name, int sessionId, boolean enabled) {
        this.name = name;
        this.sessionId = sessionId;
        this.tapName = this.name + ":" + this.sessionId;
        this.eventTap = Tap.add(new Tap.PerThread(this.tapName, Tap.TimeAndCount.class));
        if (enabled) {
            enable();
        }
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
