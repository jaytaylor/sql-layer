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

package com.akiban.server.service.instrumentation;

import com.akiban.util.tap.InOutTap;
import com.akiban.util.tap.Tap;

public class EventImpl implements Event {
    
    public EventImpl(String name, int sessionId, boolean enabled) {
        this.name = name;
        this.sessionId = sessionId;
        this.tapName = this.name + ":" + this.sessionId;
        this.eventTap = Tap.createTimer(this.tapName);
        if (enabled) {
            enable();
        }
        this.lastDuration = 0;
        this.totalTime = 0;
    }
    
    // Event interface

    @Override
    public void start() {
        eventTap.in();
    }

    @Override
    public void stop() {
        eventTap.out();
        lastDuration = eventTap.getDuration();
        totalTime += lastDuration;
    }

    @Override
    public void reset() {
        eventTap.reset();
        lastDuration = 0;
        totalTime = 0;
    }

    @Override
    public long getLastDuration() {
        return lastDuration;
    }
    
    @Override
    public long getTotalTime() {
        return totalTime;
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
    private final InOutTap eventTap;
    private long lastDuration;
    private long totalTime;
    
}
