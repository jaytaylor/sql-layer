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
