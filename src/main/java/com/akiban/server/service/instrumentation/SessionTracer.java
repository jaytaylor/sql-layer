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

import java.util.Date;

public interface SessionTracer {
    
    public void beginEvent(String eventName);
    
    public void endEvent();
    
    public Event getEvent(String eventName);
    
    public Object[] getCurrentEvents();
    
    public Object[] getCompletedEvents();
    
    public void setTraceLevel(int level);
    
    public int getTraceLevel();
    
    public void enable();
    
    public void disable();
    
    public boolean isEnabled();
    
    public String getCurrentStatement();
    
    public String getRemoteAddress();
    
    public Date getStartTime();
        
    public long getProcessingTime();
    
    public long getEventTime(String eventName);
    
    public long getTotalEventTime(String eventName);

}
