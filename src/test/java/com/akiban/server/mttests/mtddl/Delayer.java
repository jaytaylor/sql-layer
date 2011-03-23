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

package com.akiban.server.mttests.mtddl;

import com.akiban.server.mttests.mtutil.TimePoints;
import com.akiban.server.mttests.mtutil.Timing;
import com.akiban.util.ArgumentValidation;

class Delayer {
    private final long[] delays;
    private final String[] messagesBefore;
    private final String[] messagesAfter;
    private final TimePoints timePoints;
    private int count;

    Delayer(TimePoints timePoints, long... delays) {
        this.delays = new long[delays.length];
        this.messagesBefore = timePoints == null ? null : new String[delays.length];
        this.messagesAfter = timePoints == null ? null : new String[delays.length];
        System.arraycopy(delays, 0, this.delays, 0, delays.length);
        this.timePoints = timePoints;
    }

    public void delay() {
        if (count >= delays.length) {
            ++count; // not useful, just for record keeping (in case we look at this field in a debugger)
            return;
        }
        long delay = count >= delays.length ? -1 : delays[count];
        mark(messagesBefore);
        Timing.sleep(delay);
        mark(messagesAfter);
        ++count;
    }

    private void mark(String[] messages) {
        if (timePoints != null) {
            String message = messages[count];
            if (message != null) {
                timePoints.mark(message);
            }
        }
    }

    public Delayer markBefore(int index, String text) {
        defineMessage(index, text, messagesBefore);
        return this;
    }

    public Delayer markAfter(int index, String text) {
        defineMessage(index, text, messagesAfter);
        return this;
    }

    private void defineMessage(int index, String text, String[] messages) {
        ArgumentValidation.isGTE("index", index, 0);
        ArgumentValidation.isLT("index", index, delays.length);
        ArgumentValidation.notNull("timepoints", messages);
        messages[index] = text;
    }
}
