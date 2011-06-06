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

package com.akiban.server.service.servicemanager;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

public abstract class DummyInterfaces {

    // for use within this package

    static void addMessage(String message) {
        messages.add(message);
    }

    static List<String> messages() {
        return Collections.unmodifiableList(messages);
    }

    static void clearMessages() {
        messages.clear();
    }

    // class state

    private static final List<String> messages = new ArrayList<String>();

    // nested classes

    static interface DummyService {
        void start();
        void stop();
    }

    public static interface Alpha extends DummyService {}
    public static interface Beta extends DummyService {}
    public static interface Gamma extends DummyService {}
}
