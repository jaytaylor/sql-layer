
package com.akiban.server.service.servicemanager;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

abstract class DummyInterfaces {

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

    private static final List<String> messages = new ArrayList<>();

    // nested classes

    static interface DummyService {
        void start();
        void stop();
    }

    public static interface Alpha extends DummyService {}
    public static interface Beta extends DummyService {}
    public static interface Gamma extends DummyService {}
}
