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

package com.akiban.server.service.session;

import com.akiban.server.service.ServiceManager;

public final class TestSessionFactory implements SessionFactory {

    private static final TestSessionFactory INSTANCE = new TestSessionFactory();

    public static SessionFactory get() {
        return INSTANCE;
    }

    private TestSessionFactory() {
        // private ctor
    }

    @Override
    public Session createSession() {
        return new Session(null);
    }
}
