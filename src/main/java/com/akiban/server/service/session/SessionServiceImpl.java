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

import com.akiban.server.service.Service;

public final class SessionServiceImpl implements SessionService, Service<SessionService> {

    @Override
    public Session createSession() {
        return new Session();
    }

    @Override
    public SessionService cast() {
        return this;
    }

    @Override
    public Class<SessionService> castClass() {
        return SessionService.class;
    }

    @Override
    public void start() throws Exception {
    }

    @Override
    public void stop() throws Exception {
    }

    @Override
    public void crash() throws Exception {
    }
}
