/**
 * Copyright (C) 2009-2013 FoundationDB, LLC
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

package com.foundationdb.server.service.security;

import com.foundationdb.server.service.session.Session;

public class DummySecurityService implements SecurityService {
    @Override
    public User authenticate(Session session, String name, String password) {
        return null;
    }

    @Override
    public User authenticate(Session session, String name, String password, byte[] salt) {
        return null;
    }

    @Override
    public boolean isAccessible(Session session, String schema) {
        return true;
    }

    @Override
    public boolean isAccessible(java.security.Principal user, boolean inAdminRole, String schema) {
        return true;
    }

    @Override
    public boolean hasRestrictedAccess(Session session) {
        return true;
    }

    @Override
    public void addRole(String name) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void deleteRole(String name) {
        throw new UnsupportedOperationException();
    }

    @Override
    public User getUser(String name) {
        return null;
    }

    @Override
    public User addUser(String name, String password, java.util.Collection<String> roles) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void deleteUser(String name) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void changeUserPassword(String name, String password) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void clearAll(Session session) {
    }
}
