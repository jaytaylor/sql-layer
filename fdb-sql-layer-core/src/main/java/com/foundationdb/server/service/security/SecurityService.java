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

import java.security.Principal;
import java.util.Collection;

public interface SecurityService
{
    public static final String ADMIN_ROLE = "admin";

    public static final Session.Key<User> SESSION_KEY = 
        Session.Key.named("SECURITY_USER");

    public User authenticate(Session session, String name, String password);
    public User authenticate(Session session, String name, String password, byte[] salt);

    public boolean isAccessible(Session session, String schema);
    public boolean isAccessible(Principal user, boolean inAdminRole, String schema);
    public boolean hasRestrictedAccess(Session session);

    public void addRole(String name);
    public void deleteRole(String name);
    public User getUser(String name);
    public User addUser(String name, String password, Collection<String> roles);
    public void deleteUser(String name);
    public void changeUserPassword(String name, String password);
    public void clearAll(Session session);
}
