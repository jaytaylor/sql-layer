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
    /** This role allows access to all data. */
    public static final String ADMIN_ROLE = "admin";

    /** The current {@link Principal} for the {@link Session}. */
    public static final Session.Key<Principal> SESSION_PRINCIPAL_KEY = 
        Session.Key.named("SECURITY_PRINCIPAL");

    /** The current roles for the {@link Session}. */
    public static final Session.Key<Collection<String>> SESSION_ROLES_KEY = 
        Session.Key.named("SECURITY_ROLES");

    /** Authenticate user using local security database and set into {@link Session}.
     * @return the logged in {@link Principal}.
     * Throws an error if authentication fails.
     */
    public Principal authenticateLocal(Session session, String name, String password);

    /** Authenticate user using local security database and set in {@link Session}.
     * @param salt a salt to use when hashing the password
     */
    public Principal authenticateLocal(Session session, String name, String password,
                                       byte[] salt);

    /** If this {@link Session} is authenticated, does it have access to the given schema?
     *
     * NOTE: If authentication is enabled, caller must not call this (that is, allow
     * any queries) without authentication, since that is indistinguishable from
     * authentication disabled.
     *
     * @see com.foundationdb.sql.pg.PostgresServerConnection#authenticationOkay
     */
    public boolean isAccessible(Session session, String schema);
    
    /** Does the given {@link Principal} have access to the given scheam?
     * NOTE: If authentication is enabled, caller must not call this (that is, allow
     * any queries) with <code>null</code>, since that is indistinguishable from
     * authentication disabled.
     *
     * @see com.foundationdb.http.HttpConductorImpl.AuthenticationType
     */
    public boolean isAccessible(Principal user, boolean inAdminRole, String schema);

    /** If this {@link Session} is authenticated, does it administrative access?
     */
    public boolean hasRestrictedAccess(Session session);

    /** Set {@link Session}'s authentication directly. */
    public void setAuthenticated(Session session, Principal user, boolean inAdminRole);

    public void addRole(String name);
    public void deleteRole(String name);
    public User getUser(String name);
    public User addUser(String name, String password, Collection<String> roles);
    public void deleteUser(String name);
    public void changeUserPassword(String name, String password);
    public void clearAll(Session session);
}
