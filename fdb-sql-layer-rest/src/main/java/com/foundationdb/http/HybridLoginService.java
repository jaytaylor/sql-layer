/**
 * Copyright (C) 2009-2015 FoundationDB, LLC
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

package com.foundationdb.http;

import com.foundationdb.server.service.security.SecurityService;
import com.foundationdb.server.service.security.User;
import org.eclipse.jetty.security.IdentityService;
import org.eclipse.jetty.security.LoginService;
import org.eclipse.jetty.server.UserIdentity;
import java.security.Principal;
import javax.security.auth.Subject;

public class HybridLoginService implements LoginService
{
    private final LoginService delegate;
    private final SecurityService securityService;

    public HybridLoginService(LoginService delegate, SecurityService securityService) {
        this.delegate = delegate;
        this.securityService = securityService;
    }

    @Override
    public String getName() {
        return delegate.getName();
    }

    @Override
    public UserIdentity login(String username, Object credentials) {
        UserIdentity inner = delegate.login(username, credentials);
        if (inner == null)
            return null;
        String userName = inner.getUserPrincipal().getName();
        int at = userName.indexOf('@');
        if (at >= 0) userName = userName.substring(0, at);
        User user = securityService.getUser(userName);
        if (user == null)
            return inner;
        else
            return new WrappedUserIdentity(inner, user);
    }

    @Override
    public boolean validate(UserIdentity user) {
        return delegate.validate(unwrap(user));
    }

    @Override
    public IdentityService getIdentityService() {
        return delegate.getIdentityService();
    }

    @Override
    public void setIdentityService(IdentityService service) {
        delegate.setIdentityService(service);
    }

    @Override
    public void logout(UserIdentity user) {
        delegate.logout(unwrap(user));
    }

    protected static class WrappedUserIdentity implements UserIdentity {
        private final UserIdentity delegate;
        private final User user;

        public WrappedUserIdentity(UserIdentity delegate, User user) {
            this.delegate = delegate;
            this.user = user;
        }

        @Override
        public Subject getSubject() {
            return delegate.getSubject();
        }

        @Override
        public Principal getUserPrincipal() {
            return delegate.getUserPrincipal();
        }

        @Override
        public boolean isUserInRole(String role, UserIdentity.Scope scope) {
            return delegate.isUserInRole(role, scope) || user.hasRole(role);
        }
    }

    protected UserIdentity unwrap(UserIdentity user) {
        if (user instanceof WrappedUserIdentity)
            return ((WrappedUserIdentity)user).delegate;
        else
            return user;
    }
}
