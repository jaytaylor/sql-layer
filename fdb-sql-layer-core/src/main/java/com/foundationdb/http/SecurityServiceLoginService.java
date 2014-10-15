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

package com.foundationdb.http;

import java.util.List;

import com.foundationdb.server.service.security.SecurityService;
import com.foundationdb.server.service.security.User;
import com.foundationdb.util.ArgumentValidation;
import org.eclipse.jetty.security.MappedLoginService;
import org.eclipse.jetty.server.UserIdentity;
import org.eclipse.jetty.util.security.Credential;

public class SecurityServiceLoginService extends MappedLoginService
{
    public enum CredentialType { BASIC, DIGEST }

    private final SecurityService securityService;
    private final CredentialType credentialType;
    private final long cacheMillis;
    private volatile long lastCachePurge;

    public SecurityServiceLoginService(SecurityService securityService, CredentialType credentialType, int cacheSeconds) {
        ArgumentValidation.isGTE("cacheSeconds", cacheSeconds, 0);
        if(credentialType != CredentialType.BASIC && credentialType != CredentialType.DIGEST) {
            throw new IllegalArgumentException("Unknown credential: " + credentialType);
        }
        this.securityService = securityService;
        this.credentialType = credentialType;
        this.cacheMillis = cacheSeconds * 1000;
    }

    @Override
    public UserIdentity login(String username, Object credentials) {
        long now = System.currentTimeMillis();
        if((now - lastCachePurge) > cacheMillis) {
            super._users.clear();
            lastCachePurge = now;
        }
        return super.login(username, credentials);
    }

    @Override
    protected void loadUsers() {
    }

    @Override
    protected UserIdentity loadUser(String username) {
        User user = securityService.getUser(username);
        if(user != null) {
            String password = (credentialType == CredentialType.BASIC) ? user.getBasicPassword() : user.getDigestPassword();
            List<String> roles = user.getRoles();
            return putUser(username, Credential.getCredential(password), roles.toArray(new String[roles.size()]));
        }
        return null;
    }
}
