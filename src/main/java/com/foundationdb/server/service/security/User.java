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

import java.security.Principal;

import java.util.List;

public class User implements Principal
{
    private final int id;
    private final String name;
    private final String basicPassword;
    private final String digestPassword;
    private final List<String> roles;

    protected User(int id, String name, String basicPassword, String digestPassword, List<String> roles) {
        this.id = id;
        this.name = name;
        this.basicPassword = basicPassword;
        this.digestPassword = digestPassword;
        this.roles = roles;
    }

    public int getId() {
        return id;
    }

    @Override
    public String getName() {
        return name;
    }

    public String getBasicPassword() {
        return basicPassword;
    }

    public String getDigestPassword() {
        return digestPassword;
    }

    public List<String> getRoles() {
        return roles;
    }

    public boolean hasRole(String role) {
        return roles.contains(role);
    }

    @Override
    public String toString() {
        return name + "(" + id + ")";
    }

    @Override
    public boolean equals(Object other) {
        return ((other instanceof User) && (id == ((User)other).id));
    }

    @Override
    public int hashCode() {
        return id;
    }

}
