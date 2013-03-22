
package com.akiban.server.service.security;

import java.security.Principal;

import java.util.List;

public class User implements Principal
{
    private final int id;
    private final String name;
    private final List<String> roles;

    protected User(int id, String name, List<String> roles) {
        this.id = id;
        this.name = name;
        this.roles = roles;
    }

    public int getId() {
        return id;
    }

    @Override
    public String getName() {
        return name;
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
