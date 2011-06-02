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

package com.akiban.server.service.servicemanager;

import com.akiban.util.ArgumentValidation;

public final class DefaultLockableServiceBinding implements LockableServiceBinding {

    // LockableServiceBinding interface

    @Override
    public boolean isLocked() {
        return locked;
    }

    @Override
    public void lock() {
        locked = true;
    }

    // ServiceBinding interface

    @Override
    public String getInterfaceName() {
        return interfaceName;
    }

    @Override
    public void setImplementingClass(String className) {
        if (isLocked()) {
            throw new ServiceBindingException("can't set new implementing class: " + interfaceName + " is locked");
        }
        implementingClassName = className;
    }

    @Override
    public String getImplementingClassName() {
        return implementingClassName;
    }

    @Override
    public boolean isDirectlyRequired() {
        return directlyRequired;
    }

    @Override
    public void markDirectlyRequired() {
        directlyRequired = true;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof DefaultLockableServiceBinding)) return false;

        DefaultLockableServiceBinding that = (DefaultLockableServiceBinding) o;

        return !(interfaceName != null ? !interfaceName.equals(that.interfaceName) : that.interfaceName != null);

    }

    @Override
    public int hashCode() {
        return interfaceName != null ? interfaceName.hashCode() : 0;
    }

    // Object interface

    @Override
    public String toString() {
        StringBuilder builder = new StringBuilder("Binding(");
        builder.append(getInterfaceName()).append(" -> ").append(getImplementingClassName());
        builder.append(
                isLocked() ? ", locked)" : ", unlocked)"
        );
        return builder.toString();
    }

    // private methods

    // DefaultLockableServiceBinding interface

    public DefaultLockableServiceBinding(String interfaceName) {
        ArgumentValidation.notNull("interface name", interfaceName);
        this.interfaceName = interfaceName;
    }

    // object state

    private final String interfaceName;
    private String implementingClassName;
    private boolean locked;
    private boolean directlyRequired;
}
