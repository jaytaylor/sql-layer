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

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

class LockableServiceBindingsBuilder {

    // LockableServiceBindingsBuilder public interface

    public void bind(String interfaceName, String className) {
        LockableServiceBinding binding = bindings.get(interfaceName);
        if (binding == null) {
            binding = new DefaultLockableServiceBinding(interfaceName);
            binding.setImplementingClass(className);
            bindings.put(interfaceName, binding);
        } else {
            if (binding.isLocked()) {
                throw new ServiceBindingException(interfaceName + " is locked");
            }
            binding.setImplementingClass(className);
        }
    }

    public Collection<ServiceBinding> getAllBindings() {
        return new ArrayList<ServiceBinding>(bindings.values());
    }

    public Collection<ServiceBinding> getDirectlyRequiredBindings() {
        Collection<ServiceBinding> filtered = getAllBindings();
        for(Iterator<ServiceBinding> iterator = filtered.iterator(); iterator.hasNext(); ) {
            if (!iterator.next().isDirectlyRequired()) {
                iterator.remove();
            }
        }
        return filtered;
    }

    public void lock(String interfaceName) {
        require(interfaceName).lock();
    }

    public boolean isDefined(String interfaceName) {
        return bindings.get(interfaceName) != null;
    }

    public boolean isLocked(String interfaceName) {
        return require(interfaceName).isLocked();
    }

    public boolean isBound(String interfaceName) {
        return require(interfaceName).getImplementingClassName() != null;
    }

    // private methods

    private LockableServiceBinding require(String interfaceName) {
        LockableServiceBinding binding = bindings.get(interfaceName);
        if (interfaceName == null) {
            throw new ServiceBindingException(interfaceName + " is not defined");
        }
        return binding;
    }

    // object state
    // invariant: key = bindings[key].getInterfaceName()
    Map<String, LockableServiceBinding> bindings = new HashMap<String, LockableServiceBinding>();
}
