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

import org.junit.Test;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

public final class LockableServiceBindingsBuilderTest {
    @Test
    public void bindOne() {
        LockableServiceBindingsBuilder builder = new LockableServiceBindingsBuilder();
        builder.bind("one", "two");
        
        List<LockableServiceBinding> bindings = sorted(builder.getAllBindings());
        
        assertEquals("bindings count", 1, bindings.size());
        checkBinding("binding", bindings.get(0), "one", "two", false, false);
    }
    
    private static void checkBinding(String descriptor, LockableServiceBinding binding,
                                     String interfaceName, String implementingClass,
                                     boolean directlyRequired, boolean locked)
    {
        assertEquals(descriptor + ".interface", interfaceName, binding.getInterfaceName());
        assertEquals(descriptor + ".class", implementingClass, binding.getImplementingClassName());
        assertEquals(descriptor + ".required", directlyRequired, binding.isDirectlyRequired());
        assertEquals(descriptor + ".locked", locked, binding.isLocked());
    }

    private static List<LockableServiceBinding> sorted(Collection<ServiceBinding> bindings) {
        List<LockableServiceBinding> sortedList = new ArrayList<LockableServiceBinding>();
        for (ServiceBinding binding : bindings) {
            sortedList.add( (LockableServiceBinding) binding );
        }
        Collections.sort(sortedList, new Comparator<ServiceBinding>() {
            @Override
            public int compare(ServiceBinding o1, ServiceBinding o2) {
                return o1.getInterfaceName().compareTo(o2.getInterfaceName());
            }
        });
        return sortedList;
    }
}
