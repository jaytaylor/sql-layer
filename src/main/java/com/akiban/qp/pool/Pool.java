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

package com.akiban.qp.pool;

import java.util.LinkedList;
import java.util.Queue;

/* public */ class Pool
{
    public Resource take()
    {
        Resource resource = resources.remove();
        resource.prepareForUse();
        return resource;
    }

    public void add(Resource resource)
    {
        resource.prepareForDisuse();
        resources.add(resource);
    }

    // Object state

    private final Queue<Resource> resources = new LinkedList<Resource>();

    public interface Resource
    {
        void prepareForUse();
        void prepareForDisuse();
    }
}
