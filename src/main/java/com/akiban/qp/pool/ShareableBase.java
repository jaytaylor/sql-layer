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

/* public */ class ShareableBase implements Shareable, Pool.Resource
{
    // Pool.Resource interface

    public void prepareForUse()
    {
        assert references == 0 : this;
        references = 1;
    }

    public void prepareForDisuse()
    {
        assert references == 0 : this;
    }

    // Shareable interface

    public synchronized ShareableBase share()
    {
        references++;
        return this;
    }

    public synchronized ShareableBase exclusive()
    {
        return references == 1 ? this : (ShareableBase) pool.take();
    }

    public synchronized void release()
    {
        if (--references == 0) {
            pool.add(this);
        }
    }

    public Pool pool()
    {
        return pool;
    }

    public int references()
    {
        return references;
    }

    public ShareableBase(Pool pool)
    {
        this.pool = pool;
        references = 0;
    }

    // Object state

    private final Pool pool;
    private volatile int references;
}
