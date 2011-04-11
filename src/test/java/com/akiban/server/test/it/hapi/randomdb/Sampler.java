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

package com.akiban.server.test.it.hapi.randomdb;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

// For sampling without replacement

class Sampler
{
    public int take()
    {
        int position = random.nextInt(ids.size());
        int id = ids.get(position);
        ids.remove(position);
        return id;
    }

    public Sampler(Random random, int n)
    {
        this.random = random;
        ids = new ArrayList<Integer>(n);
        for (int i = 0; i < n; i++) {
            ids.add(i);
        }
    }

    private final Random random;
    private final List<Integer> ids;
}
