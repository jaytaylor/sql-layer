/**
 * Copyright (C) 2009-2013 Akiban Technologies, Inc.
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

package com.akiban.server.service;

import java.util.Collection;
import java.util.concurrent.ConcurrentHashMap;

public abstract class BackgroundWorkBase implements BackgroundWork<BackgroundObserver, BackgroundWork>
{
    private final ConcurrentHashMap<BackgroundObserver,Boolean> observers = new ConcurrentHashMap<>();
    
    public BackgroundWorkBase()
    {
        
    }

    @Override
    public void addObserver(BackgroundObserver observer)
    {
        // two observers are equal IFF they are the same object.
        observers.put(observer, true);
    }

    @Override
    public void removeObserver(BackgroundObserver observer)
    {
        observers.remove(observer);
    }

    public void removeObsevers(Collection<BackgroundObserver> os)
    {
        for (BackgroundObserver key : os) {
            observers.remove(key);
        }
    }

    @Override
    public void removeAllObservers()
    {
        observers.clear();
    }

    @Override
    public void notifyObservers()
    {
        for (BackgroundObserver o : observers.keySet())
            o.update(this);
    }
}
