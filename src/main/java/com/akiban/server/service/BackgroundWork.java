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
import java.util.Observable;


public interface BackgroundWork<O extends BackgroundObserver, W extends BackgroundWork>
{
    public void addObserver(O observer);

    public void removeObserver(O observer);
    
    public void removeObsevers(Collection<O> os);
    
    public void removeAllObservers();
    
    public void notifyObservers();

    /**
     * 
     * @return the minium time interval that one has to wait to be guaranteed that
     * the work has been executed at least once.
     */
    public abstract long getMinimumWaitTime();
}
