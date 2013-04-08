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

package com.akiban.server.types3.mcompat.mfuncs;

import com.akiban.server.service.BackgroundObserverImpl;
import com.akiban.server.service.BackgroundWork;
import java.util.LinkedList;
import java.util.List;


public class WaitFunctionHelpers
{
    /**
     * Blocks until all `works` have finished
     * @param works
     * @throws InterruptedException 
     */
    public static void waitOn(List<? extends BackgroundWork> works) throws InterruptedException
    {
        if (works != null)
        {
            List<BackgroundObserverImpl> waiters = new LinkedList<>();
            for (BackgroundWork wk : works)
            {
                // This work doesn't require wait time
                if (wk.getMinimumWaitTime() <= 0)
                    continue;
                BackgroundObserverImpl w = new BackgroundObserverImpl();
                wk.addObserver(w);
                waiters.add(w);
            }

            // busy-loop waiting for all the work to be done
            boolean allAwaken;
            while (true)
            {
                allAwaken = true;
                for (BackgroundObserverImpl w : waiters)
                {
                    allAwaken &= w.backgroundFinished();
                }
                
                if (allAwaken)
                    break;
            }
        }
    }
    
}
