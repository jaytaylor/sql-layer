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

import com.akiban.server.service.BackgroundObserver;
import com.akiban.server.service.BackgroundObserverImpl;
import com.akiban.server.service.BackgroundWork;

import java.util.Collection;
import java.util.LinkedList;
import java.util.List;

public class WaitFunctionHelpers
{
    /** Blocks until all `works` have finished */
    public static void waitOn(Collection<? extends BackgroundWork> works) throws InterruptedException
    {
        if (works != null)
        {
            List<BackgroundObserver> waiters = new LinkedList<>();
            for (BackgroundWork wk : works)
            {
                // This work doesn't require wait time
                if (wk.getMinimumWaitTime() <= 0)
                    continue;

                // add observer
                BackgroundObserverImpl w = new BackgroundObserverImpl();
                wk.addObserver(w);

                // request the task to be executed
                wk.forceExecution();
                
                waiters.add(w);
            }

            // busy-loop waiting for all the works to be done
            boolean allAwaken;
            while (true)
            {
                allAwaken = true;
                for (BackgroundObserver w : waiters)
                {
                    allAwaken &= w.backgroundFinished();
                }
                
                if (allAwaken)
                    break;
            }
            
            // clean up
            for (BackgroundWork wk : works)
            {
                wk.removeObservers(waiters);
            }
        }
    }
    
}
