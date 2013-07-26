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

package com.akiban.server.service.text;

import com.akiban.server.test.it.ITBase;
import org.junit.Before;

public class FullTextIndexServiceITBase extends ITBase
{
    protected FullTextIndexServiceImpl fullTextImpl;

    @Before
    public final void castService() {
        fullTextImpl = (FullTextIndexServiceImpl)serviceManager().getServiceByClass(FullTextIndexService.class);
    }

    protected void waitPopulate() {
        fullTextImpl.waitPopulateCycle();
    }

    protected void waitUpdate() {
        fullTextImpl.waitUpdateCycle();
    }

    protected void waitPopulateAndUpdate() {
        waitPopulate();
        waitUpdate();
    }
}
