/**
 * Copyright (C) 2009-2015 FoundationDB, LLC
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

package com.foundationdb.server.service.statusmonitor;

import com.foundationdb.server.service.Service;

public class DummyStatusMonitor implements StatusMonitorService, Service
{
    @Override
    public void start() {
        // None
    }

    @Override
    public void stop() {
        // None
    }

    @Override
    public void crash() {
        // None
    }
}
