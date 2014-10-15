/**
 * Copyright (C) 2009-2013 FoundationDB, LLC
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

package com.foundationdb.server.service.listener;

import com.foundationdb.server.service.Service;

import java.util.Set;
import java.util.concurrent.CopyOnWriteArraySet;

public class ListenerServiceImpl implements ListenerService, Service
{
    private final Set<TableListener> tableListeners;
    private final Set<RowListener> rowListeners;

    public ListenerServiceImpl() {
        // Modifications are only expected at startup and shutdown, but be prepared for dynamic services.
        this.tableListeners = new CopyOnWriteArraySet<>();
        this.rowListeners = new CopyOnWriteArraySet<>();
    }


    //
    // ListenerService
    //

    @Override
    public Iterable<TableListener> getTableListeners() {
        return tableListeners;
    }

    @Override
    public void registerTableListener(TableListener listener) {
        tableListeners.add(listener);
    }

    @Override
    public void deregisterTableListener(TableListener listener) {
        tableListeners.remove(listener);
    }

    @Override
    public Iterable<RowListener> getRowListeners() {
        return rowListeners;
    }

    @Override
    public void registerRowListener(RowListener listener) {
        rowListeners.add(listener);
    }

    @Override
    public void deregisterRowListener(RowListener listener) {
        rowListeners.remove(listener);
    }


    //
    // Service
    //

    @Override
    public void start() {
        // None
    }

    @Override
    public void stop() {
        tableListeners.clear();
        rowListeners.clear();
    }

    @Override
    public void crash() {
        stop();
    }
}
