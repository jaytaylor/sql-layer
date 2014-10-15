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

package com.foundationdb.server.service.dxl;

public final class DXLTestHookRegistry {

    public static DXLTestHooks get() {
        return INSTANCE;
    }

    private static class SingleMiddlemanHooks implements DXLTestHooks {
        @Override
        public boolean openCursorsExist() {
            return ! middleman().getScanDataMap().isEmpty();
        }

        @Override
        public String describeOpenCursors() {
            return middleman().getScanDataMap().toString();
        }

        private BasicDXLMiddleman middleman() {
            BasicDXLMiddleman middleman = BasicDXLMiddleman.last();
            if (middleman == null) {
                throw new RuntimeException("no active middleman; DXLService probably wasn't started correctly");
            }
            return middleman;
        }
    }

    private static final SingleMiddlemanHooks INSTANCE = new SingleMiddlemanHooks();
}
