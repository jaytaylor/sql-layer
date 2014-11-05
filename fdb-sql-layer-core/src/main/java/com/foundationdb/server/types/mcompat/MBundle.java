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

package com.foundationdb.server.types.mcompat;

import com.foundationdb.server.types.TBundle;
import com.foundationdb.server.types.TBundleID;

public enum  MBundle implements TBundle {
    INSTANCE;

    @Override
    public TBundleID id() {
        return bundleId;
    }

    private static TBundleID bundleId = new TBundleID("mcompat", "b9833ebf-423d-4a60-8226-1dd635ba8892");
}
