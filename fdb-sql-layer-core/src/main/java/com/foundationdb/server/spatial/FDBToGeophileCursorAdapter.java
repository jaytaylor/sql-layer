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

package com.foundationdb.server.spatial;

import com.geophile.z.Cursor;
import com.geophile.z.Index;
import com.geophile.z.Record;

import java.io.IOException;

class FDBToGeophileCursorAdapter<RECORD extends Record> extends Cursor<RECORD>
{
    // Geohpile Cursor interface

    @Override
    public RECORD next() throws IOException, InterruptedException
    {
        return null;
    }

    @Override
    public RECORD previous() throws IOException, InterruptedException
    {
        return null;
    }

    @Override
    public void goTo(RECORD key) throws IOException, InterruptedException
    {

    }

    @Override
    public boolean deleteCurrent() throws IOException, InterruptedException
    {
        return false;
    }

    // FDBToGeophileCursorAdapter interface

    FDBToGeophileCursorAdapter(Index<RECORD> index)
    {
        super(index);
    }
}
