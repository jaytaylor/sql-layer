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
package com.foundationdb.qp.persistitadapter.indexcursor;

import com.foundationdb.server.collation.AkCollator;
import com.foundationdb.server.types.AkType;
import com.foundationdb.server.types3.TInstance;
import com.persistit.Key;

public interface SortKeyTarget<S> {
    void attach(Key key);
    void append(S source, int f, AkType[] akTypes, TInstance[] tInstances, AkCollator[] collators);
    void append(S source, AkType akType, TInstance tInstance, AkCollator collator);
    void append(S source, AkCollator collator, TInstance tInstance);
}
