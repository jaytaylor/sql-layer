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

package com.foundationdb.server.service.text;

import com.foundationdb.ais.model.IndexName;
import com.foundationdb.qp.operator.RowCursor;
import com.foundationdb.qp.operator.QueryContext;

import org.apache.lucene.search.Query;

/** Full service that does index maintenance and querying. */
public interface FullTextIndexService extends FullTextIndexInfos {
    public RowCursor searchIndex(QueryContext context, IndexName name, Query query, int limit);

    /** Wait for a complete run of background workers */
    public void backgroundWait();
}
