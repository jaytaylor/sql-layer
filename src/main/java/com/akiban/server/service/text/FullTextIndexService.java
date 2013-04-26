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

import com.akiban.ais.model.IndexName;
import com.akiban.qp.operator.Cursor;
import com.akiban.qp.operator.QueryContext;
import com.akiban.server.service.BackgroundWork;
import com.akiban.server.service.session.Session;

import java.util.List;
import org.apache.lucene.search.Query;

/** Full service that does index maintenance and querying. */
public interface FullTextIndexService extends FullTextIndexInfos {
        
    /**
     * This is a 'promise' to populate the given index, which is *being*
     * created.
     * @param name 
     */
    public void schedulePopulate(String schema, String table, String index);

    /**
     * Update the given index based on the changedRow 
     * 
     * @param name
     * @param changedRow 
     */
    public void updateIndex(Session session, IndexName name, Iterable<byte[]> rows);
    
    /**
     * TODO: move this to <pre>Service</pre> ?
     * 
     * @return An array of available background works
     */
    public List<? extends BackgroundWork> getBackgroundWorks();
    public void dropIndex(Session session, IndexName name);
    public Cursor searchIndex(QueryContext context, IndexName name, 
                              Query query, int limit);
}
