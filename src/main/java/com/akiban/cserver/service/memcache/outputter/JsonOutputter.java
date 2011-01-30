/**
 * Copyright (C) 2011 Akiban Technologies Inc.
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License, version 3,
 * as published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see http://www.gnu.org/licenses.
 */

package com.akiban.cserver.service.memcache.outputter;

import com.akiban.cserver.RowData;
import com.akiban.cserver.RowDef;
import com.akiban.cserver.RowDefCache;
import com.akiban.cserver.api.HapiProcessor;

import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.List;

public final class JsonOutputter implements HapiProcessor.Outputter {
    private static final JsonOutputter instance = new JsonOutputter();

    public static JsonOutputter instance() {
        return instance;
    }

    private JsonOutputter() {}

    @Override
    public void output(RowDefCache cache, List<RowData> list, OutputStream outputStream)  throws IOException {
        PrintWriter pr = new PrintWriter(outputStream);
        int current_def_id = -1;
        List<Integer> def_id_stack = new ArrayList<Integer>();

        boolean wrote = false;
        for(RowData data : list) {
            wrote = true;
            final int def_id = data.getRowDefId();
            final RowDef def = cache.getRowDef(def_id);
            final int parent_def_id = def.getParentRowDefId();

            if(def_id_stack.isEmpty()) {
                current_def_id = def_id;
                def_id_stack.add(parent_def_id);
                pr.write("{\"");
                pr.print(def.getTableName());
                pr.write("\":");
//                if(min_val == null) {
//                    sb.append(" [ ");
//                }
            }
            else if(def_id == current_def_id) {
                // another leaf on current branch (add to current open array)
                pr.write("},");
            }
            else if(parent_def_id == current_def_id) {
                // down the tree, new branch (new open array)
                current_def_id = def_id;
                def_id_stack.add(parent_def_id);

                pr.write(",\"");
                pr.print(def.getTableName());
                pr.write("\":[");
            }
            else {
                // a) sibling branch or b) up the tree to an old branch (close array for each step up)
                current_def_id = def_id;
                int pop_count = 0;
                int last = def_id_stack.size() - 1;

                pr.write("}]");
                while(!def_id_stack.get(last).equals(parent_def_id)) {
                    if(pop_count++ > 0) {
                        pr.write(" ]");
                    }
                    pr.write("}");
                    def_id_stack.remove(last--);
                }

                if(pop_count == 0) {
                    // Was sibling
                    pr.write(",\"");
                    pr.print(def.getTableName());
                    pr.write("\":[");
                }
                else {
                    // Was child
                    pr.write(',');
                }
            }

            String json_row = data.toJSONString(cache);
            pr.write('{');
            pr.print(json_row);
        }

        if(wrote) {
            int last = def_id_stack.size() - 1;
            while(last > 0) {
                pr.write("}]");
                def_id_stack.remove(last--);
            }
            pr.write('}');
//            if(min_val == null) {
//                sb.append(" ]");
//            }
            pr.write('}');
        }
        pr.flush();
    }
}
