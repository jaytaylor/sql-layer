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

package com.akiban.server.service.memcache.outputter;

import com.akiban.ais.model.Join;
import com.akiban.ais.model.UserTable;
import com.akiban.server.RowData;
import com.akiban.server.RowDef;
import com.akiban.server.api.HapiOutputter;
import com.akiban.server.api.HapiProcessedGetRequest;
import com.akiban.util.AkibanAppender;

import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public final class JsonOutputter implements HapiOutputter {
    private static final JsonOutputter instance = new JsonOutputter();

    public static JsonOutputter instance() {
        return instance;
    }

    private JsonOutputter() {}

    private static void writeEmptyChildren(PrintWriter pr, final RowDef def, Set<String> saw_children, HapiProcessedGetRequest request)
    {
        for(Join j: def.userTable().getChildJoins()) {
            UserTable child = j.getChild();
            String childName = child.getName().getTableName();
            if(request.getProjectedTables().contains(childName)
                    && (saw_children == null || saw_children.contains(childName) == false)) {
                pr.write(",\"@");
                pr.write(childName);
                pr.write("\":[]");
            }
        }
    }

    @Override
    public void output(HapiProcessedGetRequest request, List<RowData> list, OutputStream outputStream) throws IOException {
        PrintWriter pr = new PrintWriter(outputStream);

        if (list.isEmpty()) {
            pr.write("{\"@");
            pr.write(request.getTable());
            pr.write("\":[]}");
            pr.flush();
            return;
        }

        AkibanAppender appender = AkibanAppender.of(pr);
        Deque<Integer> defIdStack = new ArrayDeque<Integer>();
        Deque<Set<String>> sawChildStack = new ArrayDeque<Set<String>>();

        for(RowData data : list) {
            final int def_id = data.getRowDefId();
            final RowDef def = request.getRowDef(def_id);
            final int parent_def_id = def.getParentRowDefId();

            if(defIdStack.isEmpty()) {
                defIdStack.add(parent_def_id);
                defIdStack.add(def_id);
                sawChildStack.add(new HashSet<String>());
                pr.write("{\"@");
                pr.print(def.getTableName());
                pr.write("\":[");
            }
            else if(defIdStack.peekLast().equals(def_id)) {
                // another leaf on current branch (add to current open array)
                writeEmptyChildren(pr, def, null, request); // sawChildStack *should* be empty anyway
                pr.write("},");
            }
            else if(defIdStack.peekLast().equals(parent_def_id)) {
                // down the tree, new child branch (new open array)
                defIdStack.add(def_id);
                sawChildStack.peekLast().add(def.getTableName());
                sawChildStack.add(new HashSet<String>());

                pr.write(",\"@");
                pr.print(def.getTableName());
                pr.write("\":[");
            }
            else {
                // a) parent sibling branch, or
                // b) up the tree to a previously known parent (close array for each step up)
                RowDef d = request.getRowDef(defIdStack.removeLast());
                writeEmptyChildren(pr, d, sawChildStack.removeLast(), request);
                
                pr.write("}]");
                int pop_count = 0;
                while(!defIdStack.peekLast().equals(parent_def_id)) {
                    if(pop_count++ > 0) {
                        pr.write(" ]");
                    }

                    d = request.getRowDef(defIdStack.removeLast());
                    writeEmptyChildren(pr, d, sawChildStack.removeLast(), request);
                    pr.write("}");
                }

                if(pop_count == 0) {
                    // Was parent sibling branch
                    pr.write(",\"@");
                    pr.print(def.getTableName());
                    pr.write("\":[");
                }
                else {
                    // Was child of a known parent
                    pr.write(',');
                }
                
                defIdStack.add(def_id);
                if(!sawChildStack.isEmpty()) {
                    sawChildStack.peekLast().add(def.getTableName());
                }
                sawChildStack.add(new HashSet<String>());
            }

            pr.write('{');
            data.toJSONString(request.getRowDef(data.getRowDefId()), appender);
            }

        boolean first = true;
        while (defIdStack.size() > 1) {
            if (first == true) {
                first = false;
            } else {
                pr.write(']');
            }
            RowDef d = request.getRowDef(defIdStack.removeLast());
            writeEmptyChildren(pr, d, sawChildStack.removeLast(), request);
            pr.write('}');
        }
        pr.write("]}");
        pr.flush();
    }
}
