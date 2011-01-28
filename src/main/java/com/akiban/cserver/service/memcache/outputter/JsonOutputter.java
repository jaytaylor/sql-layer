package com.akiban.cserver.service.memcache.outputter;

import com.akiban.ais.model.Join;
import com.akiban.ais.model.TableName;
import com.akiban.ais.model.UserTable;
import com.akiban.cserver.RowData;
import com.akiban.cserver.RowDef;
import com.akiban.cserver.RowDefCache;
import com.akiban.cserver.api.HapiGetRequest;
import com.akiban.cserver.api.HapiProcessor;
import com.akiban.util.AkibanAppender;

import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

public final class JsonOutputter implements HapiProcessor.Outputter {
    private static final JsonOutputter instance = new JsonOutputter();

    public static JsonOutputter instance() {
        return instance;
    }

    private JsonOutputter() {}



    @Override
    public void output(HapiGetRequest request, RowDefCache cache, List<RowData> list, OutputStream outputStream)  throws IOException {
        PrintWriter printWriter = new PrintWriter(outputStream);

        if (list.isEmpty()) {
            printWriter.print("{\"@");
            printWriter.print(request.getTable());
            printWriter.print("\"=[]}");
            printWriter.flush();
            return;
        }

        AkibanAppender out = AkibanAppender.of(printWriter);

        f(request, cache, list, out);
//        out.write('{';
//        outputForDepth(request.getSchema(), request.getTable(), list.iterator(), cache, out);
//        out.write('}');

        printWriter.flush();
    }

    private static Map<Integer,String> getChildren(String forSchema, String forTable, RowDefCache cache) {
        RowDef def = cache.getRowDef( RowDefCache.nameOf(forSchema, forTable) );
        Map<Integer,String> ret = new HashMap<Integer,String>();
        for(Join join : def.userTable().getChildJoins()) {
            UserTable child = join.getChild();
            assert forSchema.equals(child.getName().getSchemaName())
                    : String.format("multi-schema group: %s != %s",
                    TableName.create(forSchema, forTable), child.getName());
            assert child.getTableId() != null : child;
            ret.put(child.getTableId(), child.getName().getTableName());
        }
        return ret;
    }

    private static void outputForDepth(String schema, String table, Iterator<RowData> iterator,
                                       RowDefCache cache, AkibanAppender out)
    throws IOException
    {
        Map<Integer,String> tablesNeeded = getChildren(schema, table, cache);

        while(iterator.hasNext()) {
            final RowData data = iterator.next();
        }

        // Tables we haven't seen but should have
        for(Iterator<String> iter=tablesNeeded.values().iterator(); iter.hasNext(); ) {
            String emptyTable = iter.next();
            out.write("\"@");
            out.write(emptyTable);
            out.write("\"=[]");
            if (iter.hasNext()) {
                out.write(',');
            }
        }
    }

    private static class ChildrenMap
    {
        private final Map<Integer,Map<Integer,String>> map = new HashMap<Integer, Map<Integer, String>>();
        private final RowDefCache rowDefCache;

        private ChildrenMap(RowDefCache rowDefCache) {
            this.rowDefCache = rowDefCache;
        }

        Map<Integer,String> childrenOf(RowDef def) {
            Map<Integer,String> children = map.get(def.getRowDefId());
            if(children == null) {
                List<Join> joins = def.userTable().getChildJoins();
                if (joins.isEmpty()) {
                    children = Collections.emptyMap();
                }
                else {
                    children = new HashMap<Integer, String>();
                    for(Join join : joins) {
                        UserTable child = join.getChild();
                        assert child != null : join;
                        assert child.getTableId() != null : child;
                        children.put(child.getTableId(), child.getName().getTableName());
                    }
                }
                map.put(def.getRowDefId(), children);
            }
            return new HashMap<Integer, String>(children);
        }
    }

    private static void f(HapiGetRequest request, RowDefCache cache, List<RowData> list, AkibanAppender out)  throws IOException
    {
        int current_def_id = -1;
        List<Integer> def_id_stack = new ArrayList<Integer>();
        List<Map<Integer,String>> requiredTables = new ArrayList<Map<Integer, String>>();
        final ChildrenMap childrenMap = new ChildrenMap(cache);

        for(RowData data : list) {
            final int def_id = data.getRowDefId();
            final RowDef def = cache.getRowDef(def_id);
            final int parent_def_id = def.getParentRowDefId();

            if(def_id_stack.isEmpty()) {
                current_def_id = def_id;
                def_id_stack.add(parent_def_id);
                requiredTables.add(childrenMap.childrenOf(def));
                out.write("{\"@");
                out.write(def.getTableName());
                out.write("\":[");
            }
            else if(def_id == current_def_id) {
                requiredTables.get(requiredTables.size()-1).remove(def_id);
                // another leaf on current branch (add to current open array)
                out.write("},");
            }
            else if(parent_def_id == current_def_id) {
                requiredTables.get(requiredTables.size()-1).remove(def_id);
                // down the tree, new branch (new open array)
                current_def_id = def_id;
                Map<Integer,String> children = childrenMap.childrenOf(def);
                if(!children.isEmpty()) {
                    requiredTables.add(childrenMap.childrenOf(def));
                }
                def_id_stack.add(parent_def_id);

                out.write(",\"@");
                out.write(def.getTableName());
                out.write("\":[");
            }
            else {
                // a) sibling branch or b) up the tree to an old branch (close array for each step up)
                current_def_id = def_id;
                int pop_count = 0;
                int last = def_id_stack.size() - 1;

                out.write("}]");
                while(!def_id_stack.get(last).equals(parent_def_id)) {
                    if(pop_count++ > 0) {
                        out.write(" ]");
                    }
                    out.write("}");
                    def_id_stack.remove(last--);
                    Collection<String> unseenTables = requiredTables.remove(last).values();
                    printUnseenTables(unseenTables, out);
                }

                if(pop_count == 0) {
                    requiredTables.get(requiredTables.size()-1).remove(def_id);
                    // Was sibling
                    out.write(",\"@");
                    out.write(def.getTableName());
                    out.write("\":[");
                }
                else {
                    Map<Integer,String> children = childrenMap.childrenOf(def);
                    if(!children.isEmpty()) {
                        requiredTables.add(childrenMap.childrenOf(def));
                    }
                    def_id_stack.add(parent_def_id);
                    // Was child
                    out.write(',');
                }
            }

            out.write('{');
            data.toJSONString(cache, out);
        }

        int last = def_id_stack.size() - 1;
        while(last > 0) {
            out.write("}]");
            def_id_stack.remove(last--);
        }
        out.write("}]}");
        assert requiredTables.size() == 1 && requiredTables.get(0).isEmpty() : requiredTables;
    }

    private static void printUnseenTables(Collection<String> unseenTables, AkibanAppender out) {
        for(Iterator<String> iter=unseenTables.iterator(); iter.hasNext(); ) {
            String emptyTable = iter.next();

            out.write("\"@");
            out.write(emptyTable);
            out.write("\"=[]");
            if (iter.hasNext()) {
                out.write(',');
            }
        }
    }
}
