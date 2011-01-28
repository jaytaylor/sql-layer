package com.akiban.cserver.service.memcache.outputter;

import com.akiban.cserver.RowData;
import com.akiban.cserver.RowDef;
import com.akiban.cserver.RowDefCache;
import com.akiban.cserver.api.HapiProcessor;
import com.akiban.util.AkibanAppender;

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
        PrintWriter printWriter = new PrintWriter(outputStream);
        AkibanAppender out = AkibanAppender.of(printWriter);
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
                out.write("{\"@");
                out.write(def.getTableName());
                out.write("\":[");
            }
            else if(def_id == current_def_id) {
                // another leaf on current branch (add to current open array)
                out.write("},");
            }
            else if(parent_def_id == current_def_id) {
                // down the tree, new branch (new open array)
                current_def_id = def_id;
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
                }

                if(pop_count == 0) {
                    // Was sibling
                    out.write(",\"@");
                    out.write(def.getTableName());
                    out.write("\":[");
                }
                else {
                    // Was child
                    out.write(',');
                }
            }

            out.write('{');
            data.toJSONString(cache, out);
        }

        if(wrote) {
            int last = def_id_stack.size() - 1;
            while(last > 0) {
                out.write("}]");
                def_id_stack.remove(last--);
            }
            out.write("}]}");
        }
        printWriter.flush();
    }
}
