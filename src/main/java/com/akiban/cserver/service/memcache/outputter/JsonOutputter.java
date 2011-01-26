package com.akiban.cserver.service.memcache.outputter;

import com.akiban.cserver.RowData;
import com.akiban.cserver.RowDef;
import com.akiban.cserver.RowDefCache;
import com.akiban.cserver.api.HapiProcessor;

import java.util.ArrayList;
import java.util.List;

public final class JsonOutputter implements HapiProcessor.Outputter<String> {
    private static final JsonOutputter instance = new JsonOutputter();

    public static JsonOutputter instance() {
        return instance;
    }

    private JsonOutputter() {}

    @Override
    public String output(RowDefCache cache, List<RowData> list, StringBuilder sb) {
        int current_def_id = -1;
        List<Integer> def_id_stack = new ArrayList<Integer>();

        for(RowData data : list) {
            final int def_id = data.getRowDefId();
            final RowDef def = cache.getRowDef(def_id);
            final int parent_def_id = def.getParentRowDefId();

            if(def_id_stack.isEmpty()) {
                current_def_id = def_id;
                def_id_stack.add(parent_def_id);
                sb.append("{ \"");
                sb.append(def.getTableName());
                sb.append("\" : ");
//                if(min_val == null) {
//                    sb.append(" [ ");
//                }
            }
            else if(def_id == current_def_id) {
                // another leaf on current branch (add to current open array)
                sb.append(" }, ");
            }
            else if(parent_def_id == current_def_id) {
                // down the tree, new branch (new open array)
                current_def_id = def_id;
                def_id_stack.add(parent_def_id);

                sb.append(", \"");
                sb.append(def.getTableName());
                sb.append("\" : [ ");
            }
            else {
                // a) sibling branch or b) up the tree to an old branch (close array for each step up)
                current_def_id = def_id;
                int pop_count = 0;
                int last = def_id_stack.size() - 1;

                sb.append(" } ]");
                while(!def_id_stack.get(last).equals(parent_def_id)) {
                    if(pop_count++ > 0) {
                        sb.append(" ]");
                    }
                    sb.append(" }");
                    def_id_stack.remove(last--);
                }

                if(pop_count == 0) {
                    // Was sibling
                    sb.append(", \"");
                    sb.append(def.getTableName());
                    sb.append("\" : [ ");
                }
                else {
                    // Was child
                    sb.append(", ");
                }
            }

            sb.append("{ ");
            data.toJSONString(cache, sb);
        }

        if(sb.length() > 0) {
            int last = def_id_stack.size() - 1;
            while(last > 0) {
                sb.append(" } ]");
                def_id_stack.remove(last--);
            }
            sb.append(" }");
//            if(min_val == null) {
//                sb.append(" ]");
//            }
            sb.append(" }");
        }
        return sb.toString();
    }

    @Override
    public String error(String message) {
        return message;
    }
}
