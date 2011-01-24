package com.akiban.cserver.service.memcache;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import com.akiban.cserver.RowData;
import com.akiban.cserver.RowDef;
import com.akiban.cserver.RowDefCache;
import com.akiban.cserver.service.session.Session;
import com.akiban.cserver.store.Store;

final class HapiProcessorImpl{

	static String processRequest(Store store, Session session, String request, ByteBuffer byteBuffer)  {
            String[] tokens = request.split(":");        
            
            if(tokens.length == 3 || tokens.length == 4) {
                String schema = tokens[0];
                String table = tokens[1];
                String colkey = tokens[2];
                String min_val = null;
                String max_val = null;
                StringBuilder sb = new StringBuilder();
                

                if(tokens.length == 4) {
                    min_val = max_val = tokens[3];
                }

                final RowDefCache cache = store.getRowDefCache();

                try {
                    List<RowData> list = store.fetchRows(session, schema, table, colkey, min_val, max_val, null, byteBuffer);
                    
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
                            if(min_val == null) {
                                sb.append(" [ ");
                            }
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

                        String json_row = data.toJSONString(cache);
                        sb.append("{ ");
                        sb.append(json_row);
                    }

                    if(sb.length() > 0) {
                        int last = def_id_stack.size() - 1;
                        while(last > 0) {
                            sb.append(" } ]");
                            def_id_stack.remove(last--);
                        }
                        sb.append(" }");
                        if(min_val == null) {
                            sb.append(" ]");
                        }
                        sb.append(" }");                   
                    }
                    return sb.toString();
                }
                catch(Exception e) {
                 return ("read error: " + e.getMessage());
                }
            }
            else {
                return ("invalid key: " + request);
            }
            
	}
}
