package com.akiban.server.mttests.mthapi.common;

import com.akiban.server.mttests.mthapi.base.sais.SaisBuilder;
import com.akiban.server.mttests.mthapi.base.sais.SaisTable;
import org.json.JSONException;
import org.json.JSONObject;
import org.json.JSONStringer;
import org.junit.Test;

public class JsonUtilsTest {
    @Test
    public void coiaValid() throws JSONException {
        SaisTable cTable = coia();

        JSONObject json = new JSONObject(
            new JSONStringer()
            .object()
                .key("@c").array()
                    .object()
                        .key("cid").value(1)
                        .key("@a").array()
                            .object()
                                .key("aid").value(1)
                                .key("c_id").value(1)
                            .endObject()
                        .endArray()
                        .key("@o").array()
                            .object()
                                .key("oid").value(1)
                                .key("c_id").value(1)
                                .key("@i").array().endArray()
                            .endObject()
                            .object()
                                .key("oid").value(1)
                                .key("c_id").value(1)
                                .key("@i").array()
                                    .object()
                                        .key("iid").value(1)
                                        .key("o_id").value(1)
                                    .endObject()
                                .endArray()
                            .endObject()
                        .endArray()
                    .endObject()
                .endArray()
            .endObject()
            .toString()
        );
        // from root
        JsonUtils.validateResponse(json, cTable, cTable);
    }

    private SaisTable coia() {
        SaisTable coia;SaisBuilder builder = new SaisBuilder();
        builder.table("c", "cid").pk("cid");
        builder.table("o", "oid", "c_id").pk("oid").joinTo("c").col("cid", "c_id");
        builder.table("i", "iid", "o_id").pk("iid").joinTo("o").col("oid", "o_id");
        builder.table("a", "aid", "c_id").pk("aid").joinTo("c").col("cid", "c_id");
        coia = builder.getSoleRootTable();
        return coia;
    }
}
