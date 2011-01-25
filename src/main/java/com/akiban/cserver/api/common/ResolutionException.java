package com.akiban.cserver.api.common;


import com.akiban.ais.model.TableName;

import java.util.StringTokenizer;

public final class ResolutionException extends RuntimeException {
    public ResolutionException(Integer tableId, TableName tableName) {
        super(message(tableId, tableName, null));
    }

    public ResolutionException(Integer tableId, TableName tableName, String extraMessage) {
        super(message(tableId, tableName, extraMessage));
    }

    private static String message(Integer tableId, TableName tableName, String extraMessage) {
        StringBuilder sb = new StringBuilder();
        sb.append("id: ").append(tableId).append(", name: ").append(tableName);
        if (extraMessage != null) {
            sb.append(", message: ").append(extraMessage);
        }
        return sb.toString();
    }
}
