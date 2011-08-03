package com.akiban.server.error;

public class DuplicateGroupNameException extends InvalidOperationException {
    public DuplicateGroupNameException (String groupName) {
        super(ErrorCode.DUPLICATE_GROUP, groupName);
    }
}
