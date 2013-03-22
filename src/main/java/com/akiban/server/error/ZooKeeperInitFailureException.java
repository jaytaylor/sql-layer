
package com.akiban.server.error;

public class ZooKeeperInitFailureException extends InvalidOperationException {
    public ZooKeeperInitFailureException (String location, String failure) {
        super (ErrorCode.ZOOKEEPER_INIT_FAIL, location, failure);
    }
}
