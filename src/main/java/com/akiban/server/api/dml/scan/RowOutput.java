
package com.akiban.server.api.dml.scan;

public interface RowOutput {
    void output(NewRow row);
    void mark();
    void rewind();
}
