
package com.akiban.server.types3;

public interface TInstanceAdjuster {
    TInstance get(int i);
    TInstanceBuilder adjust(int i);
    void replace(int i, TInstance tInstance);
}
