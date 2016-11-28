package org.corfudb.router;

/**
 * Created by mwei on 11/26/16.
 */
public interface IMsgBuilder<T extends IRoutableMsg<T>> {
    T build();
}
