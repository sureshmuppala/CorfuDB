package org.corfudb.router;

/**
 * Created by mwei on 11/23/16.
 */
public interface IRoutableMsg<T> {

    T getMsgType();
}
