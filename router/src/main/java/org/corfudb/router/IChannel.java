package org.corfudb.router;

/**
 * Created by mwei on 11/29/16.
 */
public interface IChannel<M> {
    void sendMessage(M message);
}
