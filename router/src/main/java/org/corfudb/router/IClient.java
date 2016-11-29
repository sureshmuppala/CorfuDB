package org.corfudb.router;

import io.netty.channel.ChannelHandlerContext;

import java.util.Set;

/**
 * Created by mwei on 11/23/16.
 */
public interface IClient<M extends IRoutableMsg<T>, T> {

    /**
     * Handle a incoming message on the channel
     *
     * @param msg The incoming message
     * @param channel The channel
     */
    void handleMessage(M msg, IChannel<M> channel);

    /**
     * Returns a set of message types that the client handles.
     *
     * @return The set of message types this client handles.
     */
    Set<T> getHandledTypes();
}
