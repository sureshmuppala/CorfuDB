package org.corfudb.router;

import io.netty.channel.ChannelHandlerContext;

/**
 * Created by mwei on 11/23/16.
 */
public interface IServer<M extends IRoutableMsg<T>, T> {

    /** Get the message handler for this instance.
     * @return  A message handler.
     */
    ServerMsgHandler<M, T> getMsgHandler();

    /**
     * Handle a incoming message.
     *
     * @param msg An incoming message.
     * @param ctx The channel handler context.
     */
    default void handleMessage(M msg, ChannelHandlerContext ctx) {
        getMsgHandler().handle(msg, ctx);
    }

    /** Get the router this server is attached to. */
    IServerRouter<M,T> getRouter();
}
