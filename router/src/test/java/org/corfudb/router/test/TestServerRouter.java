package org.corfudb.router.test;

import io.netty.channel.ChannelHandlerContext;
import org.corfudb.router.IRespondableMsg;
import org.corfudb.router.IRoutableMsg;
import org.corfudb.router.IServer;
import org.corfudb.router.IServerRouter;

/**
 * Created by mwei on 11/29/16.
 */
public class TestServerRouter<M extends IRoutableMsg<T>, T> implements IServerRouter<M,T> {


    /**
     * Send a message, unsolicited.
     *
     * @param ctx    The context to send the response to.
     * @param outMsg The outgoing message.
     */
    @Override
    public void sendMessage(ChannelHandlerContext ctx, M outMsg) {

    }

    /**
     * Send a message, in response to a message.
     *
     * @param ctx    The context to send the response to.
     * @param inMsg  The message we are responding to.
     * @param outMsg The outgoing message.
     */
    @Override
    public void sendResponse(ChannelHandlerContext ctx, IRespondableMsg inMsg, IRespondableMsg outMsg) {

    }

    /**
     * Register a new server to route messages to.
     *
     * @param server The server to route messages to.
     * @return This server router, to support chaining.
     */
    @Override
    public IServerRouter<M, T> registerServer(IServer<M, T> server) {
        return null;
    }

    /**
     * Return a registered server of the given type.
     *
     * @param serverType The type of the server to return.
     * @return A server of the given type.
     */
    @Override
    public <R extends IServer<M, T>> R getServer(Class<R> serverType) {
        return null;
    }

    /**
     * Start listening for messages and route to the registered servers.
     *
     * @return This server router, to support chaining.
     */
    @Override
    public IServerRouter<M, T> start() {
        return null;
    }

    /**
     * Stop listening for messages and stop routing messages to the registered servers.
     *
     * @return This server router, to support chaining.
     */
    @Override
    public IServerRouter<M, T> stop() {
        return null;
    }
}
