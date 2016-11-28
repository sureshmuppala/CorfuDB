package org.corfudb.router;

import io.netty.channel.ChannelHandlerContext;
import lombok.Getter;
import lombok.Setter;

import java.util.Set;
import java.util.concurrent.CompletableFuture;

/** A basic implementation of a client which supports routable messages.
 *
 * Implement getMsgHandler by providing a message handler for your client.
 *
 * Created by mwei on 11/23/16.
 */
public abstract class AbstractClient<M extends IRoutableMsg<T>, T> implements IClient<M,T> {

    @Getter
    final IClientRouter<M,T> router;

    public AbstractClient(IClientRouter<M,T> router) {
        this.router = router;
    }

    public abstract ClientMsgHandler<M,T> getMsgHandler();

    /** Send a message, not expecting a response.
     *
     * @param msg   The message to send.
     */
    protected void sendMessage(M msg) {
        router.sendMessage(msg);
    }

    /**
     * Handle a incoming message on the channel
     *
     * @param msg The incoming message
     * @param ctx The channel handler context
     */
    public void handleMessage(M msg, ChannelHandlerContext ctx) {
        getMsgHandler().handle(msg, ctx);
    }

    /**
     * Returns a set of message types that the client handles.
     *
     * @return The set of message types this client handles.
     */
    public Set<T> getHandledTypes() {
        return getMsgHandler().getHandledTypes();
    }
}
