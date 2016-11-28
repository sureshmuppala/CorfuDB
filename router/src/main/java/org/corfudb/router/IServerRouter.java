package org.corfudb.router;

import io.netty.channel.ChannelHandlerContext;

import java.io.Closeable;
import java.util.function.Function;

/**
 * Created by mwei on 11/23/16.
 */
public interface IServerRouter<M extends IRoutableMsg<T>, T> extends AutoCloseable {

    /** Send a message, unsolicited.
     *
     * @param ctx       The context to send the response to.
     * @param outMsg    The outgoing message.
     */
    void sendMessage(ChannelHandlerContext ctx, M outMsg);

    /** Send a message, in response to a message.
     *
     * @param ctx       The context to send the response to.
     * @param inMsg     The message we are responding to.
     * @param outMsg    The outgoing message.
     */
    void sendResponse(ChannelHandlerContext ctx, IRespondableMsg inMsg, IRespondableMsg outMsg);

    /** Register a new server to route messages to, given
     * a function which generates a server from this router.
     * @param serverSupplier    The function which supplies the server.
     * @return                  This server router, to support chaining.
     */
    default IServerRouter<M,T> registerServer(Function<IServerRouter<M,T>, IServer<M,T>> serverSupplier) {
        return registerServer(serverSupplier.apply(this));
    }

    /** Register a new server to route messages to.
     *
     * @param server    The server to route messages to.
     *
     * @return          This server router, to support chaining.
     */
    IServerRouter<M,T> registerServer(IServer<M,T> server);

    /** Return a registered server of the given type.
     *
     * @param serverType    The type of the server to return.
     * @param <R>           The type of the returned server.
     * @return              A server of the given type.
     */
    <R extends IServer<M,T>> R getServer(Class<R> serverType);

    /** Start listening for messages and route to the registered servers.
     *  @return        This server router, to support chaining.
     */
    IServerRouter<M,T> start();

    /** Stop listening for messages and stop routing messages to the registered servers.
     *  @return        This server router, to support chaining.
     */
    IServerRouter<M,T> stop();


    /**
     * Closes this resource, relinquishing any underlying resources.
     * This method is invoked automatically on objects managed by the
     * {@code try}-with-resources statement.
     * <p>
     * <p>While this interface method is declared to throw {@code
     * Exception}, implementers are <em>strongly</em> encouraged to
     * declare concrete implementations of the {@code close} method to
     * throw more specific exceptions, or to throw no exception at all
     * if the close operation cannot fail.
     * <p>
     * <p> Cases where the close operation may fail require careful
     * attention by implementers. It is strongly advised to relinquish
     * the underlying resources and to internally <em>mark</em> the
     * resource as closed, prior to throwing the exception. The {@code
     * close} method is unlikely to be invoked more than once and so
     * this ensures that the resources are released in a timely manner.
     * Furthermore it reduces problems that could arise when the resource
     * wraps, or is wrapped, by another resource.
     * <p>
     * <p><em>Implementers of this interface are also strongly advised
     * to not have the {@code close} method throw {@link
     * InterruptedException}.</em>
     * <p>
     * This exception interacts with a thread's interrupted status,
     * and runtime misbehavior is likely to occur if an {@code
     * InterruptedException} is {@linkplain Throwable#addSuppressed
     * suppressed}.
     * <p>
     * More generally, if it would cause problems for an
     * exception to be suppressed, the {@code AutoCloseable.close}
     * method should not throw it.
     * <p>
     * <p>Note that unlike the {@link Closeable#close close}
     * method of {@link Closeable}, this {@code close} method
     * is <em>not</em> required to be idempotent.  In other words,
     * calling this {@code close} method more than once may have some
     * visible side effect, unlike {@code Closeable.close} which is
     * required to have no effect if called more than once.
     * <p>
     * However, implementers of this interface are strongly encouraged
     * to make their {@code close} methods idempotent.
     *
     * @throws Exception if this resource cannot be closed
     */
    @Override
    default void close() throws Exception {
        stop();
    }
}
