package org.corfudb.router;

import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;

/**
 * Created by mwei on 11/24/16.
 */
public interface IRequestClientRouter<M extends IRoutableMsg<T> & IRespondableMsg,
        T extends IRespondableMsgType>
extends IClientRouter<M,T>
{
    /** Get the default timeout for requests made through this client router.
     *
     * @return  The default timeout interval for this client router.
     */
    Duration getDefaultTimeout();

    /** Send a message and wait for the response to be returned
     * as a completable future.
     * @param outMsg    The message to send
     * @return          A completable future which will be completed with
     *                  the response, or completed exceptionally if there
     *                  was a communication error.
     */
    default CompletableFuture<M> sendMessageAndGetResponse(M outMsg) {
        return sendMessageAndGetResponse(outMsg, getDefaultTimeout());
    }

    /** Send a message and get the response as a completable future,
     * setting the lower bound for the return type.
     * @param outMsg        The message to send.
     * @param responseType  The class of the type of the response message.
     * @param <R>           The lower bound for the response message type.
     * @return              A completed future which will be completed with
     *                      the response, or completed exceptionally if there
     *                      was a communication error.
     */
    @SuppressWarnings("unchecked")
    default <R extends M> CompletableFuture<R>
    sendMessageAndGetResponse(M outMsg, Class<R> responseType) {
        return (CompletableFuture<R>) sendMessageAndGetResponse(outMsg);
    }

    /** Send a message and get a response as a completable future,
     * timing out if the response isn't received in the given amount of time.
     * @param outMsg        The message to send.
     * @param timeout       The maximum amount of time to wait for a response
     *                      before timing out with a TimeoutException.
     * @return              A completable future which will be completed with
     *                      the response, or completed exceptionally if there
     *                      was a communication error.
     */
    CompletableFuture<M> sendMessageAndGetResponse(M outMsg, Duration timeout);

    /** Send a mesaage and get a response as a completable future,
     * timing out if the response isn't received in the given amount of time
     * and setting the lower bound for the return type.
     * @param outMsg            The message to send.
     * @param responseType      The lower bound for the message which will be
     *                          returned.
     * @param timeout           The maximum amount of time to wait for a response
     *                          before timing out with a TimeoutException.
     * @param <R>               The type of the lower bound for the return.
     * @return                  A completable future which will be completed with
     *                          the response, or completed exceptionally if there
     *                          was a communication error
     */
    @SuppressWarnings("unchecked")
    default <R extends M> CompletableFuture<R>
    sendMessageAndGetResponse(M outMsg, Class<R> responseType, Duration timeout) {
        return (CompletableFuture<R>) sendMessageAndGetResponse(outMsg, timeout);
    }

    /** Register a client with this router, supplying
     * the router itself in the process.
     */
    default IRequestClientRouter<M,T> registerRequestClient(
            Function<IRequestClientRouter<M, T>, IClient<M,T>> clientSupplier) {
        registerClient(clientSupplier.apply(this));
        return this;
    }
}
