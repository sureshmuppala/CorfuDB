package org.corfudb.router;

import java.util.concurrent.CompletableFuture;

/**
 * Created by mwei on 11/25/16.
 */
public abstract class AbstractRequestClient<M extends IRoutableMsg<T> & IRespondableMsg,
        T extends IRespondableMsgType> extends AbstractClient<M,T>

{

    public AbstractRequestClient(IRequestClientRouter<M,T> router) {
        super(router);
    }

    /** Get the request router assigned to this client.
     *
     * @return  The request router assigned to this client.
     */
    public IRequestClientRouter<M,T> getRequestRouter() {
        return (IRequestClientRouter<M,T>) router;
    }

    /** Send a message and wait for the response to be returned
     * as a completable future.
     * @param outMsg    The message to send
     * @return          A completable future which will be completed with
     *                  the response, or completed exceptionally if there
     *                  was a communication error.
     */
    protected CompletableFuture<M> sendMessageAndGetResponse(M outMsg) {
        return sendMessageAndGetResponse(outMsg, null);
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
    protected <R extends M> CompletableFuture<R>
    sendMessageAndGetResponse(M outMsg, Class<R> responseType) {
        return getRequestRouter().sendMessageAndGetResponse(outMsg, responseType);
    }
}
