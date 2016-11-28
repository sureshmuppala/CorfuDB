package org.corfudb.router;

import java.util.concurrent.CompletableFuture;
import java.util.function.Function;

/**
 * Created by mwei on 11/28/16.
 */
public abstract class AbstractPostRequestClient<M extends IRoutableMsg<T> & IRespondableMsg,
        T extends IRespondableMsgType> extends AbstractRequestClient<M,T> {

    final Function<M, M> postTransformationFunction;

    public AbstractPostRequestClient(IRequestClientRouter<M,T> router,
                                     Function<M, M> postTransformationFunction) {
        super(router);
        this.postTransformationFunction = postTransformationFunction;
    }

    /** Send a message, not expecting a response.
     *
     * @param msg   The message to send.
     */
    @Override
    protected void sendMessage(M msg) {
        router.sendMessage(postTransformationFunction.apply(msg));
    }

    /**
     * Send a message and get the response as a completable future,
     * setting the lower bound for the return type.
     *
     * @param outMsg       The message to send.
     * @param responseType The class of the type of the response message.
     * @return A completed future which will be completed with
     * the response, or completed exceptionally if there
     * was a communication error.
     */
    @Override
    protected <R extends M> CompletableFuture<R> sendMessageAndGetResponse(M outMsg, Class<R> responseType) {
        return super.sendMessageAndGetResponse(postTransformationFunction.apply(outMsg), responseType);
    }
}
