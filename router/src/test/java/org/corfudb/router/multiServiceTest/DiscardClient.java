package org.corfudb.router.multiServiceTest;

import lombok.Getter;
import org.corfudb.router.AbstractRequestClient;
import org.corfudb.router.ClientMsgHandler;
import org.corfudb.router.IRequestClientRouter;

import java.lang.invoke.MethodHandles;
import java.util.concurrent.CompletableFuture;

import static org.corfudb.router.multiServiceTest.MultiServiceMsgType.DISCARD;
import static org.corfudb.router.multiServiceTest.MultiServiceMsgType.ECHO_REQUEST;

/**
 * Created by mwei on 11/27/16.
 */
public class DiscardClient extends AbstractRequestClient<MultiServiceMsg<?>, MultiServiceMsgType> {

    /** Handler for the discard client. */
    @Getter
    private final ClientMsgHandler<MultiServiceMsg<?>, MultiServiceMsgType> msgHandler =
            new ClientMsgHandler<MultiServiceMsg<?>, MultiServiceMsgType>(this)
                    .generateHandlers(MethodHandles.lookup(), this,
                            MultiServiceClientHandler.class, MultiServiceClientHandler::type);


    public DiscardClient(IRequestClientRouter<MultiServiceMsg<?>, MultiServiceMsgType> router) {
        super(router);
    }

    public void discard(String toDiscard) {
        sendMessage(DISCARD.getPayloadMsg(toDiscard));
    }

}
