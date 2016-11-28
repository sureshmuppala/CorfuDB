package org.corfudb.router.multiServiceTest;

import lombok.Getter;
import org.corfudb.router.AbstractClient;
import org.corfudb.router.AbstractRequestClient;
import org.corfudb.router.ClientMsgHandler;
import org.corfudb.router.IRequestClientRouter;
import org.corfudb.router.pingTest.PingClientHandler;
import org.corfudb.router.pingTest.PingMsg;
import org.corfudb.router.pingTest.PingMsgType;

import java.lang.invoke.MethodHandles;
import java.util.concurrent.CompletableFuture;

import static org.corfudb.router.multiServiceTest.MultiServiceMsgType.ECHO_REQUEST;

/**
 * Created by mwei on 11/27/16.
 */
public class EchoClient extends AbstractRequestClient<MultiServiceMsg<?>, MultiServiceMsgType> {

    /** Handler for the ping server. */
    @Getter
    private final ClientMsgHandler<MultiServiceMsg<?>, MultiServiceMsgType> msgHandler =
            new ClientMsgHandler<MultiServiceMsg<?>, MultiServiceMsgType>(this)
                    .generateHandlers(MethodHandles.lookup(), this,
                            MultiServiceClientHandler.class, MultiServiceClientHandler::type);


    public EchoClient(IRequestClientRouter<MultiServiceMsg<?>, MultiServiceMsgType> router) {
        super(router);
    }

    public CompletableFuture<String> echo(String toEcho) {
        return sendMessageAndGetResponse(ECHO_REQUEST.getPayloadMsg(toEcho))
                .thenApply(r -> (String)r.getPayload());
    }

}
