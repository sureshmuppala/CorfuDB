package org.corfudb.router.multiServiceTest;

import lombok.Getter;
import org.corfudb.router.AbstractRequestClient;
import org.corfudb.router.ClientMsgHandler;
import org.corfudb.router.IRequestClientRouter;

import java.lang.invoke.MethodHandles;
import java.util.concurrent.CompletableFuture;

import static org.corfudb.router.multiServiceTest.MultiServiceMsgType.GATEWAY_REQUEST;

/**
 * Created by mwei on 11/27/16.
 */
public class GatewayClient extends AbstractRequestClient<MultiServiceMsg<?>, MultiServiceMsgType> {

    @Getter
    private final ClientMsgHandler<MultiServiceMsg<?>, MultiServiceMsgType> msgHandler =
            new ClientMsgHandler<MultiServiceMsg<?>, MultiServiceMsgType>(this)
                    .generateHandlers(MethodHandles.lookup(), this,
                            MultiServiceClientHandler.class, MultiServiceClientHandler::type);

    public GatewayClient(IRequestClientRouter<MultiServiceMsg<?>, MultiServiceMsgType> router) {
        super(router);
    }

    public CompletableFuture<String> getPassword() {
        return sendMessageAndGetResponse(GATEWAY_REQUEST.getVoidMsg())
                .thenApply(x -> (String) x.getPayload());
    }
}
