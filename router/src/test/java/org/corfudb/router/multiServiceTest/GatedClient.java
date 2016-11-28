package org.corfudb.router.multiServiceTest;

import lombok.Getter;
import org.corfudb.router.AbstractPostRequestClient;
import org.corfudb.router.ClientMsgHandler;
import org.corfudb.router.IRequestClientRouter;

import java.lang.invoke.MethodHandles;
import java.util.concurrent.CompletableFuture;

import static org.corfudb.router.multiServiceTest.MultiServiceMsgType.GATED_REQUEST;

/**
 * Created by mwei on 11/28/16.
 */
public class GatedClient extends AbstractPostRequestClient<MultiServiceMsg<?>, MultiServiceMsgType> {

    @Getter
    private final ClientMsgHandler<MultiServiceMsg<?>, MultiServiceMsgType> msgHandler =
            new ClientMsgHandler<MultiServiceMsg<?>, MultiServiceMsgType>(this)
                    .generateHandlers(MethodHandles.lookup(), this,
                            MultiServiceClientHandler.class, MultiServiceClientHandler::type);

    public GatedClient(IRequestClientRouter<MultiServiceMsg<?>, MultiServiceMsgType> router,
                       GatewayClient client) {
        super(router, m -> {
            try {
                m.setPassword(client.getPassword().get());
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
            return m;
        });
    }

    public CompletableFuture<String> gatedEcho(String echoString) {
        return sendMessageAndGetResponse(GATED_REQUEST.getPayloadMsg(echoString))
                .thenApply(x -> (String)x.getPayload());
    }
}
