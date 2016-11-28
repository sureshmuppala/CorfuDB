package org.corfudb.router;

import org.corfudb.AbstractCorfuTest;
import org.corfudb.exceptions.DisconnectedException;
import org.corfudb.exceptions.ErrorResponseException;
import org.corfudb.router.multiServiceTest.*;
import org.corfudb.router.netty.NettyClientRouter;
import org.corfudb.router.netty.NettyMsgDecoder;
import org.corfudb.router.netty.NettyMsgEncoder;
import org.corfudb.router.netty.NettyServerRouter;
import org.corfudb.router.pingTest.*;
import org.junit.Test;

import java.io.IOException;
import java.net.ServerSocket;
import java.time.Duration;
import java.util.concurrent.CompletableFuture;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Created by mwei on 11/23/16.
 */
public class NettyServerRouterTest extends AbstractCorfuTest {

    private static Integer findRandomOpenPort() throws IOException {
        try (
                ServerSocket socket = new ServerSocket(0);
        ) {
            return socket.getLocalPort();
        }
    }

    @Test
    public void canPing() throws Exception {
        final int port = findRandomOpenPort();
        // Get a server router
        try (IServerRouter<PingMsg, PingMsgType> serverRouter =
                NettyServerRouter.<PingMsg, PingMsgType>builder()
                .setPort(port)
                .setDecoderSupplier(() -> new NettyMsgDecoder<>(PingMsg::decode))
                .setEncoderSupplier(NettyMsgEncoder::new)
                .build()
                .registerServer(PingServer::new)
                .start()) {
            // Get a client router
            try (IClientRouter<PingMsg, PingMsgType> clientRouter =
                    NettyClientRouter.<PingMsg, PingMsgType>builder()
                    .setPort(port)
                    .setHost("localhost")
                    .setDecoderSupplier(() -> new NettyMsgDecoder<>(PingMsg::decode))
                    .setEncoderSupplier(NettyMsgEncoder::new)
                    .build()
                    .registerRequestClient(PingClient::new)
                    .start()) {
                        assertThat(clientRouter.getClient(PingClient.class).ping().get())
                                .isTrue();
                }
        }
    }

    @Test
    public void unconnectedClientThrowsException() throws Exception {
        IClientRouter<PingMsg, PingMsgType> clientRouter =
                NettyClientRouter.<PingMsg, PingMsgType>builder()
                        .setPort(findRandomOpenPort())
                        .setHost("localhost")
                        .setDecoderSupplier(() -> new NettyMsgDecoder<>(PingMsg::decode))
                        .setEncoderSupplier(NettyMsgEncoder::new)
                        .build()
                        .registerRequestClient(PingClient::new);

        assertThatThrownBy(() -> clientRouter.getClient(PingClient.class).ping().get())
                .hasCauseInstanceOf(DisconnectedException.class);
    }

    @Test
    public void disconnectedClientThrowsException() throws Exception {
        final int port = findRandomOpenPort();
        // Get a server router
        try (IServerRouter<PingMsg, PingMsgType> serverRouter =
                     NettyServerRouter.<PingMsg, PingMsgType>builder()
                             .setPort(port)
                             .setDecoderSupplier(() -> new NettyMsgDecoder<>(PingMsg::decode))
                             .setEncoderSupplier(NettyMsgEncoder::new)
                             .build()
                             .registerServer(PingServer::new)
                             .start()) {
            // Get a client router
            try (IClientRouter<PingMsg, PingMsgType> clientRouter =
                         NettyClientRouter.<PingMsg, PingMsgType>builder()
                                 .setPort(port)
                                 .setHost("localhost")
                                 .setDecoderSupplier(() -> new NettyMsgDecoder<>(PingMsg::decode))
                                 .setEncoderSupplier(NettyMsgEncoder::new)
                                 .setAutomaticallyReconnect(false)
                                 .build()
                                 .registerRequestClient(PingClient::new)
                                 .start()) {
                // Stop the server after a connection
                serverRouter.stop();
                // Get and ping, should throw disconnected exception.
                assertThatThrownBy(() -> clientRouter.getClient(PingClient.class).ping().get())
                        .hasCauseInstanceOf(DisconnectedException.class);
            }
        }
    }

    @Test
    public void messagesSentAfterDisconnectAreResent() throws Exception {
        final int port = findRandomOpenPort();
        // Get a server router
        try (IServerRouter<PingMsg, PingMsgType> serverRouter =
                     NettyServerRouter.<PingMsg, PingMsgType>builder()
                             .setPort(port)
                             .setDecoderSupplier(() -> new NettyMsgDecoder<>(PingMsg::decode))
                             .setEncoderSupplier(NettyMsgEncoder::new)
                             .build()
                             .registerServer(PingServer::new)
                             .start()) {
            // A completable future we will use to monitor disconnection.
            CompletableFuture<Boolean> disconnectFuture = new CompletableFuture<>();
            // Get a client router
            try (IClientRouter<PingMsg, PingMsgType> clientRouter =
                         NettyClientRouter.<PingMsg, PingMsgType>builder()
                                 .setPort(port)
                                 .setHost("localhost")
                                 .setDecoderSupplier(() -> new NettyMsgDecoder<>(PingMsg::decode))
                                 .setEncoderSupplier(NettyMsgEncoder::new)
                                 .setDefaultTimeout(Duration.ofSeconds(1))
                                 .setDisconnectFunction(r -> disconnectFuture.complete(true))
                                 .build()
                                 .registerRequestClient(PingClient::new)
                                 .start()) {
                // Stop the server after a connection
                serverRouter.stop();
                // Wait for the client to become disconnected.
                disconnectFuture.get();
                // Try to ping, asynchronously.
                CompletableFuture<Boolean> pingResult =
                        clientRouter.getClient(PingClient.class).ping();
                // Restart the server
                serverRouter.start();
                // The ping should be successful
                assertThat(pingResult.get())
                        .isTrue();
            }
        }
    }


    @Test
    public void multipleServicesCorrectlyRoute() throws Exception {
        final int port = findRandomOpenPort();
        // Get a server router
        try (IServerRouter<MultiServiceMsg<?>, MultiServiceMsgType> serverRouter =
                     NettyServerRouter.<MultiServiceMsg<?>, MultiServiceMsgType>builder()
                             .setPort(port)
                             .setDecoderSupplier(() -> new NettyMsgDecoder<>(MultiServiceMsg::decode))
                             .setEncoderSupplier(NettyMsgEncoder::new)
                             .build()
                             .registerServer(EchoServer::new)
                             .registerServer(DiscardServer::new)
                             .registerServer(x -> new GatewayServer(x, "open sesame"))
                             .registerServer(x -> new GatedServer(x, x.getServer(GatewayServer.class)))
                             .start()) {
            // Get a client router
            try (IClientRouter<MultiServiceMsg<?>, MultiServiceMsgType> clientRouter =
                         NettyClientRouter.<MultiServiceMsg<?>, MultiServiceMsgType>builder()
                                 .setPort(port)
                                 .setHost("localhost")
                                 .setDecoderSupplier(() -> new NettyMsgDecoder<>(MultiServiceMsg::decode))
                                 .setEncoderSupplier(NettyMsgEncoder::new)
                                 .setDefaultTimeout(Duration.ofSeconds(5))
                                 .build()
                                 .registerRequestClient(EchoClient::new)
                                 .registerRequestClient(DiscardClient::new)
                                 .registerRequestClient(GatewayClient::new)
                                 .registerRequestClient(x -> new GatedClient(x, x.getClient(GatewayClient.class)))
                                 .start()) {

                // Test if we get an echo back from the echo sever.
                assertThat(clientRouter.getClient(EchoClient.class).echo("hello world!").get())
                        .isEqualTo("hello world!");

                // Make sure we can discard a message.
                clientRouter.getClient(DiscardClient.class).discard("abc");
                assertThat(serverRouter.getServer(DiscardServer.class).getDiscarded().get())
                        .isTrue();

                // And check if we can get a "gated" echo
                assertThat(clientRouter.getClient(GatedClient.class).gatedEcho("hello world!").get())
                        .isEqualTo("hello world!");
            }
        }
    }

    @Test
    public void preconditionFailureTest() throws Exception {
        final int port = findRandomOpenPort();
        // A gateway server which will provide the secret to the gated server
        GatewayServer gs = new GatewayServer(null, "open sesame");
        // Get a server router
        try (IServerRouter<MultiServiceMsg<?>, MultiServiceMsgType> serverRouter =
                     NettyServerRouter.<MultiServiceMsg<?>, MultiServiceMsgType>builder()
                             .setPort(port)
                             .setDecoderSupplier(() -> new NettyMsgDecoder<>(MultiServiceMsg::decode))
                             .setEncoderSupplier(NettyMsgEncoder::new)
                             .build()
                             .registerServer(x -> new GatewayServer(x, "wrong password")) //serve the wrong password.
                             .registerServer(x -> new GatedServer(x, gs)) //get the password from the other gs
                             .start()) {
            // Get a client router
            try (IClientRouter<MultiServiceMsg<?>, MultiServiceMsgType> clientRouter =
                         NettyClientRouter.<MultiServiceMsg<?>, MultiServiceMsgType>builder()
                                 .setPort(port)
                                 .setHost("localhost")
                                 .setDecoderSupplier(() -> new NettyMsgDecoder<>(MultiServiceMsg::decode))
                                 .setEncoderSupplier(NettyMsgEncoder::new)
                                 .setDefaultTimeout(Duration.ofSeconds(5))
                                 .build()
                                 .registerRequestClient(GatewayClient::new)
                                 .registerRequestClient(x -> new GatedClient(x, x.getClient(GatewayClient.class)))
                                 .start()) {

                // And check if we can get a "gated" echo
                assertThatThrownBy(() -> clientRouter.getClient(GatedClient.class).gatedEcho("hello world!").get())
                        .hasCauseInstanceOf(ErrorResponseException.class);
            }
        }
    }
}
