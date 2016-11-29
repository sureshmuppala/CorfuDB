package org.corfudb.router.netty;

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.codec.LengthFieldBasedFrameDecoder;
import io.netty.handler.codec.LengthFieldPrepender;
import io.netty.util.concurrent.DefaultEventExecutorGroup;
import io.netty.util.concurrent.EventExecutorGroup;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.Setter;
import lombok.experimental.Accessors;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.exceptions.DisconnectedException;
import org.corfudb.exceptions.ErrorResponseException;
import org.corfudb.exceptions.NetworkException;
import org.corfudb.router.*;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;

import static java.time.temporal.ChronoUnit.SECONDS;

/**
 * Created by mwei on 11/23/16.
 */
@Slf4j
@RequiredArgsConstructor
@ChannelHandler.Sharable
public class NettyClientRouter<M extends IRoutableMsg<T> & IRespondableMsg, T extends IRespondableMsgType>
        implements IRequestClientRouter<M, T> {

    /** A map of client class to clients. */
    private final Map<Class<? extends IClient<M,T>>, IClient<M,T>> clientMap =
            new ConcurrentHashMap<>();

    /** A map of message types to clients. */
    private final Map<T, IClient<M,T>> handlerMap =
            new ConcurrentHashMap<>();

    /** A map of completions, for request-response messages. */
    private final Map<Long, CompletableFuture<M>> completionMap =
            new ConcurrentHashMap<>();

    /** A counter for requests sent down the currently registered channel context. */
    private final AtomicLong requestCounter = new AtomicLong(0);

    /**
     * The worker group for this router.
     */
    private EventLoopGroup workerGroup;

    /**
     * The event executor group for this router.
     */
    private EventExecutorGroup ee;

    /**
     * The currently registered channel.
     */
    private Channel channel;

    /**
     * The currently registered handler for the channel.
     */
    private NettyClientChannelHandler channelHandler;

    /**
     * Whether or not this client has been started or is stopped.
     */
    private volatile boolean started = false;

    /** A scheduled executor service for timing out
     * requests.
     */
    private static final ScheduledExecutorService timeoutScheduler =
            Executors.newScheduledThreadPool(
                    1,
                    new NamedThreadFactory("timeout"));

    /** A queue of messages queued during a reconnect.
     *
     */
    private final Queue<M> queuedMessages = new ConcurrentLinkedQueue<>();

    /**
     * The hostname of the server we should connect to.
     */
    @Getter
    private final String host;

    /**
     * The port of the server we should connect to.
     */
    @Getter
    private final int port;

    /** The message decoder. */
    private final Supplier<ChannelHandlerAdapter> decoderSupplier;

    /** The message encoder. */
    private final Supplier<ChannelHandlerAdapter> encoderSupplier;

    /** The default amount of time to wait before timing out requests. */
    @Getter
    private final Duration defaultTimeout;

    /** Whether or not to automatically reconnect on failed connection. */
    private final boolean automaticallyReconnect;

    /** A function to execute before reconnecting which gets this router and
     *  returns true to continue reconnecting, or false to shutdown.
     */
    private final Function<NettyClientRouter<M,T>, Boolean> reconnectFunction;

    /** Whether or not to queue messages on a failed connection, or to
     * immediately throw a disconnected exception. Note that this does not
     * affect messages in flight - any messages which have already been
     * sent but not responded to will throw a disconnected exception
     * because we will obtain a different channel.
     */
    private final boolean queueMessagesOnFailure;

    /** A function which executes whenever this router gets disconnected unexpectedly.
     *
     */
    private final Consumer<NettyClientRouter<M,T>> disconnectedFunction;

    /** This builder class sets defaults for the client. */
    @Accessors(chain=true)
    public static class Builder<M extends IRoutableMsg<T> & IRespondableMsg, T extends IRespondableMsgType> {
        @Setter
        String endpoint;

        @Setter
        String host;

        @Setter
        int port;

        @Setter
        Supplier<ChannelHandlerAdapter> decoderSupplier;

        @Setter
        Supplier<ChannelHandlerAdapter> encoderSupplier;

        @Setter
        Duration defaultTimeout = Duration.of(5, SECONDS);

        @Setter
        boolean automaticallyReconnect = true;

        // By default, we wait one fourth default timeout time to reconnect,
        // and try forever. This ensures that we try to reconnect before
        // any futures timeout.
        @Setter
        Function<NettyClientRouter<M,T>, Boolean> reconnectFunction = (r) ->
        {
            try {
                Thread.sleep(defaultTimeout.toMillis() / 4);
            } catch (InterruptedException ie) {
                return false; // abort reconnecting if interrupted
            }
            return true;
        };

        @Setter
        boolean queueMessageOnFailure = true;

        @Setter
        Consumer<NettyClientRouter<M,T>> disconnectFunction = r -> {};

        public NettyClientRouter<M,T> build() {
            if (endpoint != null ) {
                host = endpoint.split(":")[0];
                port = Integer.parseInt(endpoint.split(":")[1]);
            }
            return new NettyClientRouter<M, T>(host, port, decoderSupplier, encoderSupplier,
                    defaultTimeout, automaticallyReconnect, reconnectFunction, queueMessageOnFailure,
                    disconnectFunction);
        }
    }

    public static <M extends IRoutableMsg<T> & IRespondableMsg, T extends IRespondableMsgType> Builder<M, T>
        builder() {return new Builder<>();}



    /**
     * Retrieve the first client of a given type registered to the router.
     *
     * @param clientType    The type of the client to retrieve.
     */
    @Override
    @SuppressWarnings("unchecked")
    public <C extends IClient<M,T>> C getClient(Class<C> clientType) {
        return (C) clientMap.get(clientType);
    }

    /**
     * Register a client with this router.
     *
     * @param client    The client to register.
     */
    @Override
    @SuppressWarnings("unchecked")
    public IClientRouter<M, T> registerClient(IClient<M, T> client) {
        client.getHandledTypes().forEach(x -> handlerMap.put(x, client));
        clientMap.put((Class<? extends IClient<M,T>>)client.getClass(), client);
        return this;
    }

    /**
     * Asynchronously send a message to the server.
     *
     * @param msg   The message to send to the server.
     */
    @Override
    public void sendMessage(M msg) {
        if (channel != null && channel.isActive()) {
            try {
                channel.writeAndFlush(msg);
            } catch (Exception e) {
                if (automaticallyReconnect && queueMessagesOnFailure) {
                    queuedMessages.add(msg);
                    return;
                }
                throw new NetworkException(getEndpoint(), e);
            }
        }
        else {
            if (started && automaticallyReconnect && queueMessagesOnFailure) {
                queuedMessages.add(msg);
                return;
            }
            throw new DisconnectedException(getEndpoint());
        }
    }

    /**
     * Connects to a server endpoint and starts routing client
     * requests, as well as server messages to clients.
     */
    @Override
    public synchronized IClientRouter<M, T> start() {
        // If the worker group and the event group already exist, try to
        // reuse them, otherwise, create new pools.

        workerGroup =
                workerGroup != null && !workerGroup.isShutdown() ? workerGroup :
                new NioEventLoopGroup(Runtime.getRuntime().availableProcessors() * 2,
                new NamedThreadFactory("io"));

        ee =
                ee != null && !ee.isShutdown() ? ee :
                new DefaultEventExecutorGroup(Runtime.getRuntime().availableProcessors() * 2,
                new NamedThreadFactory("event"));

        Bootstrap b = new Bootstrap();
        b.group(workerGroup);
        b.channel(NioSocketChannel.class);
        b.option(ChannelOption.SO_KEEPALIVE, true);
        b.option(ChannelOption.SO_REUSEADDR, true);
        b.option(ChannelOption.TCP_NODELAY, true);

        final NettyClientChannelHandler handler = new NettyClientChannelHandler();
        b.handler(new ChannelInitializer<SocketChannel>() {
            @Override
            public void initChannel(SocketChannel ch) throws Exception {
                ch.pipeline().addLast(new LengthFieldPrepender(4));
                ch.pipeline().addLast(new LengthFieldBasedFrameDecoder(Integer.MAX_VALUE, 0, 4, 0, 4));
                ch.pipeline().addLast(ee, decoderSupplier.get());
                ch.pipeline().addLast(ee, encoderSupplier.get());
                ch.pipeline().addLast(ee, handler);
            }
        });

        try {
            ChannelFuture cf = b.connect(host, port);
            cf.syncUninterruptibly();
            channel = cf.channel();
            channel.closeFuture().addListener(this::channelClosed);
            channelHandler = handler;
            started = true;
        } catch (Exception e) {
            // No need to change the state of started,
            // If we were previously started, we are still started (retry)
            // If we were not previously started, then we will not start.
            throw new NetworkException(host + ":" + port, e);
        }

        return this;
    }


    /**
     * Disconnects from a server endpoint and stops routing client
     * requests, as well as server messages to clients.
     */
    @Override
    public synchronized IClientRouter<M, T> stop() {
        started = false;

        // Close the channel, if it is open.
        if (channel != null && channel.isOpen()) {
            channel.close();
        }
        // Shutdown all worker thread pools
        if (workerGroup != null) { workerGroup.shutdownGracefully(); }
        if (ee != null) {ee.shutdownGracefully();}
        // Wait for the channel to close.
        if (channel != null) {
            try {
                channel.closeFuture().sync();
            } catch (InterruptedException ie) {
                log.warn("Unexpected InterruptedException while shutting down netty client router", ie);
                throw new RuntimeException("Interrupted while trying to shutdown netty client router", ie);
            }
        }
        return this;
    }

    private void channelClosed(Future<?> channelFuture) {
        // Cleanup any outstanding messages by causing them to
        // throw a disconnected exception.
        final DisconnectedException de = new DisconnectedException(getEndpoint());
        completionMap.values().parallelStream()
                .forEach(x -> x.completeExceptionally(de));
        completionMap.clear();

        // If we didn't expect this channel to unregister (the router was not stopped),
        // log a WARN level message
        if (started) {
            log.warn("Unexpected disconnection from endpoint " + getEndpoint() + "!");
            // Fire the disconnected function.
            disconnectedFunction.accept(this);
            // If the client router has been configured to automatically reconnect, handle it.
            if (automaticallyReconnect) {
                while (reconnectFunction.apply(this)) {
                    try {
                        log.info("Trying to reconnect to endpoint " + getEndpoint());
                        start();
                        return;
                    } catch (Exception e) {
                        log.warn("Reconnecting to endpoint " + getEndpoint() + " failed!");
                    }
                }
                // The reconnect function asked us to exit, try stopping to cleanup
                // anything left over from the reconnect loop.
                stop();
            } else {
                stop();
            }
        }
    }


    /**
     * Send a message and get a response as a completable future,
     * timing out if the response isn't received in the given amount of time.
     *
     * @param outMsg  The message to send.
     * @param timeout The maximum amount of time to wait for a response
     *                before timing out with a TimeoutException.
     * @return A completable future which will be completed with
     * the response, or completed exceptionally if there
     * was a communication error.
     */
    @Override
    public CompletableFuture<M> sendMessageAndGetResponse(M outMsg, Duration timeout) {
        final CompletableFuture<M> future = new CompletableFuture<>();
        final long requestID = requestCounter.getAndIncrement();
        outMsg.setRequestID(requestID);
        completionMap.put(requestID, future);
        try {
            sendMessage(outMsg);
        } catch (Exception e) {
            // If an error occurs during the send of the request
            // remove it from the completion map and immediately
            // return an exceptionally completed future.
            future.completeExceptionally(e);
            completionMap.remove(requestID);
            return future;
        }
        return within(future, timeout, requestID);
    }

    private class NettyClientChannelHandler
        extends SimpleChannelInboundHandler {

        /** The currently registered channel handler context. */
        ChannelHandlerContext context;
        IChannel<M> channel;

        /**
         * Read a message in the channel
         *
         * @param channelHandlerContext The channel context the message is coming from.
         * @param m                     The message read from the channel.
         * @throws Exception An exception thrown during channel processing.
         */
        @Override
        @SuppressWarnings("unchecked")
        protected void channelRead0(ChannelHandlerContext channelHandlerContext, Object m) throws Exception {
            final M message = (M) m;
            if (message.getMsgType().isResponse()) {
                final CompletableFuture<M> future = completionMap.get(message.getRequestID());
                if (future != null) {
                    completionMap.remove(message.getRequestID());
                    if (message.getMsgType().isError()) {
                        future.completeExceptionally(new ErrorResponseException(message));
                    } else {
                        future.complete(message);
                    }
                } else {
                    log.error("Received a response for a message we don't have a completion for! " +
                            "Request ID={}", message.getRequestID());
                }
            } else {
                final IClient<M, T> client = handlerMap.get(message.getMsgType());
                if (client != null) {
                    client.handleMessage(message, channel);
                }
            }
        }


        /**
         * Called when the channel is registered (i.e., when a remote endpoint
         * connects.  Since only one bootstrap should use this channel, channelUnregistered must
         * be called before this method will be called again.
         *
         * @param ctx The context of the channel that was registered.
         * @throws Exception An exception thrown during the registration of this
         *                   channel.
         */
        @Override
        public void channelRegistered(ChannelHandlerContext ctx) throws Exception {
            super.channelRegistered(ctx);
            log.debug("Registered new channel {}", ctx);
        }

        /**
         * Called when the channel is unregistered (i.e., the remote endpoint
         * disconnects. Since only one bootstrap should use this channel, channelRegistered must
         * be called before this method is called.
         *
         * @param ctx The context for the channel which was disconnected.
         * @throws Exception An exception thrown during the disconnection of the channel.
         */
        @Override
        public void channelUnregistered(ChannelHandlerContext ctx) throws Exception {
            log.debug("Unregistered channel {}", ctx);
        }

        @Override
        public void channelActive(ChannelHandlerContext ctx) throws Exception {
            super.channelActive(ctx);
            context = ctx;
            channel = new NettyChannelWrapper<M>(ctx);
            log.debug("Channel {} active", ctx);
            if (queueMessagesOnFailure && queuedMessages.size() > 0) {
                log.info("Resending {} queued messages", queuedMessages.size());
                M msg;
                while ((msg = queuedMessages.poll()) != null) {
                    sendMessage(msg);
                }
            }
        }
    }

    /**
     * Generates a completable future which times out. Removes the request from the completion
     * table on timeout.
     * inspired by NoBlogDefFound: http://www.nurkiewicz.com/2014/12/asynchronous-timeouts-with.html
     *
     * @param duration  The duration to timeout after.
     * @param requestID The ID of the request.
     * @param <T>       Ignored, since the future will always timeout.
     * @return A completable future that will time out.
     */
    private <T> CompletableFuture<T> timeoutFuture(final Duration duration, final long requestID) {
        final CompletableFuture<T> promise = new CompletableFuture<>();
        timeoutScheduler.schedule(() -> {
            final TimeoutException ex = new TimeoutException("Timeout after " + duration.toMillis() + " ms");
            completionMap.remove(requestID);
            return promise.completeExceptionally(ex);
        }, duration.toMillis(), TimeUnit.MILLISECONDS);
        return promise;
    }

    /**
     * Takes a completable future, and ensures that it completes within a certain duration. If it does
     * not, it is cancelled and completes exceptionally with TimeoutException, and removes the request
     * from the completion table.
     * inspired by NoBlogDefFound: http://www.nurkiewicz.com/2014/12/asynchronous-timeouts-with.html
     *
     * @param future   The completable future that must be completed within duration.
     * @param duration The duration the future must be completed in.
     * @param <T>      The return type of the future.
     * @return A completable future which completes with the original value if completed within
     * duration, otherwise completes exceptionally with TimeoutException.
     */
    private <T> CompletableFuture<T> within(CompletableFuture<T> future, Duration duration, long requestID) {
        final CompletableFuture<T> timeout = timeoutFuture(duration, requestID);
        return future.applyToEither(timeout, Function.identity());
    }

    /**
     * Get the name of the endpoint this router is connected to.
     * @return The name of the endpoint the router is connected to.
     */
    private String getEndpoint() {
        return host + ":" + port;
    }
}
