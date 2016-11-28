package org.corfudb.router.netty;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.channel.*;

import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.ServerSocketChannel;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.codec.LengthFieldBasedFrameDecoder;
import io.netty.handler.codec.LengthFieldPrepender;
import io.netty.util.concurrent.DefaultEventExecutorGroup;
import io.netty.util.concurrent.EventExecutorGroup;
import lombok.RequiredArgsConstructor;
import lombok.Setter;
import lombok.experimental.Accessors;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.router.IRespondableMsg;
import org.corfudb.router.IRoutableMsg;
import org.corfudb.router.IServer;
import org.corfudb.router.IServerRouter;

import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Supplier;

/**
 * Created by mwei on 11/23/16.
 */
@Slf4j
@ChannelHandler.Sharable
@RequiredArgsConstructor
public class NettyServerRouter<M extends IRoutableMsg<T>, T>
        extends ChannelInboundHandlerAdapter
        implements IServerRouter<M, T> {

    /**
     * This map stores the mapping from message type to server handler.
     */
    final private Map<T, IServer<M,T>> handlerMap = new ConcurrentHashMap<>();

    /** The boss group services incoming connections. */
    private EventLoopGroup bossGroup;

    /** The worker group performs the actual work after getting an incoming message. */
    private EventLoopGroup workerGroup;

    /** The event group handles actual application level logic. */
    private EventExecutorGroup ee;

    /** The future for the channel the server is listening on. */
    private ChannelFuture channelFuture;

    /** The port to listen to incoming requests on. */
    private final int port;

    /** The message decoder. */
    private final Supplier<ChannelHandlerAdapter> decoderSupplier;

    /** The message encoder. */
    private final Supplier<ChannelHandlerAdapter> encoderSupplier;

    /** The type of channel to use. */
    private final Class<? extends ServerSocketChannel> channelType;

    @Accessors(chain=true)
    public static class Builder<M extends IRoutableMsg<T>, T> {
        @Setter
        int port;

        @Setter
        Supplier<ChannelHandlerAdapter> decoderSupplier;

        @Setter
        Supplier<ChannelHandlerAdapter> encoderSupplier;

        @Setter
        Class<? extends ServerSocketChannel> channelType = NioServerSocketChannel.class;

        public NettyServerRouter<M,T> build() {
            return new NettyServerRouter<M, T>(port, decoderSupplier, encoderSupplier, channelType);
        }
    }

    public static <M extends IRoutableMsg<T>, T> Builder<M, T> builder() {return new Builder<>();}

    @Override
    public void sendMessage(ChannelHandlerContext ctx, M outMsg) {
        ctx.writeAndFlush(outMsg);
    }

    /**
     * Send a message, in response to a message.
     *
     * @param ctx    The context to send the response to.
     * @param inMsg  The message we are responding to.
     * @param outMsg The outgoing message.
     */
    @Override
    public void sendResponse(ChannelHandlerContext ctx, IRespondableMsg inMsg, IRespondableMsg outMsg) {
        inMsg.copyFieldsToResponse(outMsg);
        sendMessage(ctx, (M) outMsg);
    }

    /**
     * Handle an incoming message read on the channel.
     *
     * @param ctx Channel handler context
     * @param msg The incoming message on that channel.
     */
    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) {
        try {
            // The incoming message should have been transformed earlier in the pipeline.
            M m = ((M) msg);
            // We get the handler for this message from the map
            IServer<M,T> handler = handlerMap.get(m.getMsgType());
            if (handler == null) {
                // The message was unregistered, we are dropping it.
                log.warn("Received unregistered message {}, dropping", m);
            } else {
                    handler.handleMessage(m, ctx);
            }
        } catch (Exception e) {
            log.error("Exception during read!", e);
        }
    }

    /**
     * Add a new netty server handler to the router.
     *
     * @param server The server to add.
     */
    @Override
    public IServerRouter<M,T> registerServer(IServer<M, T>  server) {
        server.getMsgHandler().getHandledTypes()
                .forEach(x -> {
                    handlerMap.put(x, server);
                    log.trace("Registered {} to handle messages of type {}", server, x);
                });
        return this;
    }

    /**
     * Return a registered server of the given type.
     *
     * @param serverType The type of the server to return.
     * @return A server of the given type.
     */
    @Override
    @SuppressWarnings("unchecked")
    public <R extends IServer<M, T>> R getServer(Class<R> serverType) {
        Optional<IServer<M, T>> ret =  handlerMap.values()
                .stream()
                .filter(serverType::isInstance)
                .findFirst();
        return ret.isPresent() ? (R) ret.get() : null;
    }

    /** Start routing messages. */
    @Override
    public synchronized IServerRouter<M,T> start() {
        // Don't allow a server router to be started twice.
        if (channelFuture != null && channelFuture.channel().isOpen()) {
            throw new RuntimeException("Attempted to start a server with an open channel!");
        }

        bossGroup = new NioEventLoopGroup(1, new NamedThreadFactory("accept"));

        workerGroup = new NioEventLoopGroup(Runtime.getRuntime().availableProcessors() * 2,
                new NamedThreadFactory("io"));

        ee = new DefaultEventExecutorGroup(Runtime.getRuntime().availableProcessors() * 2,
                new NamedThreadFactory("event"));

        try {
            ServerBootstrap b = new ServerBootstrap();
            b.group(bossGroup, workerGroup)
                    .channel(channelType)
                    .option(ChannelOption.SO_BACKLOG, 100)
                    .childOption(ChannelOption.SO_KEEPALIVE, true)
                    .childOption(ChannelOption.SO_REUSEADDR, true)
                    .childOption(ChannelOption.TCP_NODELAY, true)
                    .childOption(ChannelOption.ALLOCATOR, PooledByteBufAllocator.DEFAULT)
                    .childHandler(new ChannelInitializer<SocketChannel>() {
                        @Override
                        public void initChannel(io.netty.channel.socket.SocketChannel ch) throws Exception {
                            ch.pipeline().addLast(new LengthFieldPrepender(4));
                            ch.pipeline().addLast(new LengthFieldBasedFrameDecoder(Integer.MAX_VALUE, 0, 4, 0, 4));
                            ch.pipeline().addLast(ee, decoderSupplier.get());
                            ch.pipeline().addLast(ee, encoderSupplier.get());
                            ch.pipeline().addLast(ee, NettyServerRouter.this);
                        }
                    });

            channelFuture = b.bind(port).sync();
        }  catch (InterruptedException ie) {
            log.error("Netty server router shut down unexpectedly during startup due to interruption", ie);
            throw new RuntimeException("InterruptionException starting Netty server router", ie);
        }
        return this;
    }

    /**
     * Stop listening for messages and stop routing messages to the registered servers.
     */
    @Override
    public synchronized IServerRouter<M,T> stop() {
        // Shutdown the channel, if there is one.
        if (channelFuture != null) {
            channelFuture.channel().close();
        }
        // Shutdown the worker threads, if there are any active.
        if (bossGroup != null) {bossGroup.shutdownGracefully();}
        if (workerGroup != null) {workerGroup.shutdownGracefully();}
        if (ee != null) {ee.shutdownGracefully();}
        // Wait for the channel to shutdown.
        if (channelFuture != null) {
            try {
                channelFuture.channel().closeFuture().sync();
            } catch (InterruptedException ie) {
                log.error("Netty server router shut down unexpectedly during shutdown due to interruption", ie);
                throw new RuntimeException("InterruptionException stopping Netty server router", ie);
            }
        }
        return this;
    }

    /** Catch an exception handling an inbound message type.
     *
     * @param ctx       The context that encountered the error.
     * @param cause     The reason for the error.
     */
    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        log.error("Error in handling inbound message, {}", cause);
        ctx.close();
    }
}
