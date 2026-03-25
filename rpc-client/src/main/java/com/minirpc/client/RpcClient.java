package com.minirpc.client;

import com.minirpc.core.codec.netty.RpcMessageDecoder;
import com.minirpc.core.codec.netty.RpcMessageEncoder;
import com.minirpc.core.protocol.RpcRequest;
import com.minirpc.core.protocol.RpcResponse;
import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.codec.LengthFieldBasedFrameDecoder;
import io.netty.handler.codec.LengthFieldPrepender;

import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicLong;
// RpcClient 的核心职责是维护连接、发送请求、按 requestId 关联响应，并处理超时、断连和连接复用等客户端治理问题
public class RpcClient {// 站在调用方视角，把“本地方法调用请求”发出去，并把响应接回来, 关心的是“怎么把请求发出去，并把响应收回来”。
    // 单条消息最大长度：10MB。防止恶意包或异常包导致内存被打爆。
    private static final int MAX_FRAME_LENGTH = 10 * 1024 * 1024;
    // 请求等待响应的超时时间。默认 5 秒；调试时可通过 JVM 参数覆盖：
    // -Drpc.client.timeout.seconds=120
    private static final long RESPONSE_TIMEOUT_SECONDS = Long.getLong("rpc.client.timeout.seconds", 5L);
    // 断连时最多重连次数。
    private static final int MAX_RECONNECT_RETRIES = 3;
    // 每次重连失败后的退避时间，避免瞬时重连风暴。
    private static final long RECONNECT_BACKOFF_MILLIS = 300;

    private final String host;
    private final int port;
    private final EventLoopGroup group;
    private final Bootstrap bootstrap;// 客户端启动器和连接模板

    // 核心映射：requestId -> future。并发场景下用来做请求响应精确配对。
    private final Map<String, CompletableFuture<RpcResponse>> pendingRequests = new ConcurrentHashMap<>();// 挂起请求表，Day3 核心
    private final RpcClientResponseHandler responseHandler;
    private final Object connectLock = new Object();// 避免并发重复建连

    // 下面四个指标是 Day3 增加的统计指标，方便快速判断调用健康度。
    private final AtomicLong totalRequests = new AtomicLong();
    private final AtomicLong successRequests = new AtomicLong();
    private final AtomicLong failedRequests = new AtomicLong();
    private final AtomicLong timeoutRequests = new AtomicLong();
    private volatile Channel channel;// 让多个线程读取 channel 时能看到“最新值”（可见性）
    // 但它不保证复合操作原子性 ，所以代码里还需要 synchronized (connectLock) 配合。

    public RpcClient(String host, int port) {
        this.host = host;
        this.port = port;
        // 客户端一个 event loop 线程先满足当前学习和 demo 场景。
        this.group = new NioEventLoopGroup(1);// 只创建一次 Netty I/O 线程池
        // 传入 markDisconnected 回调：当通道失效时把本地 channel 标记为空，便于后续触发重连。
        this.responseHandler = new RpcClientResponseHandler(pendingRequests, this::markDisconnected);// 共享同一张表
        this.bootstrap = createBootstrap();
    }

    public RpcResponse sendRequest(RpcRequest request) {
        // 1) 统计总请求量。
        totalRequests.incrementAndGet();
        // 2) 每个请求先创建一个 future 放入映射表，再发请求。
        // 这样即使响应非常快返回，也不会发生“响应先到但映射还没建立”的竞态问题。
        CompletableFuture<RpcResponse> future = new CompletableFuture<>();
        pendingRequests.put(request.getRequestId(), future);// 请求入映射表
        try {
            // 3) 确保连接可用（复用或重连）。
            Channel activeChannel = ensureConnected();
            // 4) 出站发送请求对象，Netty pipeline 会负责编码与封帧。
            activeChannel.writeAndFlush(request).sync();// 发请求，把 RpcRequest 对象交给 Netty pipeline 出站。
            // 5) 同步等待该 requestId 对应的 future 完成（由响应处理器回填）。
            RpcResponse response = future.get(RESPONSE_TIMEOUT_SECONDS, TimeUnit.SECONDS);// 这句会一直等，直到有别的线程把 future 完成。即等待 future.complete(msg);
            // 6) 统计成功。
            successRequests.incrementAndGet();
            return response;
        } catch (TimeoutException e) {
            // 超时后清理映射，避免内存泄漏。
            pendingRequests.remove(request.getRequestId());
            timeoutRequests.incrementAndGet();
            failedRequests.incrementAndGet();
            throw new RuntimeException("远程调用超时", e);
        } catch (ExecutionException e) {
            // 这里代表 future 被异常完成（如断连、handler 异常等）。
            pendingRequests.remove(request.getRequestId());
            failedRequests.incrementAndGet();
            throw new RuntimeException("远程调用失败", e.getCause());
        } catch (InterruptedException e) {
            // 保留线程中断语义，避免吞掉中断信号。
            pendingRequests.remove(request.getRequestId());
            failedRequests.incrementAndGet();
            Thread.currentThread().interrupt();
            throw new RuntimeException("远程调用失败", e);
        } catch (RuntimeException e) {
            // 兜底分支，保证所有异常路径都做映射清理和失败计数。
            pendingRequests.remove(request.getRequestId());
            failedRequests.incrementAndGet();
            throw e;
        }
    }

    public void shutdown() {
        Channel localChannel = channel;
        // 优先关闭连接，再关闭线程组，释放网络和线程资源。
        if (localChannel != null && localChannel.isActive()) {
            localChannel.close();
        }
        group.shutdownGracefully();
    }

    public ClientStats getStats() {
        // 返回快照，便于 demo/压测时读取统计数据。
        return new ClientStats(
                totalRequests.get(),
                successRequests.get(),
                failedRequests.get(),
                timeoutRequests.get()
        );
    }

    private Bootstrap createBootstrap() {
        // 创建一次 Bootstrap，后续连接复用同一个配置。
        Bootstrap newBootstrap = new Bootstrap();
        newBootstrap.group(group)
                .channel(NioSocketChannel.class)
                .option(ChannelOption.TCP_NODELAY, true)
                .handler(new ChannelInitializer<SocketChannel>() {
                    @Override
                    protected void initChannel(SocketChannel ch) {
                        ChannelPipeline pipeline = ch.pipeline();
                        // 入站：先按长度拆帧，解决粘包半包问题。
                        pipeline.addLast(new LengthFieldBasedFrameDecoder(MAX_FRAME_LENGTH, 0, 4, 0, 4));
                        // 入站：字节 -> Java 对象。
                        pipeline.addLast(new RpcMessageDecoder());

                        // 出站：Java 对象 -> 字节后，在前面补长度头。
                        pipeline.addLast(new LengthFieldPrepender(4));
                        pipeline.addLast(new RpcMessageEncoder());

                        // 入站业务处理：按 requestId 回填对应 future。
                        pipeline.addLast(responseHandler);// 也就是把 RpcClientResponseHandler 注册进去。
                    }
                });
        return newBootstrap;
    }

    private Channel ensureConnected() {// 多个业务线程会并发调用 sendRequest ，都会进 ensureConnected()
        Channel localChannel = channel;
        // 快速路径：已有可用连接直接复用。
        if (localChannel != null && localChannel.isActive()) {
            return localChannel;
        }
        // 慢速路径：加锁避免并发请求同时触发重复建连。
        synchronized (connectLock) { // 保证“同一时刻只有一个线程负责建连接”---------------------------------------------------------
            localChannel = channel;
            if (localChannel != null && localChannel.isActive()) {
                return localChannel;
            }
            // 断连后重试连接，最多尝试 MAX_RECONNECT_RETRIES 次。
            for (int attempt = 1; attempt <= MAX_RECONNECT_RETRIES; attempt++) {
                try {
                    Channel newChannel = bootstrap.connect(host, port).sync().channel();
                    // 连接关闭后清空本地引用，确保下一次请求会触发重连逻辑。
                    newChannel.closeFuture().addListener(future -> markDisconnected());
                    channel = newChannel;// 新建连接成功赋值,断连时把 channel 清空
                    return channel;
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new RuntimeException("连接服务端失败", e);
                } catch (Exception e) {
                    if (attempt == MAX_RECONNECT_RETRIES) {
                        throw new RuntimeException("连接服务端失败", e);
                    }
                    // 退避等待后再重试。
                    sleepBackoff();
                }
            }
            throw new RuntimeException("连接服务端失败");
        }
    }

    private void markDisconnected() {// 标记 channel 断连
        // 置空表示“当前无可用连接”。
        channel = null;
    }

    private void sleepBackoff() {// // 每次重连失败后的退避时间，避免瞬时重连风暴。
        try {
            Thread.sleep(RECONNECT_BACKOFF_MILLIS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException("重连等待被中断", e);
        }
    }

    public record ClientStats(long totalRequests, long successRequests, long failedRequests, long timeoutRequests) {
    }
}
