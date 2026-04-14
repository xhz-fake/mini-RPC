package com.minirpc.client;

import com.minirpc.core.codec.netty.RpcMessageDecoder;
import com.minirpc.core.codec.netty.RpcMessageEncoder;
import com.minirpc.core.protocol.RpcRequest;
import com.minirpc.core.protocol.RpcResponse;
import com.minirpc.registry.LoadBalancer;
import com.minirpc.registry.RandomLoadBalancer;
import com.minirpc.registry.RegistryCenter;
import com.minirpc.registry.ServiceInstance;
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

import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicLong;

// RpcClient 的核心职责是维护连接、发送请求、按 requestId 关联响应，并处理超时、断连和连接复用等客户端治理问题。
public class RpcClient {
    private static final int MAX_FRAME_LENGTH = 10 * 1024 * 1024;
    private static final long RESPONSE_TIMEOUT_SECONDS = Long.getLong("rpc.client.timeout.seconds", 5L);
    // Day4 最小可用重试次数：默认 2 次（首次 + 1 次重试）。
    private static final int MAX_REQUEST_ATTEMPTS = Integer.getInteger("rpc.client.retry.maxAttempts", 2);// 去 JVM 的系统属性里查一个名字叫 rpc.client.retry.maxAttempts 的配置项，如果查到了，就把它转成 Integer；如果没查到，就用默认值 2
    private static final int MAX_RECONNECT_RETRIES = 3;
    private static final long RECONNECT_BACKOFF_MILLIS = 300;

    private final String directHost;
    private final int directPort;
    private final EventLoopGroup group;
    private final Bootstrap bootstrap;
    private final RegistryCenter registryCenter;
    private final LoadBalancer loadBalancer;

    // requestId -> PendingRequest。Day4 继续沿用 Day3 的 requestId 精准配对能力。
    private final Map<String, PendingRequest> pendingRequests = new ConcurrentHashMap<>();
    // endpoint(host:port) -> Channel。不同服务实例维护各自连接，便于连接复用。
    private final Map<String, Channel> channels = new ConcurrentHashMap<>();// 与Day3不同(只有一个channel，即固定的服务端地址)，而此处的 channels 是一个 Map
    // endpoint(host:port) -> lock。避免并发请求同时对同一实例重复建连。
    private final Map<String, Object> connectLocks = new ConcurrentHashMap<>();

    private final AtomicLong totalRequests = new AtomicLong();
    private final AtomicLong successRequests = new AtomicLong();
    private final AtomicLong failedRequests = new AtomicLong();
    private final AtomicLong timeoutRequests = new AtomicLong();

    public RpcClient(String host, int port) {
        this(host, port, null, new RandomLoadBalancer());
    }

    public RpcClient(RegistryCenter registryCenter, LoadBalancer loadBalancer) {
        this(null, -1, registryCenter, loadBalancer);
    }

    private RpcClient(String directHost, int directPort, RegistryCenter registryCenter, LoadBalancer loadBalancer) {
        this.directHost = directHost;
        this.directPort = directPort;
        this.registryCenter = registryCenter;
        this.loadBalancer = loadBalancer == null ? new RandomLoadBalancer() : loadBalancer;
        this.group = new NioEventLoopGroup(1);
        this.bootstrap = createBootstrap();
    }

    public RpcResponse sendRequest(RpcRequest request) {
        totalRequests.incrementAndGet();
        // 记录“这次请求已经尝试过哪些实例”。
        // Day4 重试时要避开它们，尽量换一台机器再试。
        List<ServiceInstance> triedInstances = new ArrayList<>();
        RuntimeException lastFailure = null;
        // 至少尝试 1 次；如果你在 VM options 里配置了更大的值，就按配置走。
        int maxAttempts = Math.max(1, MAX_REQUEST_ATTEMPTS);
        for (int attempt = 1; attempt <= maxAttempts; attempt++) {
            // 先决定“这次请求准备打到哪台服务实例”。
            ServiceInstance targetInstance = selectServiceInstance(request, triedInstances);
            triedInstances.add(targetInstance);
            // 重试时换一个新的 requestId，避免和第一次请求的挂起 future 冲突。
            RpcRequest attemptRequest = copyRequestWithNewIdIfNeeded(request, attempt);
            CompletableFuture<RpcResponse> future = new CompletableFuture<>();
            // Day4 这里不只保存 future，还顺手记住“它发往哪个实例”。
            // 这样某条连接断开时，只清理属于该实例的挂起请求，不误伤其他实例。
            pendingRequests.put(attemptRequest.getRequestId(), new PendingRequest(future, endpointKey(targetInstance)));
            try {
                Channel activeChannel = ensureConnected(targetInstance);
                activeChannel.writeAndFlush(attemptRequest).sync();
                // 经过编码和前置长度头之后，真正进入网络：
                // 现在数据已经变成：[4字节长度][请求体字节]
                //接下来这段不是你自己写的 Java 业务代码，而是：
                //- Netty
                //- Java NIO
                //- 操作系统 Socket
                //- TCP 协议栈
                //- 网卡
                //共同完成的。
                RpcResponse response = future.get(RESPONSE_TIMEOUT_SECONDS, TimeUnit.SECONDS);
                successRequests.incrementAndGet();
                return response;
            } catch (TimeoutException e) {
                pendingRequests.remove(attemptRequest.getRequestId());
                timeoutRequests.incrementAndGet();
                failedRequests.incrementAndGet();
                throw new RuntimeException("远程调用超时", e);
            } catch (ExecutionException e) {
                pendingRequests.remove(attemptRequest.getRequestId());
                failedRequests.incrementAndGet();
                throw new RuntimeException("远程调用失败", e.getCause());
            } catch (InterruptedException e) {
                pendingRequests.remove(attemptRequest.getRequestId());
                failedRequests.incrementAndGet();
                Thread.currentThread().interrupt();
                throw new RuntimeException("远程调用失败", e);
            } catch (RuntimeException e) {
                pendingRequests.remove(attemptRequest.getRequestId());
                failedRequests.incrementAndGet();
                lastFailure = e;
                // 这里只是先记住失败，是否继续重试交给下一轮 for 决定。
                if (attempt == maxAttempts) {
                    throw e;
                }
            }
        }
        throw lastFailure == null ? new RuntimeException("远程调用失败") : lastFailure;
    }

    public void shutdown() {
        for (Channel channel : channels.values()) {
            if (channel != null && channel.isActive()) {
                channel.close();
            }
        }
        channels.clear();
        group.shutdownGracefully();
    }

    public ClientStats getStats() {
        return new ClientStats(
                totalRequests.get(),                successRequests.get(),
                failedRequests.get(),
                timeoutRequests.get()
        );
    }

    private Bootstrap createBootstrap() {
        Bootstrap newBootstrap = new Bootstrap();
        newBootstrap.group(group)
                .channel(NioSocketChannel.class)
                .option(ChannelOption.TCP_NODELAY, true)
                .handler(new ChannelInitializer<SocketChannel>() {
                    @Override
                    protected void initChannel(SocketChannel ch) {
                        ChannelPipeline pipeline = ch.pipeline();
                        pipeline.addLast(new LengthFieldBasedFrameDecoder(MAX_FRAME_LENGTH, 0, 4, 0, 4));
                        pipeline.addLast(new RpcMessageDecoder());
                        pipeline.addLast(new LengthFieldPrepender(4));// 在消息体前面加 4 个字节，表示“后面的正文长度是多少”
                        pipeline.addLast(new RpcMessageEncoder());
                        pipeline.addLast(new RpcClientResponseHandler(pendingRequests, RpcClient.this::markDisconnected));
                    }
                });
        return newBootstrap;
    }

    private Channel ensureConnected(ServiceInstance instance) {
        String endpointKey = endpointKey(instance);
        // Day4 的连接缓存不再是“单个 channel”，而是“实例地址 -> channel”。
        Channel localChannel = channels.get(endpointKey);
        if (localChannel != null && localChannel.isActive()) {
            return localChannel;
        }
        // 每个实例单独一把锁，避免多个线程同时给同一个实例重复建连。
        Object connectLock = connectLocks.computeIfAbsent(endpointKey, key -> new Object());
        synchronized (connectLock) {
            localChannel = channels.get(endpointKey);
            if (localChannel != null && localChannel.isActive()) {
                return localChannel;
            }
            for (int attempt = 1; attempt <= MAX_RECONNECT_RETRIES; attempt++) {
                try {
                    Channel newChannel = bootstrap.connect(instance.getHost(), instance.getPort()).sync().channel();
                    newChannel.closeFuture().addListener(future -> markDisconnected(newChannel));
                    channels.put(endpointKey, newChannel);
                    return newChannel;
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new RuntimeException("连接服务端失败", e);
                } catch (Exception e) {
                    if (attempt == MAX_RECONNECT_RETRIES) {
                        throw new RuntimeException("连接服务端失败", e);
                    }
                    sleepBackoff();
                }
            }
            throw new RuntimeException("连接服务端失败");
        }
    }

    private ServiceInstance selectServiceInstance(RpcRequest request, List<ServiceInstance> triedInstances) {
        if (registryCenter == null) {// - 说明走的是 Day3/直连模式， 就直接返回固定地址实例
            return new ServiceInstance(request.getInterfaceName(), directHost, directPort);
        }
        // discover 的意思是：去注册中心问一句“这个服务现在有哪些可用地址”。
        List<ServiceInstance> instances = registryCenter.discover(request.getInterfaceName());
        if (instances.isEmpty()) {
            throw new RuntimeException("没有发现可用服务实例: " + request.getInterfaceName());
        }
        return loadBalancer.select(instances, triedInstances);// 从多个实例里挑一个
    }

    private RpcRequest copyRequestWithNewIdIfNeeded(RpcRequest request, int attempt) {
        if (attempt == 1) {
            // 第一次请求直接用原 request。
            return request;
        }
        // 从第二次开始说明进入“重试模式”。
        // 这里复制出一个新请求对象，并换一个新的 requestId。
        RpcRequest retryRequest = new RpcRequest();
        retryRequest.setRequestId(request.getRequestId() + "-retry-" + attempt);
        retryRequest.setInterfaceName(request.getInterfaceName());
        retryRequest.setMethodName(request.getMethodName());
        retryRequest.setParameterTypes(request.getParameterTypes());
        retryRequest.setArgs(request.getArgs());
        return retryRequest;
    }

    private void markDisconnected(Channel channel) {
        // 某条连接失效后，把它从连接缓存中移除。
        // 下次如果还想访问这个实例，就会重新建连。
        channels.entrySet().removeIf(entry -> entry.getValue() == channel);
    }

    private void sleepBackoff() {
        try {
            Thread.sleep(RECONNECT_BACKOFF_MILLIS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException("重连等待被中断", e);
        }
    }

    static String endpointKey(ServiceInstance instance) {
        // 把实例对象统一转成 host:port 形式的字符串 key。
        return instance.getHost() + ":" + instance.getPort();
    }

    static String endpointKey(Channel channel) {
        // 反过来：从一条真实连接上提取出它连的是哪个远端地址。
        SocketAddress remoteAddress = channel.remoteAddress();
        if (remoteAddress instanceof InetSocketAddress socketAddress) {
            return socketAddress.getHostString() + ":" + socketAddress.getPort();
        }
        return String.valueOf(remoteAddress);
    }

    record PendingRequest(CompletableFuture<RpcResponse> future, String endpointKey) {
    }// 它在表达：这条请求现在还没回来，我不仅要记住“将来用哪个future去唤醒调用方”，还要记住“它是发往哪台服务机器的

    public record ClientStats(long totalRequests, long successRequests, long failedRequests, long timeoutRequests) {
    }
}
