package com.minirpc.client;

import com.minirpc.client.exception.RemoteInvocationException;
import com.minirpc.client.exception.RpcClientException;
import com.minirpc.client.exception.RpcConnectionException;
import com.minirpc.client.exception.RpcTimeoutException;
import com.minirpc.client.exception.ServiceDiscoveryException;
import com.minirpc.core.codec.netty.RpcMessageDecoder;
import com.minirpc.core.codec.netty.RpcMessageEncoder;
import com.minirpc.core.protocol.RpcRequest;
import com.minirpc.core.protocol.RpcResponse;
import com.minirpc.registry.LoadBalancer;
import com.minirpc.registry.LoadBalancerFactory;
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
public class RpcClient {// 它做的事只是：-连接 ZooKeeper -询服务节点 -读取子节点 -发现有哪些实例可用
    private static final int MAX_FRAME_LENGTH = 10 * 1024 * 1024;
    private static final String RESPONSE_TIMEOUT_PROPERTY = "rpc.client.timeout.seconds";
    private static final long DEFAULT_RESPONSE_TIMEOUT_SECONDS = 5L;
    // Day4 最小可用重试次数：默认 2 次（首次 + 1 次重试）。
    private static final String MAX_REQUEST_ATTEMPTS_PROPERTY = "rpc.client.retry.maxAttempts";
    private static final int DEFAULT_MAX_REQUEST_ATTEMPTS = 2;
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
        this(host, port, null, null);
    }// 构造器委托，调用下方的私有的四个参数的构造器

    public RpcClient(RegistryCenter registryCenter, LoadBalancer loadBalancer) {// 构造器委托，调用下方的私有的四个参数的构造器
        this(null, -1, registryCenter, loadBalancer);
    }

    private RpcClient(String directHost, int directPort, RegistryCenter registryCenter, LoadBalancer loadBalancer) {// - 使用 private 修饰,外部不允许乱传四个参数随便拼只能走作者允许的两种入口：直连模式、注册中心模式
        this.directHost = directHost;
        this.directPort = directPort;
        this.registryCenter = registryCenter;
        // Day7：默认负载均衡器也支持通过 JVM 参数切换，而不是固定写死随机策略。
        this.loadBalancer = loadBalancer == null ? LoadBalancerFactory.fromSystemProperty() : loadBalancer;// - 如果外面没手动传策略,就按 JVM 配置自动决定当前负载均衡器
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
        int maxAttempts = Math.max(1, maxRequestAttempts());
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
                // 接下来这段不是你自己写的 Java 业务代码，而是：
                //- Netty
                //- Java NIO
                //- 操作系统 Socket
                //- TCP 协议栈
                //- 网卡
                //共同完成的。
                RpcResponse response = future.get(responseTimeoutSeconds(), TimeUnit.SECONDS);
                //- 当前线程先暂停
                //- 去等这次 requestId 对应的响应回来
                //- 等到了就拿 RpcResponse
                //- 等不到就抛 TimeoutException

                successRequests.incrementAndGet();
                return response;
            } catch (TimeoutException e) {// 如果超时，代码会走这里：
                pendingRequests.remove(attemptRequest.getRequestId());
                timeoutRequests.incrementAndGet();
                failedRequests.incrementAndGet();
                RpcTimeoutException timeoutFailure = new RpcTimeoutException("远程调用超时: " + attemptRequest.getRequestId(), e);
                lastFailure = timeoutFailure;
                if (shouldRetry(timeoutFailure, attempt, maxAttempts)) {
                    continue;
                }
                throw timeoutFailure;
            } catch (ExecutionException e) {// ExecutionException 本身只是个包装壳
                pendingRequests.remove(attemptRequest.getRequestId());
                failedRequests.incrementAndGet();
                RuntimeException executionFailure = classifyExecutionFailure(e.getCause(), targetInstance);// 真正原因在 e.getCause()
                lastFailure = executionFailure;
                if (shouldRetry(executionFailure, attempt, maxAttempts)) {
                    continue;
                }
                throw executionFailure;
            } catch (InterruptedException e) {// 当前调用线程在等待过程中被别人中断了
                pendingRequests.remove(attemptRequest.getRequestId());
                failedRequests.incrementAndGet();
                Thread.currentThread().interrupt();//因为 InterruptedException 抛出后，线程的中断标记会被清掉。 如果你希望上层还能感知“这个线程曾经被中断过”，就要手动把中断标记补回去。
                //这是 Java 并发里一个经典写法。

                RpcClientException interruptedFailure = new RpcClientException("远程调用被中断", e);
                lastFailure = interruptedFailure;
                throw interruptedFailure;
            } catch (RuntimeException e) {
                pendingRequests.remove(attemptRequest.getRequestId());
                failedRequests.incrementAndGet();
                RuntimeException runtimeFailure = classifyRuntimeFailure(e);
                lastFailure = runtimeFailure;
                // 这里只是先记住失败，是否继续重试交给下一轮 for 决定。
                if (!shouldRetry(runtimeFailure, attempt, maxAttempts)) {
                    throw runtimeFailure;
                }
            }
        }
        throw lastFailure == null ? new RpcClientException("远程调用失败") : lastFailure;
    }

    public void shutdown() {// 把 RPC 通信层的连接关掉
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
                totalRequests.get(), successRequests.get(),
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
                        // 入站
                        pipeline.addLast(new LengthFieldBasedFrameDecoder(MAX_FRAME_LENGTH, 0, 4, 0, 4));
                        pipeline.addLast(new RpcMessageDecoder());

                        // 出站
                        pipeline.addLast(new LengthFieldPrepender(4));// 在消息体前面加 4 个字节，表示“后面的正文长度是多少”
                        pipeline.addLast(new RpcMessageEncoder());

                        // 入站
                        pipeline.addLast(new RpcClientResponseHandler(pendingRequests, RpcClient.this::markDisconnected));// 这里把 handler 加进了 pipeline, 这句话等于在告诉 Netty: 以后这条连接上的入站消息、连接状态变化、异常事件都可以传到这个 handler
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
                } catch (InterruptedException e) {//建连失败
                    Thread.currentThread().interrupt();
                    throw new RpcConnectionException("连接服务端失败: " + instance, e);// 它表达的是： 客户端连目标 provider 这一步就没成功
                } catch (Exception e) {
                    if (attempt == MAX_RECONNECT_RETRIES) {
                        throw new RpcConnectionException("连接服务端失败: " + instance, e);
                    }
                    sleepBackoff();
                }
            }
            throw new RpcConnectionException("连接服务端失败: " + instance);
        }
    }

    private ServiceInstance selectServiceInstance(RpcRequest request, List<ServiceInstance> triedInstances) {
        if (registryCenter == null) {// - 说明走的是 Day3/直连模式， 就直接返回固定地址实例
            return new ServiceInstance(request.getInterfaceName(), directHost, directPort);
        }
        // discover 的意思是：去注册中心问一句“这个服务现在有哪些可用地址”。
        // Day6 以后，这句话背后通常先查的是“当前 JVM 里的本地缓存”，
        // 只有第一次还没缓存时，才会去 ZooKeeper 拉一份初始数据，并启动 watcher。
        List<ServiceInstance> instances;
        try {
            instances = registryCenter.discover(request.getInterfaceName());// 多态，调用方法时，会在运行时找到真正的实现类方法执行，面向接口编程
        } catch (RuntimeException e) {
            throw new ServiceDiscoveryException("发现服务实例失败: " + request.getInterfaceName(), e);
        }
        if (instances.isEmpty()) {// 注册中心里没有服务实例
            throw new ServiceDiscoveryException("没有发现可用服务实例: " + request.getInterfaceName());
        }
        try {
            return loadBalancer.select(instances, triedInstances);// 从多个实例里挑一个
        } catch (IllegalStateException e) {
            throw new ServiceDiscoveryException("没有剩余可重试的服务实例: " + request.getInterfaceName(), e);
        }
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
            throw new RpcClientException("重连等待被中断", e);
        }
    }

    private boolean shouldRetry(RuntimeException failure, int attempt, int maxAttempts) {
        if (attempt >= maxAttempts) {
            return false;
        }
        // Day7：把“是否值得重试”的语义明确下来。
        // - 连接失败：大概率是目标实例暂时不可用，适合换实例再试
        // - 超时：可能是瞬时抖动，也适合有限重试
        // - 服务发现失败 / 远端业务异常：通常继续重试意义不大
        return failure instanceof RpcConnectionException || failure instanceof RpcTimeoutException;
    }

    private RuntimeException classifyExecutionFailure(Throwable cause, ServiceInstance targetInstance) {
        // ExecutionException 的意思是：future 最终是“异常完成”的。
        // 但它自己只是一个“包装壳”，真正失败原因在 cause 里。
        //
        // 所以这里要做的事情不是“看到 ExecutionException 就结束”，
        // 而是继续往里拆，看看底层到底是：
        // - 连接断开
        // - 远端业务异常
        // - 其他客户端侧错误
        if (cause instanceof RuntimeException runtimeException) {
            return classifyRuntimeFailure(runtimeException);
        }
        return new RpcClientException("远程调用失败: " + targetInstance, cause);
    }

    private RuntimeException classifyRuntimeFailure(RuntimeException runtimeException) {
        if (runtimeException instanceof RpcClientException || runtimeException instanceof RemoteInvocationException) {
            // 如果它本来就已经是我们 Day7 明确分好的异常类型，就直接原样往外抛。
            return runtimeException;
        }
        String message = runtimeException.getMessage();
        if (message != null && message.contains("连接已断开")) {
            // 这里把“连接已断开”这种运行时错误，重新包装成更明确的连接类异常。
            return new RpcConnectionException(message, runtimeException);// 如果消息里能判断出是“连接已断开”，就升级成 RpcConnectionException
        }

        // 走到这里，说明它既不是我们已知的清晰异常类型，也不是明显的断连场景，
        // 那就统一兜底成 RpcClientException，表示“客户端侧确实失败了，但不属于前面几类特定情况”。
        return new RpcClientException("远程调用失败", runtimeException);
    }

    private static long responseTimeoutSeconds() {
        return Long.getLong(RESPONSE_TIMEOUT_PROPERTY, DEFAULT_RESPONSE_TIMEOUT_SECONDS);
    }

    private static int maxRequestAttempts() {
        return Integer.getInteger(MAX_REQUEST_ATTEMPTS_PROPERTY, DEFAULT_MAX_REQUEST_ATTEMPTS);
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
