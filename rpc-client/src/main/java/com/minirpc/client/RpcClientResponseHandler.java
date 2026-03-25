package com.minirpc.client;

import com.minirpc.core.protocol.RpcResponse;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;

import java.util.Map;
import java.util.concurrent.CompletableFuture;

public class RpcClientResponseHandler extends SimpleChannelInboundHandler<RpcResponse> {// 这个类本身就是 Netty 为“处理入站消息”准备的适配器类。
    // 与 RpcClient 共享的挂起请求表：键是 requestId，值是等待中的 future。
    private final Map<String, CompletableFuture<RpcResponse>> pendingRequests;
    // 连接断开回调：由 RpcClient 提供，用于重置 channel 状态。
    private final Runnable onDisconnected;

    public RpcClientResponseHandler(Map<String, CompletableFuture<RpcResponse>> pendingRequests, Runnable onDisconnected) {
        this.pendingRequests = pendingRequests;
        this.onDisconnected = onDisconnected;
    }

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, RpcResponse msg) {// 客户端收到响应对象的具体位置
        // 这里的参数： RpcResponse msg 就是“客户端从网络中收到并解码完成的响应对象”。
        // 通过 requestId 找到“发起这次请求的等待方”。
        CompletableFuture<RpcResponse> future = pendingRequests.remove(msg.getRequestId());
        if (future != null) {
            // 正常完成 future，sendRequest 中阻塞等待的线程会被唤醒。
            future.complete(msg);// 一旦 complete(msg) 执行了，原来阻塞在 future.get(...) 的业务线程就被唤醒了。客户端底层通信是异步的，但业务接口体验仍然可以保持同步。
        }
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) {
        // 通道失活通常意味着网络断开或服务端关闭连接。
        onDisconnected.run();
        // 将所有还在等待的请求统一失败回填，避免调用方永久挂起。
        failAllPending(new IllegalStateException("连接已断开"));
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        // handler 层面出现异常时，同样要让等待中的请求尽快失败返回。
        failAllPending(cause);
        ctx.close();
    }

    private void failAllPending(Throwable cause) {
        // 批量失败回填：让每个等待中的 future 都收到异常完成信号。
        pendingRequests.forEach((requestId, future) -> {
            if (pendingRequests.remove(requestId) != null) {
                future.completeExceptionally(cause);
            }
        });
    }
}
