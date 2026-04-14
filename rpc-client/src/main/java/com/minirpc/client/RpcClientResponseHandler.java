package com.minirpc.client;

import com.minirpc.core.protocol.RpcResponse;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;

import java.util.Map;
import java.util.function.Consumer;

public class RpcClientResponseHandler extends SimpleChannelInboundHandler<RpcResponse> {
    // 与 RpcClient 共享的挂起请求表：键是 requestId，值是等待中的请求上下文。
    private final Map<String, RpcClient.PendingRequest> pendingRequests;
    // 连接断开回调：由 RpcClient 提供，用于重置连接缓存状态。
    private final Consumer<Channel> onDisconnected;

    public RpcClientResponseHandler(Map<String, RpcClient.PendingRequest> pendingRequests, Consumer<Channel> onDisconnected) {
        this.pendingRequests = pendingRequests;
        this.onDisconnected = onDisconnected;
    }

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, RpcResponse msg) {
        RpcClient.PendingRequest pendingRequest = pendingRequests.remove(msg.getRequestId());
        if (pendingRequest != null) {
            pendingRequest.future().complete(msg);
        }
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) {
        Channel channel = ctx.channel();
        onDisconnected.accept(channel);
        failPendingOfChannel(channel, new IllegalStateException("连接已断开"));
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        failPendingOfChannel(ctx.channel(), cause);
        ctx.close();
    }

    private void failPendingOfChannel(Channel channel, Throwable cause) {
        String endpointKey = RpcClient.endpointKey(channel);
        pendingRequests.forEach((requestId, pendingRequest) -> {
            if (pendingRequest.endpointKey().equals(endpointKey) && pendingRequests.remove(requestId) != null) {
                pendingRequest.future().completeExceptionally(cause);
            }
        });
    }
}
