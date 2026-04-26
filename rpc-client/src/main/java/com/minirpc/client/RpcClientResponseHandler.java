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
    protected void channelRead0(ChannelHandlerContext ctx, RpcResponse msg) {// 收到一条入站消息时调用
        RpcClient.PendingRequest pendingRequest = pendingRequests.remove(msg.getRequestId());
        if (pendingRequest != null) {
            pendingRequest.future().complete(msg);// 正常收到响应时：
        }
    }
    //channelRead0() 的触发链
    //1. 对端发来响应字节流
    //2. LengthFieldBasedFrameDecoder 拆出完整帧
    //3. RpcMessageDecoder 把字节反序列化成 RpcResponse
    //4. Netty 发现后面的 handler 能处理 RpcResponse
    //5. 调用 channelRead0(ctx, msg)

    @Override
    public void channelInactive(ChannelHandlerContext ctx) {// 连接变成非活跃状态时调用
        Channel channel = ctx.channel();
        onDisconnected.accept(channel);
        failPendingOfChannel(channel, new IllegalStateException("连接已断开"));
    }
    //channelInactive() 的触发链
    //1. TCP 连接断开或关闭
    //2. Netty 感知到 channel 失活
    //3. 生成 channelInactive 事件
    //4. 事件沿 pipeline 传播
    //5. 调用 channelInactive(ctx)

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {// pipeline 处理过程中捕获到异常时调用，比如：解码异常, handler 处理异常, pipeline 某个环节抛错, 底层 I/O 异常上传上来
        failPendingOfChannel(ctx.channel(), cause);
        ctx.close();
    }
    //exceptionCaught() 的触发链
    //1. pipeline 某个阶段抛异常
    //2. Netty 捕获这个异常
    //3. 异常事件沿 pipeline 传播
    //4. 调用 exceptionCaught(ctx, cause)

    private void failPendingOfChannel(Channel channel, Throwable cause) { //- 某条连接出事了
        String endpointKey = RpcClient.endpointKey(channel);
        pendingRequests.forEach((requestId, pendingRequest) -> {// - 那这条连接上所有还在等待的请求，别一直傻等
            if (pendingRequest.endpointKey().equals(endpointKey) && pendingRequests.remove(requestId) != null) {
                pendingRequest.future().completeExceptionally(cause);//- 直接标记为“异常完成”
            }
        });
    }
}
