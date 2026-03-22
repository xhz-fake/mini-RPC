package com.minirpc.server;

import com.minirpc.core.protocol.RpcRequest;
import com.minirpc.core.protocol.RpcResponse;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;

public class RpcServerRequestHandler extends SimpleChannelInboundHandler<RpcRequest> {
    // 持有 RpcServer 是为了复用其中的 invoke 业务执行逻辑。
    private final RpcServer rpcServer;

    public RpcServerRequestHandler(RpcServer rpcServer) {
        this.rpcServer = rpcServer;
    }

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, RpcRequest request) { // 服务端收到请求，它是 Netty 框架在收到入站消息时自动回调 的方法。
        // 这类方法叫“回调方法”（callback），和 Spring 的 Controller 被框架调用是同一思想。
        // 1) 这里拿到的 request 已经是“解码后的对象”，无需再手动反序列化。
        RpcResponse response = rpcServer.invoke(request);
        // 2) 回写响应对象，Netty 会自动触发出站编码与封帧流程。
        ctx.writeAndFlush(response);// 回响应是出站
    }
}
