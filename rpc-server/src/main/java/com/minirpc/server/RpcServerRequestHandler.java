package com.minirpc.server;

import com.minirpc.core.protocol.RpcRequest;
import com.minirpc.core.protocol.RpcResponse;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;

public class RpcServerRequestHandler extends SimpleChannelInboundHandler<RpcRequest> {//这句话非常关键。 它告诉 Netty: 这个 handler 专门处理入站的 RpcRequest 类型消息。
    // SimpleChannelInboundHandler<> 这个类本身就是 Netty 为“处理入站消息”准备的适配器类。
    private final RpcServer rpcServer;

    public RpcServerRequestHandler(RpcServer rpcServer) {
        this.rpcServer = rpcServer;
    }

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, RpcRequest request) { // 服务端收到请求的“业务入口”，它是 Netty 框架在收到入站消息时自动回调的方法。真正收到请求对象的业务入口
        // 这类方法叫“回调方法”（callback），和 Spring 的 Controller 被框架调用是同一思想。
        // 1) 这里拿到的 request 已经是前面的解码器帮你还原好的对象。
        RpcResponse response = rpcServer.invoke(request);// 所以你可以把这一行理解成：Netty 已经帮我把网络中的字节请求变成 RpcRequest 交到我手上了。
        // 2) 回写响应对象，Netty 会自动触发出站编码与封帧流程。
        ctx.writeAndFlush(response);// 回响应是出站
    }
}
