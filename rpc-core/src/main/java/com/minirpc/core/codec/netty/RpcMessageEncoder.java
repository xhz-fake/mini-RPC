package com.minirpc.core.codec.netty;

import com.minirpc.core.codec.RpcMessageCodec;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToByteEncoder;

public class RpcMessageEncoder extends MessageToByteEncoder<Object> {
    @Override
    protected void encode(ChannelHandlerContext ctx, Object msg, ByteBuf out) {// 这是 Netty 框架自动回调的。不是你手动调用 encode(...) ，而是你调用了：activeChannel.writeAndFlush(attemptRequest)之后，Netty 发现你要发送一个对象，于是自动沿着出站 pipeline 往前找能处理它的出站处理器，最后调用了 RpcMessageEncoder.encode(...)。
        // 把任意 Java 消息对象（RpcRequest / RpcResponse）编码成字节数组。
        // 真正的序列化细节仍由 RpcMessageCodec 统一负责，避免 Netty 层重复实现。
        byte[] bytes = RpcMessageCodec.encode(msg);
        // 写入 ByteBuf，后续由 LengthFieldPrepender 在前面追加长度头。
        out.writeBytes(bytes);// 把刚才那串序列化后的字节，写进 Netty 的发送缓冲区
    }
}
