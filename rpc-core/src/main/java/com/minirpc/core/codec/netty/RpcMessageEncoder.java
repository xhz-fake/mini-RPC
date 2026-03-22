package com.minirpc.core.codec.netty;

import com.minirpc.core.codec.RpcMessageCodec;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToByteEncoder;

public class RpcMessageEncoder extends MessageToByteEncoder<Object> {
    @Override
    protected void encode(ChannelHandlerContext ctx, Object msg, ByteBuf out) {
        // 把任意 Java 消息对象（RpcRequest / RpcResponse）编码成字节数组。
        // 真正的序列化细节仍由 RpcMessageCodec 统一负责，避免 Netty 层重复实现。
        byte[] bytes = RpcMessageCodec.encode(msg);
        // 写入 ByteBuf，后续由 LengthFieldPrepender 在前面追加长度头。
        out.writeBytes(bytes);
    }
}
