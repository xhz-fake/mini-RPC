package com.minirpc.core.codec.netty;

import com.minirpc.core.codec.RpcMessageCodec;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.ByteToMessageDecoder;

import java.util.List;

public class RpcMessageDecoder extends ByteToMessageDecoder {
    @Override
    protected void decode(ChannelHandlerContext ctx, ByteBuf in, List<Object> out) {// 这里就是服务端把网络字节真正“还原成 Java 对象”的地方。
        // 走到这里时，LengthFieldBasedFrameDecoder 已经保证 in 是“一整条完整请求消息体字节”。
        // 所以可以直接按可读长度取出整条消息字节。
        byte[] bytes = new byte[in.readableBytes()];
        in.readBytes(bytes);// 把当前这一整帧正文从 ByteBuf 里读出来，放到 byte[] 数组中
        // 反序列化为 Java 对象并加入 out，交给后续业务处理器。
        out.add(RpcMessageCodec.decode(bytes));// 把解码出来的 Java 对象交给 Netty，让它继续往后传给下一个入站 handler
    }
}
