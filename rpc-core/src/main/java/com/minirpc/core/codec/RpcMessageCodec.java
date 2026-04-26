package com.minirpc.core.codec;

import com.minirpc.core.codec.serialization.Serializer;
import com.minirpc.core.codec.serialization.SerializerFactory;

public final class RpcMessageCodec {
    private RpcMessageCodec() {
    }

    public static byte[] encode(Object obj) {
        // Day7：不再把序列化方式写死在这里，而是交给 SerializerFactory 按配置选择。
        // 这样后面要切 JDK / JSON，甚至继续扩展其他实现时，编解码主链不需要重写。
        Serializer serializer = SerializerFactory.getConfiguredSerializer();
        // 注意：这里每次 encode/decode 都是“先看当前配置，再拿对应实现”。
        // 这样客户端和服务端只要 JVM 参数保持一致，就会使用同一套序列化策略。
        return serializer.serialize(obj);// 这一步之后，数据已经不再是 Java 对象形态了，而是：一串字节数组 byte[]
    }

    public static Object decode(byte[] bytes) {
        Serializer serializer = SerializerFactory.getConfiguredSerializer();
        // 把网络字节还原成 Java 对象，客户端和服务端都依赖这一步。
        return serializer.deserialize(bytes); // 把网络字节还原成 Java 对象
    }
}
