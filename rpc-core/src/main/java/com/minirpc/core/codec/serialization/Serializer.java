package com.minirpc.core.codec.serialization;

public interface Serializer {// - 我先不管你是 JDK 序列化还是 JSON; 只要你能完成“编码”和“解码”这两个动作; 你就可以作为一种序列化方案存在
    // 便于日志、配置和测试里识别当前使用的是哪种序列化实现。
    String name();

    byte[] serialize(Object obj);

    Object deserialize(byte[] bytes);
}
