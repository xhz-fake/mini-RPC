package com.minirpc.core.codec.serialization;

import java.util.Locale;

public final class SerializerFactory {// 它本质上是在做一件事： 从 JVM 参数里读当前想用哪种序列化策略
                                      // 如果没配，就默认 jdk
                                      // 如果配了 json ，就切到 JSON 实现
    private static final String SERIALIZER_PROPERTY = "rpc.serializer";
    private static final String DEFAULT_SERIALIZER = "jdk";// 也就是你现在可以通过这种方式切换：-Drpc.serializer=jdk ;Drpc.serializer=json

    private SerializerFactory() {
    }

    public static Serializer getConfiguredSerializer() {
        // Day7：这里统一读 JVM 参数，而不是让编码器、解码器自己到处判断“该用哪种序列化”。
        // 这样主链只关心“我要一个 Serializer”，不关心它背后具体是 JDK 还是 JSON。
        return create(System.getProperty(SERIALIZER_PROPERTY, DEFAULT_SERIALIZER));
    }

    public static Serializer create(String serializerType) {
        // 这里先把外部传入的配置值做一次“去空格 + 转小写”的归一化处理。
        // 这样用户写：
        // - json
        // - JSON
        // - " json "
        // 最终都能落到同一个分支上。
        String normalizedType = serializerType == null
                ? DEFAULT_SERIALIZER
                : serializerType.trim().toLowerCase(Locale.ROOT);
        return switch (normalizedType) {
            case "", "jdk" -> new JdkSerializer();
            case "json" -> new JsonSerializer();
            default -> throw new IllegalArgumentException("不支持的序列化策略: " + serializerType);
        };
    }
}
