package com.minirpc.core.codec;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;

public final class RpcMessageCodec {
    private RpcMessageCodec() {
    }

    public static byte[] encode(Object obj) {
        try (ByteArrayOutputStream bos = new ByteArrayOutputStream();
             ObjectOutputStream oos = new ObjectOutputStream(bos)) {
            // Day1 先使用 JDK 原生序列化，重点在“跑通链路”，后续再替换更高性能方案。
            oos.writeObject(obj);
            oos.flush();
            return bos.toByteArray();
        } catch (IOException e) {
            throw new RuntimeException("编码失败", e);
        }
    }

    public static Object decode(byte[] bytes) {
        try (ByteArrayInputStream bis = new ByteArrayInputStream(bytes);
             ObjectInputStream ois = new ObjectInputStream(bis)) {
            // 把网络字节还原成 Java 对象，客户端和服务端都依赖这一步。
            return ois.readObject(); // 把网络字节还原成 Java 对象
        } catch (IOException | ClassNotFoundException e) {
            throw new RuntimeException("解码失败", e);
        }
    }
}
