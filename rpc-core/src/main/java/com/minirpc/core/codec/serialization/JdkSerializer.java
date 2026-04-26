package com.minirpc.core.codec.serialization;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;

public class JdkSerializer implements Serializer {
    // 思路是：
    //- 用 ObjectOutputStream / ObjectInputStream
    //- 直接走 Java 原生对象流序列化

    @Override
    public String name() {
        return "jdk";
    }

    @Override
    public byte[] serialize(Object obj) {
        try (ByteArrayOutputStream bos = new ByteArrayOutputStream();
             ObjectOutputStream oos = new ObjectOutputStream(bos)) {
            oos.writeObject(obj);
            oos.flush();
            return bos.toByteArray();
        } catch (IOException e) {
            throw new SerializationException("JDK 序列化失败", e);
        }
    }

    @Override
    public Object deserialize(byte[] bytes) {
        try (ByteArrayInputStream bis = new ByteArrayInputStream(bytes);
             ObjectInputStream ois = new ObjectInputStream(bis)) {
            return ois.readObject();
        } catch (IOException | ClassNotFoundException e) {
            throw new SerializationException("JDK 反序列化失败", e);
        }
    }
}
