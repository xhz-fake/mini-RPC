package com.minirpc.core.transport;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

public final class RpcIO {
    private RpcIO() {
    }

    public static void writeFrame(OutputStream outputStream, byte[] payload) throws IOException {
        DataOutputStream dos = new DataOutputStream(outputStream);
        // 先写消息长度，再写消息体，接收方才能准确切分出“一条完整消息”。
        dos.writeInt(payload.length);
        dos.write(payload);
        dos.flush();
    }

    public static byte[] readFrame(InputStream inputStream) throws IOException {
        DataInputStream dis = new DataInputStream(inputStream);
        // 先读长度，再按长度读取完整字节数组，避免半包/粘包问题。
        int length = dis.readInt();
        byte[] payload = new byte[length];
        dis.readFully(payload);
        return payload;
    }
}
