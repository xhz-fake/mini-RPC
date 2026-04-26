package com.minirpc.core.codec.serialization;

import com.minirpc.core.protocol.RpcRequest;
import com.minirpc.core.protocol.RpcResponse;
import org.junit.Test;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class SerializerTest {

    @Test// JdkSerializer 能不能正确往返 RpcRequest
    public void jdkSerializerShouldRoundTripRpcRequest() {
        assertRequestRoundTrip(new JdkSerializer());
    }

    @Test// JdkSerializer 能不能正确往返 RpcResponse
    public void jdkSerializerShouldRoundTripRpcResponse() {
        assertResponseRoundTrip(new JdkSerializer());
    }

    @Test// JsonSerializer 能不能正确往返 RpcRequest
    public void jsonSerializerShouldRoundTripRpcRequest() {
        assertRequestRoundTrip(new JsonSerializer());
    }

    @Test// JsonSerializer 能不能正确往返 RpcResponse
    public void jsonSerializerShouldRoundTripRpcResponse() {
        assertResponseRoundTrip(new JsonSerializer());
    }

    @Test
    public void factoryShouldCreateExpectedSerializer() {
        assertTrue(SerializerFactory.create("jdk") instanceof JdkSerializer);
        assertTrue(SerializerFactory.create("json") instanceof JsonSerializer);
    }

    private void assertRequestRoundTrip(Serializer serializer) {
        RpcRequest request = new RpcRequest();
        request.setRequestId("req-1");
        request.setInterfaceName("com.minirpc.demo.service.HelloService");
        request.setMethodName("hello");
        request.setParameterTypes(new Class<?>[]{String.class, Integer.class});
        request.setArgs(new Object[]{"mini-rpc", 7});

        RpcRequest decoded = (RpcRequest) serializer.deserialize(serializer.serialize(request));
        assertEquals(request.getRequestId(), decoded.getRequestId());
        assertEquals(request.getInterfaceName(), decoded.getInterfaceName());
        assertEquals(request.getMethodName(), decoded.getMethodName());
        assertArrayEquals(request.getParameterTypes(), decoded.getParameterTypes());
        assertArrayEquals(request.getArgs(), decoded.getArgs());
        // 对 RpcRequest 这种包含字符串、数组、参数类型、参数值的协议对象，序列化后再还原，信息不能丢
    }

    private void assertResponseRoundTrip(Serializer serializer) {
        RpcResponse response = new RpcResponse();
        response.setRequestId("req-1");
        response.setData("hello-response");

        RpcResponse decoded = (RpcResponse) serializer.deserialize(serializer.serialize(response));
        assertEquals(response.getRequestId(), decoded.getRequestId());
        assertEquals(response.getData(), decoded.getData());
    }
}
