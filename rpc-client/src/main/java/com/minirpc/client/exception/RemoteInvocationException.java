package com.minirpc.client.exception;

public class RemoteInvocationException extends RpcClientException {// 请求已经到服务端了，服务端业务自己报错
    // 这个异常专门表达一种场景：
    // 请求已经成功到达服务端，也进入了真实业务方法，
    // 但服务端业务执行时自己报错了。
    //
    // 所以它和“连接失败”“超时”不是一类问题。
    // 连接失败更像“路上没送到”，
    // 远端业务异常更像“已经送到了，但对方处理时报错”。
    public RemoteInvocationException(String message) {
        super(message);
    }

    public RemoteInvocationException(String message, Throwable cause) {
        super(message, cause);
    }
}
