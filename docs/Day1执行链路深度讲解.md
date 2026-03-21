# Day1 执行链路深度讲解

## 1. 讲解目标

只围绕一个目标来理解全部代码：

`客户端和服务端通信成功，并在客户端控制台输出 rpc result: + result`

讲解方式不是按文件目录，而是按“代码执行路径”。

## 2. 先看终点，再倒推

最终输出发生在客户端：

```java
String result = helloService.hello("mini-RPC");
System.out.println("rpc result: " + result);
```

你要先明确一件事：  
`helloService.hello("mini-RPC")` 这行看起来像本地方法，实际上是远程调用。

因此我们沿着这行代码，做一次“深度优先”追踪。

## 3. 深度优先追踪（从调用点一路钻到底）

### 第 1 层：调用从哪里发起

入口是客户端主程序：

- 创建网络客户端（目标地址 127.0.0.1:9000）
- 创建动态代理工厂
- 生成 HelloService 代理对象
- 调用 `hello`

这一步的本质：业务代码只关心接口，不关心网络细节。

## 第 2 层：为什么 `helloService.hello()` 会变成远程调用

代理创建发生在 `RpcClientProxy#create`。  
这里通过 `Proxy.newProxyInstance` 注入了一个 InvocationHandler。

当你调用 `helloService.hello("mini-RPC")` 时，不会直接进实现类，而会先进入 handler。

handler 做了 2 件关键事：

1. 把方法调用信息组装成 `RpcRequest`
2. 交给 `rpcClient.sendRequest` 发到服务端

到这里你应该理解：  
动态代理就是 RPC 的“翻译官”，把“方法调用语义”翻译成“网络请求语义”。

### 第 3 层：请求对象为什么要长这样

`RpcRequest` 包含：

- requestId
- interfaceName
- methodName
- parameterTypes
- args

这些字段不是为了好看，而是为了服务端“准确复现这次调用”：

- 没 `interfaceName`，不知道找哪个服务
- 没 `methodName`，不知道执行哪个方法
- 没 `parameterTypes`，方法重载时无法定位
- 没 `args`，没有参数可执行
- 没 `requestId`，响应不好匹配请求

## 第 4 层：请求如何跨进程发送

进入 `RpcClient#sendRequest` 后发生三步：

1. 建立 Socket 连接（host + port）
2. 请求对象编码为字节数组
3. 按“长度 + 内容”写入网络流

这里有两个关键辅助组件：

- `RpcMessageCodec`：对象 <-> 字节
- `RpcIO`：帧边界控制（先写长度再写内容）

为什么要帧边界：  
TCP 是字节流，不是消息流。你不写长度，接收方不知道一条消息到哪里结束。

## 第 5 层：服务端如何接住请求

服务端主程序先做初始化：

1. 建立 `ServiceRegistry`
2. 注册 `HelloService -> HelloServiceImpl`
3. 启动 `RpcServer` 监听 9000

一旦客户端连上，`RpcServer#start` 接收 socket 并分配线程处理。

处理线程中的逻辑是：

1. 从网络流读一帧字节
2. 解码成 `RpcRequest`
3. 调用 `invoke` 执行真实业务
4. 把结果封装 `RpcResponse`，编码回传

## 第 6 层：服务端如何“执行到正确的方法”

`invoke` 的关键流程：

1. 用 `interfaceName` 从注册表拿到服务实例
2. 用 `methodName + parameterTypes` 反射定位方法
3. 用 `args` 执行方法
4. 把结果放进 `RpcResponse.data`

如果异常发生，则写入 `RpcResponse.error`。

这就是“远程调用还原成本地执行”的核心动作。

## 第 7 层：响应如何回到客户端并打印

客户端 `sendRequest` 在发完请求后会阻塞等待响应：

1. 读响应帧
2. 解码成 `RpcResponse`
3. 返回给代理层

代理层检查 `response.hasError()`：

- 有错误：抛异常
- 无错误：返回 `response.getData()`

最终 `ClientBootstrap` 拿到字符串并打印：

`rpc result: hello mini-RPC`

## 4. 你现在应该建立的“脑内模型”

把 Day1 想成一个翻译流水线：

本地调用  
→ 代理拦截  
→ 请求对象  
→ 字节编码  
→ 网络传输  
→ 服务端解码  
→ 反射执行  
→ 响应编码  
→ 客户端解码  
→ 返回本地结果

只要这个模型稳定，后面换 Netty、加注册中心、加负载均衡都只是替换流水线中的某一段实现。

## 5. Day1 关键代码跳转（按执行顺序）

1. 客户端入口：`ClientBootstrap.main`
2. 代理转换：`RpcClientProxy.create`
3. 请求模型：`RpcRequest`
4. 客户端发送：`RpcClient.sendRequest`
5. 编解码：`RpcMessageCodec`
6. 帧读写：`RpcIO`
7. 服务端入口：`ServerBootstrap.main`
8. 服务处理：`RpcServer.start` / `RpcServer.handle`
9. 执行还原：`RpcServer.invoke`
10. 响应模型：`RpcResponse`

## 6. 自测练习（检验你是否真的懂了）

### 练习 1：改返回值

把 `HelloServiceImpl#hello` 改成返回 `"[v2] hello " + name`，再跑一次。  
如果客户端输出变化，说明你理解了“服务端执行结果如何回传”。

### 练习 2：造一个失败请求

把客户端端口改为 9001。  
你会看到连接失败，说明你理解了“调用语义最终还是网络通信”。

### 练习 3：观察关键字段

在代理层打出 `requestId/methodName`，在服务端 `invoke` 前也打同样字段。  
如果两边一致，说明你理解了请求生命周期。
