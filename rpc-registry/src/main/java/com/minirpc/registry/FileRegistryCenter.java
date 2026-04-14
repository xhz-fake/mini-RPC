package com.minirpc.registry;

import java.io.IOException;
import java.io.StringReader;
import java.io.StringWriter;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.OpenOption;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Objects;
import java.util.Properties;
import java.util.Set;

public class FileRegistryCenter implements RegistryCenter {// 它是一个“教学版注册中心， - 服务端启动时，它负责把地址写进去 - 客户端调用前，它负责把地址读出来
    // 允许通过 JVM 参数自定义注册中心文件路径：
    // -Drpc.registry.file=D:/tmp/my-registry.properties
    private static final String REGISTRY_FILE_PROPERTY = "rpc.registry.file";

    // 这个 Path 指向“注册中心文件”的真实位置。
    // Day4 说的“文件注册中心”，本质上就是把服务实例信息写进这个文件里。
    private final Path registryFile;

    public FileRegistryCenter() {// - 读取 JVM 参数 rpc.registry.file - 如果你没配，就走默认路径 - 最后把路径变成一个 Path 对象交给另一个构造器
        // 默认文件位置放在系统临时目录下。
        // 例如在 Windows 上，通常会落到类似：
        // C:/Users/你的用户名/AppData/Local/Temp/mini-rpc-registry/services.properties
        this(Paths.get(System.getProperty(
                REGISTRY_FILE_PROPERTY,
                Paths.get(System.getProperty("java.io.tmpdir"), "mini-rpc-registry", "services.properties").toString()
        )));
    }

    public FileRegistryCenter(Path registryFile) {// 它只是把这个路径保存到成员变量里：
        this.registryFile = Objects.requireNonNull(registryFile, "registryFile");
    }

    @Override
    public void register(ServiceInstance instance) {
        // register 的动作可以理解为：
        // “把这个服务实例的地址，追加登记到 serviceName 对应的地址列表里”。
        updateRegistry(instance.getServiceName(), endpoints -> endpoints.add(serializeEndpoint(instance)));// 那再注册一次不会重复加，因为底层用的是 Set 去重。
    }

    @Override
    public void unregister(ServiceInstance instance) {
        // unregister 就是反过来：
        // “服务下线了，把它的地址从地址列表里删掉”。
        updateRegistry(instance.getServiceName(), endpoints -> endpoints.remove(serializeEndpoint(instance)));
    }

    @Override
    public List<ServiceInstance> discover(String serviceName) {// 把文件内容变成 instances
        // 第一步：先把整个注册中心文件读进内存。
        Properties properties = loadProperties();// Properties 对象，里面是整份注册中心的内容
        // 第二步：取出指定服务名对应的值。
        // 例如 raw 可能是："127.0.0.1:9000,127.0.0.1:9010"
        String raw = properties.getProperty(serviceName, "");
        if (raw.isBlank()) {
            return List.of();// 如果没查到，返回空列表
        }
        // 第三步：把字符串形式的地址列表，逐个还原成 ServiceInstance 对象。
        List<ServiceInstance> instances = new ArrayList<>();
        for (String endpoint : raw.split(",")) {// 会得到：127.0.0.1:9000 以及 127.0.0.1:9010
            if (endpoint.isBlank()) {
                continue;
            }
            String[] hostPort = endpoint.trim().split(":");// 每个 endpoint 再拆成 host 和 port
            if (hostPort.length != 2) {
                continue;
            }
            instances.add(new ServiceInstance(serviceName, hostPort[0], Integer.parseInt(hostPort[1])));
        }
        return instances;
    }

    private void updateRegistry(String serviceName, EndpointUpdater updater) {
        /*
        1. 先保证文件存在
        2. 打开文件
        3. 加锁
        4. 读取旧内容
        5. 找到这个服务原来的地址列表
        6. 按外部传入的动作去修改这个列表
        7. 把修改后的结果写回文件
        */
        // 先保证目录和文件存在。
        ensureRegistryFile();
        OpenOption[] options = new OpenOption[]{
                StandardOpenOption.READ,
                StandardOpenOption.WRITE,
                StandardOpenOption.CREATE
        };
        try (FileChannel channel = FileChannel.open(registryFile, options);// FileChannel channel表示： 我现在怎么去操作这个文件，
             // 根据这个文件路径，把这个文件真正打开，并返回一个“可读可写的操作通道”
             // 加独占锁，避免多个进程同时改文件导致内容互相覆盖。
             FileLock ignored = channel.lock()) {
            // 先读取“当前旧内容”。
            Properties properties = loadProperties(channel);// 这一步会把文件里的所有 key=value 读进内存。
            // 用 LinkedHashSet 既能去重，又能保留写回时的顺序。
            Set<String> endpoints = new LinkedHashSet<>();
            String existing = properties.getProperty(serviceName, "");// 先看这个服务原来已经登记了哪些地址
            if (!existing.isBlank()) {
                for (String endpoint : existing.split(",")) {
                    if (!endpoint.isBlank()) {
                        endpoints.add(endpoint.trim());
                    }
                }
            }
            // 这里不直接写死“增”或“删”，而是把如何修改 endpoints 交给外面传进来的 updater。
            updater.update(endpoints);
            if (endpoints.isEmpty()) {
                // 如果这个服务已经没有任何实例了，就把整条记录删除。
                properties.remove(serviceName);
            } else {
                // 否则把最新地址列表重新拼成逗号分隔字符串写回去。
                properties.setProperty(serviceName, String.join(",", endpoints));
            }
            storeProperties(channel, properties);
        } catch (IOException e) {
            throw new RuntimeException("更新注册中心失败: " + registryFile, e);
        }
    }

    private Properties loadProperties() {
        ensureRegistryFile();// 保证文件存在
        try (FileChannel channel = FileChannel.open(registryFile, StandardOpenOption.READ);// 以只读方式打开文件
             // 读文件时加共享锁，避免读到一半别人正好在改。
             FileLock ignored = channel.lock(0L, Long.MAX_VALUE, true)) {// 避免脏读,解决隔离级别：读已提交
            return loadProperties(channel);//
        } catch (IOException e) {
            throw new RuntimeException("读取注册中心失败: " + registryFile, e);
        }
    }

    private Properties loadProperties(FileChannel channel) {
        Properties properties = new Properties();
        try {
            // 每次都从文件头开始读，避免复用 channel 时读偏了位置。
            channel.position(0);
            byte[] bytes = new byte[(int) channel.size()];
            java.nio.ByteBuffer buffer = java.nio.ByteBuffer.wrap(bytes);
            while (buffer.hasRemaining()) {
                channel.read(buffer);// 循环把字节读进来
            }
            if (bytes.length == 0) {
                // 文件是空的，就返回空 Properties。
                return properties;
            }
            // 把磁盘里的文本内容解析成 key=value 结构。
            properties.load(new StringReader(new String(bytes, StandardCharsets.UTF_8)));
            return properties;
        } catch (IOException e) {
            throw new RuntimeException("读取注册中心失败: " + registryFile, e);
        }
    }

    private void storeProperties(FileChannel channel, Properties properties) {
        try {
            StringWriter writer = new StringWriter();
            // Properties.store(...) 最终会生成一段文本，格式就是常见的 properties 文件格式。
            properties.store(writer, "mini-rpc registry");
            byte[] bytes = writer.toString().getBytes(StandardCharsets.UTF_8);
            // 先清空旧内容，再从头写入新内容。
            channel.truncate(0);// 先把旧文件清空
            channel.position(0);
            channel.write(java.nio.ByteBuffer.wrap(bytes));
            // force(true) 表示尽量把数据刷到磁盘，减少“看起来写了但实际上还没落盘”的风险。
            channel.force(true);
        } catch (IOException e) {
            throw new RuntimeException("写入注册中心失败: " + registryFile, e);
        }
    }

    private void ensureRegistryFile() {
        try {
            // 先创建目录，再创建文件。
            Files.createDirectories(registryFile.getParent());
            if (!Files.exists(registryFile)) {
                Files.createFile(registryFile);
            }
        } catch (IOException e) {
            throw new RuntimeException("初始化注册中心文件失败: " + registryFile, e);
        }
    }

    private String serializeEndpoint(ServiceInstance instance) {
        // 写进文件时只保存地址部分，格式统一为 host:port。
        return instance.getHost() + ":" + instance.getPort();
    }

    @FunctionalInterface
    private interface EndpointUpdater {
        // 这是一个“小钩子”接口：
        // 外面可以传入“往集合里 add”或“从集合里 remove”的动作，
        // updateRegistry 只负责公共流程：读文件、改集合、再写回文件。
        void update(Set<String> endpoints);
    }
}
