package com.minirpc.server;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class ServiceRegistry {
    private final Map<String, Object> services = new ConcurrentHashMap<>();

    public void register(Class<?> interfaceClass, Object impl) {
        services.put(interfaceClass.getName(), impl);
    }

    public Object getService(String interfaceName) {
        return services.get(interfaceName);
    }
}
