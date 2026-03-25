package com.minirpc.demo.service;

public class HelloServiceImpl implements HelloService {// 是真实的业务实现
    @Override
    public String hello(String name) {
        return "hello " + name;
    }//这个项目的价值不在“这句输出结果本身”，而在于你借这句最简单的业务，练习并证明了一整套分布式调用底层机制
}
