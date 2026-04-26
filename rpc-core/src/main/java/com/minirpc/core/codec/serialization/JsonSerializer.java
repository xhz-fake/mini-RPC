package com.minirpc.core.codec.serialization;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.PropertyAccessor;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.json.JsonMapper;
import com.fasterxml.jackson.databind.jsontype.BasicPolymorphicTypeValidator;

import java.io.IOException;

public class JsonSerializer implements Serializer {
    //思路是：
    //- 借助 Jackson
    //- 走 JSON 文本格式的字节表示

    // Day7 这里用 Jackson 的默认类型信息，目的是让 Object 字段、Object[] 参数等也能正确还原。
    // 对这个学习项目来说，它能帮助我们把“序列化可切换”这件事先落地跑通。
    private final ObjectMapper objectMapper = JsonMapper.builder()
            .activateDefaultTyping(// 实际是在说：序列化时把实际类型也带上，反序列化时才能还原出来
                    BasicPolymorphicTypeValidator.builder()// 它是在告诉 Jackson：哪些多态子类型允许被反序列化
                            .allowIfSubType(Object.class)
                            .build(),
                    ObjectMapper.DefaultTyping.NON_FINAL// 对那些“不是 final 的类型”，自动附加类型信息
            )//- 这段配置是为了让 Jackson 在 JSON 里保留“对象实际类型信息”
             // - 这样反序列化时，才能把 Object 、 Object[] 这些模糊类型还原得更准
            .build()
            .setVisibility(PropertyAccessor.FIELD, JsonAutoDetect.Visibility.ANY);// - 让 Jackson 直接按“字段”维度去看对象，就算字段不是 public，也允许它读写
            //- 提高 JSON 序列化/反序列化对字段的兼容性
            //- 少依赖 JavaBean 规范那一套细节

    @Override
    public String name() {
        return "json";
    }

    @Override
    public byte[] serialize(Object obj) {
        try {
            return objectMapper.writeValueAsBytes(obj);
        } catch (IOException e) {
            throw new SerializationException("JSON 序列化失败", e);
        }
    }

    @Override
    public Object deserialize(byte[] bytes) {
        try {
            return objectMapper.readValue(bytes, Object.class);
            //- 正因为前面开了默认类型信息 activateDefaultTyping(...)
            //- JSON 里已经带了足够的类型提示
            //- 所以 Jackson 才能从 Object.class 这个宽入口，再恢复成具体对象类型
        } catch (IOException e) {
            throw new SerializationException("JSON 反序列化失败", e);
        }
    }
}
