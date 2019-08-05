package com.example.mqtt.netty.config;

import lombok.Data;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

@Component
@Data
public class BrokerConfig {

    @Value("${broker.port}")
    private int port;


}
