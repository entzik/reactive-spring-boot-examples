package com.example.demo;

import com.example.demo.functional.Handlers;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.web.reactive.function.server.RequestPredicates;
import org.springframework.web.reactive.function.server.RouterFunction;
import org.springframework.web.reactive.function.server.RouterFunctions;
import org.springframework.web.reactive.function.server.ServerResponse;

@SpringBootApplication
public class DemoApplication {

	@Bean
	RouterFunction<ServerResponse> helloRouterFunction() {
		return RouterFunctions.route(RequestPredicates.GET("/sayhello"), Handlers.SAY_HELLO);
	}

	public static void main(String[] args) {
		SpringApplication.run(DemoApplication.class, args);
	}
}
