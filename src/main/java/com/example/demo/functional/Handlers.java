package com.example.demo.functional;

import org.springframework.web.reactive.function.server.HandlerFunction;
import org.springframework.web.reactive.function.server.ServerResponse;
import reactor.core.publisher.Mono;

public class Handlers {
    public static final HandlerFunction<ServerResponse> SAY_HELLO = serverRequest -> ServerResponse.ok().body(Mono.just("Hello World!"), String.class);
/*
    public static final HandlerFunction<ServerResponse> UPLOAD = request ->
            request.body(BodyExtractors.toMultipartData()).flatMap(parts -> {
                Map<String, Part> map = parts.toSingleValueMap();
                FilePart filePart = (FilePart) map.get("files");
                filePart.transferTo(new File(dir + "/" + filePart.getFilename().get()));
                return ServerResponse.ok().body(map.get("submit-name").getContentAsString(), String.class);
            }
*/

}
