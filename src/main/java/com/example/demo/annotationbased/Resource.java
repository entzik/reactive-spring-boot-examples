package com.example.demo.annotationbased;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.core.io.buffer.DataBuffer;
import org.springframework.http.MediaType;
import org.springframework.http.codec.multipart.FilePart;
import org.springframework.http.codec.multipart.Part;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.AsynchronousFileChannel;
import java.nio.channels.CompletionHandler;
import java.nio.file.StandardOpenOption;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

@RestController
@RequestMapping(path = "/api/files/upload/v1")
public class Resource {
    Logger LOGGER = LoggerFactory.getLogger(Resource.class);

    @RequestMapping(method = RequestMethod.POST, consumes = MediaType.MULTIPART_FORM_DATA_VALUE, produces = MediaType.APPLICATION_JSON_VALUE)
    public Flux<String> upload(@RequestBody Flux<Part> parts) {
        return parts
                .filter(part -> part instanceof FilePart)
                .ofType(FilePart.class)
                .flatMap(this::saveFile);
    }

    private Mono<String> saveFile(FilePart filePart) {
        LOGGER.info("handling file upload {}", filePart.filename());

        final String filename = filePart.filename();
        File file = new File(filename);
        if (file.exists())
            file.delete();
        try {
            file.createNewFile();
        } catch (IOException e) {
            return Mono.error(e);
        }


        try {
            AsynchronousFileChannel fileChannel = AsynchronousFileChannel.open(file.toPath(), StandardOpenOption.WRITE);

            AtomicInteger fileWriteOffset = new AtomicInteger(0);
            AtomicBoolean errorFlag = new AtomicBoolean(false);

            LOGGER.info("subscribing to file parts");
            return filePart.content().doOnEach(dataBufferSignal -> {
                if (dataBufferSignal.hasValue() && !errorFlag.get()) {
                    DataBuffer dataBuffer = dataBufferSignal.get();
                    int count = dataBuffer.readableByteCount();
                    byte[] bytes = new byte[count];
                    dataBuffer.read(bytes);

                    final ByteBuffer byteBuffer = ByteBuffer.allocate(count);
                    byteBuffer.put(bytes);
                    byteBuffer.flip();

                    final int filePartNumber = fileWriteOffset.getAndAdd(count);
                    LOGGER.info("processing file part at offset {}", filePartNumber);
                    fileChannel.write(byteBuffer, filePartNumber, null, new CompletionHandler<Integer, ByteBuffer>() {
                        @Override
                        public void completed(Integer result, ByteBuffer attachment) {
                            LOGGER.info("done saving file part {}", filePartNumber);
                            byteBuffer.clear();
                        }

                        @Override
                        public void failed(Throwable exc, ByteBuffer attachment) {
                            errorFlag.set(true);
                            LOGGER.info("error saving file part {}", filePartNumber);
                            throw new IllegalStateException(exc);
                        }
                    });
                }
            }).doOnComplete(() -> {
                LOGGER.info("done processing file parts");
                try {
                    fileChannel.close();
                } catch (IOException ignored) {
                }

            }).doOnError(t -> {
                LOGGER.info("error processing file parts");
                try {
                    fileChannel.close();
                } catch (IOException ignored) {
                }

            }).last().map(dataBuffer -> filePart.filename() + " " + (errorFlag.get() ? "error" : "uploaded"));
/*
            filePart.content()
                    .subscribe(dataBuffer -> { // onNext
                        int count = dataBuffer.readableByteCount();
                        byte[] bytes = new byte[count];
                        dataBuffer.read(bytes);

                        final ByteBuffer buffer = ByteBuffer.allocate(count);
                        buffer.put(bytes);
                        buffer.flip();

                        final int filePartNumber = atomicInteger.getAndAdd(count);
                        LOGGER.info("processing file part {}", filePartNumber);
                        fileChannel.write(buffer, filePartNumber, null, new CompletionHandler<Integer, ByteBuffer>() {
                            @Override
                            public void completed(Integer result, ByteBuffer attachment) {
                                LOGGER.info("done saving file part {}", filePartNumber);
                                buffer.clear();
                            }

                            @Override
                            public void failed(Throwable exc, ByteBuffer attachment) {
                                LOGGER.info("error saving file part {}", filePartNumber);
                                throw new IllegalStateException(exc);
                            }
                        });
                    }, cause -> { // onError
                        LOGGER.info("done processing file parts with error");
                        try {
                            fileChannel.close();
                        } catch (IOException ignored) {}
                        processor.onError(cause);
                    }, () -> { // onComplete
                        LOGGER.info("done processing file parts");
                        try {
                            fileChannel.close();
                        } catch (IOException ignored) {}
                        processor.onComplete();
                    });
*/
        } catch (IOException e) {
            LOGGER.info("error opening the file channel");
            return Mono.error(e);
        }

//        LOGGER.info("returning processor");
//        return processor;
    }

/*
    private Mono<Void> saveFile(FilePart filePart) {
        File dest = new File("/tmp/" + filePart.filename());
        if (!dest.exists())
            try {
                dest.createNewFile();
            } catch (IOException e) {
                e.printStackTrace();
            }
        return filePart.transferTo(dest);
    }
*/
}