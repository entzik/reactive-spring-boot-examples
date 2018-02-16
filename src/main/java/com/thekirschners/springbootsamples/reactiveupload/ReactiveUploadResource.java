package com.thekirschners.springbootsamples.reactiveupload;

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

/**
 * example to demonstrate fully reactive file upload using using SpringBoot2 WebFlux and NIO {@link AsynchronousFileChannel}
 */
@RestController
@RequestMapping(path = "/api/reactive-upload")
public class ReactiveUploadResource {
    Logger LOGGER = LoggerFactory.getLogger(ReactiveUploadResource.class);

    /**
     * upload handler method, mapped to POST. Like any file upload handler it consumes MULTIPART_FORM_DATA.
     * Produces a JSON respomse
     *
     * @param parts a flux providing all part contained in the MULTIPART_FORM_DATA request
     * @return a flux of results - one element per uploaded file
     */
    @RequestMapping(method = RequestMethod.POST, consumes = MediaType.MULTIPART_FORM_DATA_VALUE, produces = MediaType.APPLICATION_JSON_VALUE)
    public Flux<String> uploadHandler(@RequestBody Flux<Part> parts) {
        return parts
                .filter(part -> part instanceof FilePart) // only retain file parts
                .ofType(FilePart.class) // convert the flux to FilePart
                .flatMap(this::saveFile); // save each file and flatmap it to a flux of results
    }

    /**
     * tske a {@link FilePart}, transfer it to disk using {@link AsynchronousFileChannel}s and return a {@link Mono} representing the result
     * @param filePart - the request part containing the file to be saved
     * @return a {@link Mono} representing the result of the operation
     */
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
        } catch (IOException e) {
            LOGGER.info("error opening the file channel");
            return Mono.error(e);
        }
    }
}