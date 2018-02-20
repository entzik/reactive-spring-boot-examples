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
     *
     * @param filePart - the request part containing the file to be saved
     * @return a {@link Mono} representing the result of the operation
     */
    private Mono<String> saveFile(FilePart filePart) {
        LOGGER.info("handling file upload {}", filePart.filename());

        // if a file with the same name already exists in a repository, delete and recreate it
        final String filename = filePart.filename();
        File file = new File(filename);
        if (file.exists())
            file.delete();
        try {
            file.createNewFile();
        } catch (IOException e) {
            return Mono.error(e); // if creating a new file fails return an error
        }

        try {
            // create an async file channel to store the file on disk
            final AsynchronousFileChannel fileChannel = AsynchronousFileChannel.open(file.toPath(), StandardOpenOption.WRITE);

            final CloseCondition closeCondition = new CloseCondition();

            // pointer to the end of file offset
            AtomicInteger fileWriteOffset = new AtomicInteger(0);
            // error signal
            AtomicBoolean errorFlag = new AtomicBoolean(false);

            LOGGER.info("subscribing to file parts");
            // FilePart.content produces a flux of data buffers, each need to be written to the file
            return filePart.content().doOnEach(dataBufferSignal -> {
                if (dataBufferSignal.hasValue() && !errorFlag.get()) {
                    // read data from the incoming data buffer into a file array
                    DataBuffer dataBuffer = dataBufferSignal.get();
                    int count = dataBuffer.readableByteCount();
                    byte[] bytes = new byte[count];
                    dataBuffer.read(bytes);

                    // create a file channel compatible byte buffer
                    final ByteBuffer byteBuffer = ByteBuffer.allocate(count);
                    byteBuffer.put(bytes);
                    byteBuffer.flip();

                    // get the current write offset and increment by the buffer size
                    final int filePartOffset = fileWriteOffset.getAndAdd(count);
                    LOGGER.info("processing file part at offset {}", filePartOffset);
                    // write the buffer to disk
                    closeCondition.onTaskSubmitted();
                    fileChannel.write(byteBuffer, filePartOffset, null, new CompletionHandler<Integer, ByteBuffer>() {
                        @Override
                        public void completed(Integer result, ByteBuffer attachment) {
                            // file part successfuly written to disk, clean up
                            LOGGER.info("done saving file part {}", filePartOffset);
                            byteBuffer.clear();

                            if (closeCondition.onTaskCompleted())
                                try {
                                    LOGGER.info("closing after last part");
                                    fileChannel.close();
                                } catch (IOException ignored) {
                                    ignored.printStackTrace();
                                }
                        }

                        @Override
                        public void failed(Throwable exc, ByteBuffer attachment) {
                            // there as an error while writing to disk, set an error flag
                            errorFlag.set(true);
                            LOGGER.info("error saving file part {}", filePartOffset);
                        }
                    });
                }
            }).doOnComplete(() -> {
                // all done, close the file channel
                LOGGER.info("done processing file parts");
                if (closeCondition.canCloseOnComplete())
                    try {
                        LOGGER.info("closing after complete");
                        fileChannel.close();
                    } catch (IOException ignored) {
                    }

            }).doOnError(t -> {
                // ooops there was an error
                LOGGER.info("error processing file parts");
                try {
                    fileChannel.close();
                } catch (IOException ignored) {
                }
                // take last, map to a status string
            }).last().map(dataBuffer -> filePart.filename() + " " + (errorFlag.get() ? "error" : "uploaded"));
        } catch (IOException e) {
            // unable to open the file channel, return an error
            LOGGER.info("error opening the file channel");
            return Mono.error(e);
        }
    }
}