package ir.donbee.beam.sample.functions.business;

import org.apache.beam.examples.common.ExampleUtils;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Distribution;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.transforms.DoFn;

/**
 * Concept #2: You can make your pipeline assembly code less verbose by defining your DoFns
 * statically out-of-line. This DoFn tokenizes lines of text into individual words; we pass it to
 * a ParDo in the pipeline.
 */
public class ExtractWordsFn extends DoFn<String, String> {
    private final Counter emptyLines = Metrics.counter(ExtractWordsFn.class, "emptyLines");
    private final Distribution lineLenDist =
            Metrics.distribution(ExtractWordsFn.class, "lineLenDistro");

    @ProcessElement
    public void processElement(@Element String element, OutputReceiver<String> receiver) {
        lineLenDist.update(element.length());
        if (element.trim().isEmpty()) {
            emptyLines.inc();
        }

        // Split the line into words.
        String[] words = element.split(ExampleUtils.TOKENIZER_PATTERN, -1);

        // Output each word encountered into the output PCollection.
        for (String word : words) {
            if (!word.isEmpty()) {
                receiver.output(word);
            }
        }
    }
}