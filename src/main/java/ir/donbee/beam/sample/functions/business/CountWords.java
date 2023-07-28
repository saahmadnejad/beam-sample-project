package ir.donbee.beam.sample.functions.business;

import org.apache.beam.sdk.transforms.Count;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;

/**
 * A PTransform that converts a PCollection containing lines of text into a PCollection of
 * formatted word counts.
 *
 * <p>Concept #3: This is a custom composite transform that bundles two transforms (ParDo and
 * Count) as a reusable PTransform subclass. Using composite transforms allows for easy reuse,
 * modular testing, and an improved monitoring experience.
 */
public class CountWords
        extends PTransform<PCollection<String>, PCollection<KV<String, Long>>> {
    @Override
    public PCollection<KV<String, Long>> expand(PCollection<String> lines) {

        // Convert lines of text into individual words.
        PCollection<String> words = lines.apply(ParDo.of(new ExtractWordsFn()));

        // Count the number of times each word occurs.
        PCollection<KV<String, Long>> wordCounts = words.apply(Count.perElement());

        return wordCounts;
    }
}