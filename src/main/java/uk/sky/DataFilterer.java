package uk.sky;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.Reader;
import java.util.*;
import java.util.function.Consumer;
import java.util.regex.MatchResult;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.StreamSupport;

import static java.lang.String.format;
import static java.util.stream.Collectors.toSet;

public class DataFilterer {
    private static final Pattern logExtractRegex = Pattern.compile("([\\d]+),([A-Z]+),([\\d]+)");

    public static Collection<?> filterByCountry(Reader source, String country) {
        final List<String> result = new ArrayList<>();
        final List<String> lines = getLinesFromReader(source);
        for (String line : lines) {
            final Set<String> countriesSet = StreamSupport.stream(new MatcherSpliterator(logExtractRegex.matcher(line)), false)
                    .filter(matchResult -> matchResult.group(2).equalsIgnoreCase(country))
                    .map(matchResult -> format("%s,%s,%s", matchResult.group(1), matchResult.group(2), matchResult.group(3)))
                    .collect(toSet());
            result.addAll(countriesSet);
        }

        return result;
    }

    public static Collection<?> filterByCountryWithResponseTimeAboveLimit(Reader source, String country, long limit) {
        final List<String> result = new ArrayList<>();
        final List<String> lines = getLinesFromReader(source);
        for (String line : lines) {
            final Set<String> countriesSet = StreamSupport.stream(new MatcherSpliterator(logExtractRegex.matcher(line)), false)
                        .filter(matchResult -> matchResult.group(2).equalsIgnoreCase(country))
                        .filter(matchResult -> new Long(matchResult.group(3)) > limit)
                        .map(matchResult -> format("%s,%s,%s", matchResult.group(1), matchResult.group(2), matchResult.group(3)))
                        .collect(toSet());
            result.addAll(countriesSet);
        }

        return result;
    }

    public static Collection<?> filterByResponseTimeAboveAverage(Reader source) {
        final List<String> result = new ArrayList<>();
        final List<String> lines = getLinesFromReader(source);
        long sum = 0;
        for (String line : lines) {
            sum += StreamSupport.stream(new MatcherSpliterator(logExtractRegex.matcher(line)), false)
                    .mapToLong(matchResult -> new Long(matchResult.group(3)))
                    .sum();
        }
        final long average = sum / (lines.size() - 1);
        for (String line : lines) {
            final Set<String> aboveAverageRequests = StreamSupport.stream(new MatcherSpliterator(logExtractRegex.matcher(line)), false)
                    .filter(matchResult -> new Long(matchResult.group(3)) > average)
                    .map(matchResult -> format("%s,%s,%s", matchResult.group(1), matchResult.group(2), matchResult.group(3)))
                    .collect(toSet());
            result.addAll(aboveAverageRequests);
        }

        return result;
    }


    private static List<String> getLinesFromReader(Reader source) {
        final BufferedReader br = new BufferedReader(source);
        final List<String> lines = new ArrayList<>();
        String line;
        try {
            while ((line = br.readLine()) != null) {
                lines.add(line);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        return lines;
    }

    private static class MatcherSpliterator extends Spliterators.AbstractSpliterator<MatchResult> {

        private final Matcher matcher;

        public MatcherSpliterator(Matcher matcher) {
            super(Long.MAX_VALUE, ORDERED | NONNULL | IMMUTABLE);
            this.matcher = matcher;
        }

        @Override
        public boolean tryAdvance(Consumer<? super MatchResult> action) {
            if (matcher.find()) {
                action.accept(matcher.toMatchResult());
                return true;
            }

            return false;
        }
    }

}