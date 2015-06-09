package uk.sky;

import org.junit.Test;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.util.Collection;

import static org.junit.Assert.*;

public class DataFiltererTest {

    @Test
    public void whenEmpty() throws FileNotFoundException {
        assertTrue(DataFilterer.filterByCountry(openFile("src/test/resources/empty"), "GB").isEmpty());
    }

    @Test
    public void whenSingleLine() throws FileNotFoundException {
        final Collection<?> result = DataFilterer.filterByCountry(openFile("src/test/resources/single-line"), "GB");
        assertNotNull(result);
        assertEquals(1, result.size());
        assertEquals("1431592497,GB,200", result.iterator().next());
    }

    @Test
    public void whenMultipleLinesAndCountryGB() throws FileNotFoundException {
        final Collection<?> result = DataFilterer.filterByCountry(openFile("src/test/resources/multi-lines"), "GB");
        assertNotNull(result);
        assertEquals(2, result.size());
        assertTrue(result.contains("1432917066,GB,37"));
        assertTrue(result.contains("1432364089,GB,515"));
    }

    @Test
    public void whenMultipleLinesAndCountryUS() throws FileNotFoundException {
        final Collection<?> result = DataFilterer.filterByCountry(openFile("src/test/resources/multi-lines"), "US");
        assertNotNull(result);
        assertEquals(3, result.size());
        assertTrue(result.contains("1433190845,US,539"));
        assertTrue(result.contains("1433666287,US,789"));
        assertTrue(result.contains("1432484176,US,850"));
    }

    @Test
    public void whenMultipleLinesAndCountryNotFound() throws FileNotFoundException {
        final Collection<?> result = DataFilterer.filterByCountry(openFile("src/test/resources/multi-lines"), "NONE");
        assertNotNull(result);
        assertTrue(result.isEmpty());
    }

    @Test
    public void whenMultipleLinesAndCountryGBAndResponseTimeAboveLimit() throws FileNotFoundException {
        final Collection<?> result = DataFilterer.filterByCountryWithResponseTimeAboveLimit(openFile("src/test/resources/multi-lines"), "GB", 600);
        assertNotNull(result);
        assertTrue(result.isEmpty());
    }

    @Test
    public void whenMultipleLinesAndCountryUSAndResponseTimeAboveLimit() throws FileNotFoundException {
        final Collection<?> result = DataFilterer.filterByCountryWithResponseTimeAboveLimit(openFile("src/test/resources/multi-lines"), "US", 600);
        assertNotNull(result);
        assertEquals(2, result.size());
        assertTrue(result.contains("1433666287,US,789"));
        assertTrue(result.contains("1432484176,US,850"));
    }

    @Test
    public void whenMultipleLinesAndResponseTimeAboveAverage() throws FileNotFoundException {
        final Collection<?> result = DataFilterer.filterByResponseTimeAboveAverage(openFile("src/test/resources/multi-lines"));
        assertNotNull(result);
        assertEquals(4, result.size());
        assertTrue(result.contains("1433190845,US,539"));
        assertTrue(result.contains("1433666287,US,789"));
        assertTrue(result.contains("1432484176,US,850"));
        assertTrue(result.contains("1432364090,DE,615"));
    }



    private FileReader openFile(String filename) throws FileNotFoundException {
        return new FileReader(new File(filename));
    }
}
