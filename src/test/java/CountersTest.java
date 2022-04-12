import bdtc.lab1.CompositeKey;
import bdtc.lab1.CounterType;
import bdtc.lab1.HW1Reducer;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;


public class CountersTest {

    private ReduceDriver<CompositeKey, IntWritable, Text, Text> reduceDriver;
    private List<IntWritable> values = new ArrayList<>();

    private final CompositeKey unknownMetricKey = new CompositeKey(147, 1_510_670_880_000L);
    private final CompositeKey testMetricKey = new CompositeKey(2, 1_510_670_880_000L);

    @Before
    public void setUp() {
        HW1Reducer reducer = new HW1Reducer();
        reduceDriver = ReduceDriver.newReduceDriver(reducer);

        values.add(new IntWritable(50));
        values.add(new IntWritable(16));

        reduceDriver.getConfiguration().set("time_range", "min");
        // добавление кэш файла для теста
        URI cacheFile = new Path("src/test/resources/metrics.json").toUri();
        reduceDriver.addCacheFile(cacheFile);
    }

    @Test
    public void testReducerCounterOne() throws IOException  {
        reduceDriver
                .withInput(unknownMetricKey, values)
                .runTest();
        assertEquals("Expected 1 counter increment", 1, reduceDriver.getCounters()
                .findCounter(CounterType.UNKNOWN_METRIC).getValue());
    }

    @Test
    public void testReducerCounterZero() throws IOException {
        reduceDriver
                .withInput(testMetricKey, values)
                .withOutput(new Text("Node3RAM" + ", " + "1510670880000"),
                        new Text("min" + ", " + "33"))
                .runTest();
        assertEquals("Expected no counter increment", 0, reduceDriver.getCounters()
                .findCounter(CounterType.UNKNOWN_METRIC).getValue());
    }

    @Test
    public void testReducerCounters() throws IOException {
        reduceDriver
                .withInput(unknownMetricKey, values)
                .withInput(testMetricKey, values)
                .withInput(unknownMetricKey, values)
                .withOutput(new Text("Node3RAM" + ", " + "1510670880000"),
                        new Text("min" + ", " + "33"))
                .runTest();
        assertEquals("Expected 2 counter increment", 2, reduceDriver.getCounters()
                .findCounter(CounterType.UNKNOWN_METRIC).getValue());
    }

}

