import bdtc.lab1.CompositeKey;
import bdtc.lab1.HW1Mapper;
import bdtc.lab1.HW1Reducer;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.List;


public class MapReduceTest {

    private MapDriver<LongWritable, Text, CompositeKey, IntWritable> mapDriver;
    private ReduceDriver<CompositeKey, IntWritable, Text, Text> reduceDriver;
    private MapReduceDriver<LongWritable, Text, CompositeKey, IntWritable, Text, Text> mapReduceDriver;

    private final String testMetric = "2, 1510670916249, 50\n";

    @Before
    public void setUp() {
        HW1Mapper mapper = new HW1Mapper();
        HW1Reducer reducer = new HW1Reducer();
        mapDriver = MapDriver.newMapDriver(mapper);
        reduceDriver = ReduceDriver.newReduceDriver(reducer);
        mapReduceDriver = MapReduceDriver.newMapReduceDriver(mapper, reducer);
        // задаём диапазон времени для агрегации
        mapDriver.getConfiguration().set("time_range", "min");
        reduceDriver.getConfiguration().set("time_range", "min");
        mapReduceDriver.getConfiguration().set("time_range", "min");
        // добавление кэш файла для теста
        URI cacheFile = new Path("src/test/resources/metrics.json").toUri();
        mapDriver.addCacheFile(cacheFile);
        reduceDriver.addCacheFile(cacheFile);
        mapReduceDriver.addCacheFile(cacheFile);
    }

    @Test
    public void testMapper() throws IOException {
        mapDriver
                .withInput(new LongWritable(), new Text(testMetric))
                .withOutput(new CompositeKey(2, 1_510_670_880_000L),
                        new IntWritable(50))
                .runTest();
    }

    @Test
    public void testReducer() throws IOException, URISyntaxException {
        List<IntWritable> values = new ArrayList<>();
        values.add(new IntWritable(50));
        values.add(new IntWritable(16));
        reduceDriver
                .withInput(new CompositeKey(2, 1_510_670_880_000L), values)
                .withOutput(new Text("Node3RAM" + ", " + "1510670880000"),
                        new Text("min" + ", " + "33"))
                .runTest();
    }

    @Test
    public void testMapReduce() throws IOException, URISyntaxException {
        String testMetric_2 = "2, 1510670916958, 16\n";
        mapReduceDriver
                .withInput(new LongWritable(), new Text(testMetric))
                .withInput(new LongWritable(), new Text(testMetric_2))
                .withOutput(new Text("Node3RAM" + ", " + "1510670880000"),
                        new Text("min" + ", " + "33"))
                .runTest();
    }
}
