import org.apache.kafka.clients.producer.MockProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.json.JSONObject;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

public class DataProducerTest {
    private static final String TESTTRACE = "test_trace";

    private MockProducer<String, String> producer;

    @Before
    public void setUp() {
        producer = new MockProducer<>(
                true, new StringSerializer(), new StringSerializer());
    }

    private List<ProducerRecord<String, String>> send() throws IOException {
        new DataProducer(producer, TESTTRACE).sendData();
        return producer.history();
    }

    private static int blockId(ProducerRecord<String, String> r) {
        return new JSONObject(r.value()).getInt("blockId");
    }

    private static String type(ProducerRecord<String, String> r) {
        return new JSONObject(r.value()).getString("type");
    }

    private static List<ProducerRecord<String, String>> ofType(
            List<ProducerRecord<String, String>> history, String type) {
        return history.stream().filter(r -> type(r).equals(type)).collect(Collectors.toList());
    }

    private static int partitionOf(List<ProducerRecord<String, String>> history,
            String topic, int blockId) {
        return history.stream()
                .filter(r -> r.topic().equals(topic) && blockId(r) == blockId)
                .findFirst()
                .orElseThrow(AssertionError::new)
                .partition();
    }

    /**
     * This test checks if the messages go to the correct topic and partition as
     * required.
     * Additional test cases can be added by adding more entries to test_trace and
     * verifying here.
     * 
     * @throws IOException
     */
    @Test
    public void testProducer() throws IOException {
        List<ProducerRecord<String, String>> expected = Arrays.asList(
                // 5648 % 27 = 5
                new ProducerRecord<>("events", 5, null, "{\"blockId\":5648,\"type\":\"ENTERING_BLOCK\"}"),
                // 5649 % 20 = 9
                new ProducerRecord<>("driver-locations", 9, null, "{\"blockId\":5649,\"type\":\"DRIVER_LOCATION\"}"),
                // 1930 % 27 = 13
                new ProducerRecord<>("events", 13, null,
                        "{\"blockId\":1930,\"clientId\":6343,\"type\":\"RIDE_REQUEST\"}"),
                // 42 % 13 = 3
                new ProducerRecord<>("payments", 3, null, "{\"blockId\":42,\"type\":\"PAYMENT_INITIATED\"}"),
                // 5649 % 20 = 9
                new ProducerRecord<>("driver-locations", 9, null,
                        "{\"blockId\":5649,\"driverId\":2,\"type\":\"DRIVER_LOCATION\"}"),
                // 5669 % 20 = 9
                new ProducerRecord<>("driver-locations", 9, null, "{\"blockId\":5669,\"type\":\"DRIVER_LOCATION\"}"),
                // 5675 % 27 = 5
                new ProducerRecord<>("events", 5, null, "{\"blockId\":5675,\"type\":\"RIDE_COMPLETE\"}"),
                // 43 % 13 = 4
                new ProducerRecord<>("payments", 4, null, "{\"blockId\":43,\"type\":\"PAYMENT_CONFIRMED\"}"),
                // 5648 % 27 = 5
                new ProducerRecord<>("events", 5, null, "{\"blockId\":5648,\"type\":\"LEAVING_BLOCK\"}"));

        Assert.assertEquals("Producer records not matched!", expected, send());
    }

    // Verify events are sent to correct topic
    @Test
    public void driverLocationRoutedToDriverLocations() throws IOException {
        List<ProducerRecord<String, String>> history = send();
        for (ProducerRecord<String, String> r : history) {
            Assert.assertEquals(r.value(),
                    type(r).equals("DRIVER_LOCATION"), r.topic().equals("driver-locations"));
        }
        Assert.assertEquals(3, ofType(history, "DRIVER_LOCATION").size());
    }

    @Test
    public void rideRequestRoutedToEvents() throws IOException {
        List<ProducerRecord<String, String>> requests = ofType(send(), "RIDE_REQUEST");
        Assert.assertEquals(1, requests.size());
        Assert.assertEquals("events", requests.get(0).topic());
    }

    @Test
    public void paymentEventsRoutedToPayments() throws IOException {
        List<ProducerRecord<String, String>> history = send();
        List<ProducerRecord<String, String>> payments = ofType(history, "PAYMENT_INITIATED");
        payments.addAll(ofType(history, "PAYMENT_CONFIRMED"));
        Assert.assertEquals(2, payments.size());
        for (ProducerRecord<String, String> r : payments) {
            Assert.assertEquals("payments", r.topic());
        }
    }

    @Test
    public void sameBlockIdSamePartition() throws IOException {
        List<ProducerRecord<String, String>> history = send();
        // Two DRIVER_LOCATION records for block 5649.
        List<Integer> dl5649 = history.stream()
                .filter(r -> r.topic().equals("driver-locations") && blockId(r) == 5649)
                .map(ProducerRecord::partition).distinct().collect(Collectors.toList());
        Assert.assertEquals(Arrays.asList(9), dl5649);
        // ENTERING_BLOCK and LEAVING_BLOCK for block 5648.
        List<Integer> ev5648 = history.stream()
                .filter(r -> r.topic().equals("events") && blockId(r) == 5648)
                .map(ProducerRecord::partition).distinct().collect(Collectors.toList());
        Assert.assertEquals(Arrays.asList(5), ev5648);
    }

    @Test
    public void differentBlockIdSamePartition() throws IOException {
        List<ProducerRecord<String, String>> history = send();
        // 5649 ≡ 5669 (mod 20)
        Assert.assertEquals(partitionOf(history, "driver-locations", 5649),
                partitionOf(history, "driver-locations", 5669));
        // 5648 ≡ 5675 (mod 27)
        Assert.assertEquals(partitionOf(history, "events", 5648),
                partitionOf(history, "events", 5675));
    }
}
