import java.io.BufferedReader;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.json.JSONArray;
import org.json.JSONObject;

public class DataProducer {
    private Producer<String, String> producer;
    private String traceFileName;
    private Map<String, Integer> partitionCounts;

    public DataProducer(Producer producer, String traceFileName) {
        this.producer = producer;
        this.traceFileName = traceFileName;
        try {
            this.partitionCounts = loadPartitionCounts("topic_design.json");
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }

    /**
     * Task 1:
     * In Task 1, you need to read the content in the tracefile we give to you,
     * create two streams, and feed the messages in the tracefile to different
     * streams based on the value of "type" field in the JSON string.
     * 
     * Please note that you're working on an ec2 instance, but the streams should
     * be sent to your flink cluster. Make sure you can consume the topics on the
     * master node of your samza cluster before you make a submission.
     */

    // Helper function to convert "type" field into the Kafka topic
    static String topicFor(String type) {
        switch (type) {
            case "DRIVER_LOCATION":
                return "driver-locations";
            case "ENTERING_BLOCK":
            case "LEAVING_BLOCK":
            case "RIDE_REQUEST":
            case "RIDE_COMPLETE":
                return "events";
            case "PAYMENT_INITIATED":
            case "PAYMENT_CONFIRMED":
                return "payments";
            default:
                throw new IllegalArgumentException("Unknown event type: " + type);
        }
    }

    // Helper function to read partition data form topic_design.json
    static Map<String, Integer> loadPartitionCounts(String designFile) throws IOException {
        String json = new String(Files.readAllBytes(Paths.get(designFile)), StandardCharsets.UTF_8);
        JSONArray topics = new JSONObject(json).getJSONArray("topics");
        Map<String, Integer> counts = new HashMap<>();
        for (int i = 0; i < topics.length(); i++) {
            JSONObject t = topics.getJSONObject(i);
            int n = t.getInt("partition_count");
            if (n <= 0) {
                throw new IllegalArgumentException("partition_count must be positive for "
                        + t.getString("topic_name"));
            }
            counts.put(t.getString("topic_name"), n);
        }
        return counts;
    }

    public void sendData() {
        try (BufferedReader reader = Files.newBufferedReader(Paths.get(traceFileName), StandardCharsets.UTF_8)) {
            String line;
            while ((line = reader.readLine()) != null) {
                // Skip empty lines
                if (line.trim().isEmpty()) {
                    continue;
                }

                // Convert to JSON and read "type" field
                JSONObject record = new JSONObject(line);
                String topic = topicFor(record.getString("type"));

                int partition = Math.floorMod(record.getInt("blockId"), partitionCounts.get(topic));

                // Send data
                producer.send(new ProducerRecord<>(topic, partition, null, line), (metadata, e) -> {
                    if (e != null) {
                        e.printStackTrace();
                    }
                });

            }
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }
}
