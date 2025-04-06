package org.example;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.connector.base.DeliveryGuarantee;

public class DataStreamJob {

	public static void main(String[] args) throws Exception {
		// Sets up the execution environment
		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		// Define your source
		DataStream<String> dataStream = env.socketTextStream("localhost", 9999);

		// Define some transformation
		DataStream<String> transformedStream = dataStream.map(String::toLowerCase);

		// Define Kafka Sink configuration using the new KafkaSink API
		KafkaSink<String> kafkaSink = KafkaSink.<String>builder()
				.setBootstrapServers("localhost:9095")
				.setRecordSerializer(KafkaRecordSerializationSchema.builder()
						.setTopic("alerts")
						.setValueSerializationSchema(new SimpleStringSchema())  // Serialize String as value
						.build())
				.setDeliverGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
				.build();

		// Add the sink to the transformed stream
		transformedStream.sinkTo(kafkaSink);

		// Execute the Flink job
		env.execute("Flink Streaming Job");
	}
}
