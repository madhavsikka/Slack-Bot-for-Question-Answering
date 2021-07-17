package com.slack.kafka;

import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.util.EntityUtils;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.json.JSONArray;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class gpt3Utilities {

    Logger logger = LoggerFactory.getLogger(gpt3Utilities.class.getName());
    String apiKey = "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";
    String openAiURL = "https://api.openai.com/v1/engines/davinci/completions";

    public KafkaStreams createNewKafkaStream(String topicName) {

        Properties properties = new Properties();
        properties.setProperty(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        properties.setProperty(StreamsConfig.APPLICATION_ID_CONFIG, "slack-kafka-stream");
        properties.setProperty(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.StringSerde.class.getName());
        properties.setProperty(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.StringSerde.class.getName());
        properties.setProperty(StreamsConfig.POLL_MS_CONFIG, "10");
        properties.setProperty(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, "10");

        StreamsBuilder builder = new StreamsBuilder();
        KStream<String, String> stream = builder.stream(topicName);
        KStream<String, String>[] branches = stream.branch(
                (id, msg) -> {
                    String sentiment = getSentimentFromGpt3(msg);
                    logger.info("~~~Stream: || " + sentiment + " ||" + msg);
                    return sentiment.equals("positive");
                },
                (id, msg) -> getSentimentFromGpt3(msg).equals("negative")
        );
        branches[0].to("positive_" + topicName);
        branches[1].to("negative_" + topicName);

        KafkaStreams kafkaStreams = new KafkaStreams(
                builder.build(),
                properties
        );
        return kafkaStreams;
    }

    public String getSentimentFromGpt3(String msg) {

        JSONObject json = new JSONObject();
        json.put("prompt", "This is a tweet sentiment classifier\n\n###\nTweet: \"" + msg + "\"\nSentiment:");
        json.put("temperature", 0.3);
        json.put("max_tokens", 10);
        json.put("top_p", 1.0);
        json.put("frequency_penalty", 0.5);
        json.put("presence_penalty", 0.0);
        json.put("stop", "[\"###\"]");

        String sentimentAnswer = "";
        try (CloseableHttpClient httpClient = HttpClientBuilder.create().build()) {
            HttpPost request = new HttpPost(openAiURL);
            StringEntity params = new StringEntity(json.toString());
            request.addHeader("content-type", "application/json");
            request.addHeader("Authorization", "Bearer " + apiKey);
            request.setEntity(params);
            HttpResponse httpResponse = httpClient.execute(request);
            HttpEntity responseEntity = httpResponse.getEntity();
            if (responseEntity != null) {
                String response = EntityUtils.toString(responseEntity);
                JSONObject responseJson = new JSONObject(response);
                JSONArray choicesArray = responseJson.getJSONArray("choices");
                String textAnswer = choicesArray.getJSONObject(0).getString("text");
                String[] splitString = textAnswer.split("\n");
                sentimentAnswer = splitString[0].replaceAll("\\s", "").toLowerCase();
            }
        } catch (Exception ex) {
            System.out.println("Error");
        }
        return sentimentAnswer;
    }

    public void run() {
        String topicName = "slack_messages";
        KafkaStreams kafkaStreams = createNewKafkaStream(topicName);
        kafkaStreams.start();
        Runtime.getRuntime().addShutdownHook(new Thread(kafkaStreams::close));
    }

    public static void main(String[] args) {
        new gpt3Utilities().run();
    }
}
