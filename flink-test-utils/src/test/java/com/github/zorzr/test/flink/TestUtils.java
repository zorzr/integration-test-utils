package com.github.zorzr.test.flink;

import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

public abstract class TestUtils {
    public static final ObjectMapper mapper = initMapper();

    private TestUtils() {
        // Utility class
    }

    public static ObjectMapper initMapper() {
        ObjectMapper objectMapper = new ObjectMapper();
        objectMapper.findAndRegisterModules();
        objectMapper.setSerializationInclusion(Include.NON_NULL);
        return objectMapper;
    }

    public static String loadResource(String path) {
        try {
            ClassLoader classLoader = TestUtils.class.getClassLoader();
            URL url = classLoader.getResource(path);
            if (url != null) {
                Path resource = Paths.get(url.toURI());
                return new String(Files.readAllBytes(resource), StandardCharsets.UTF_8);
            } else throw new IllegalArgumentException("Failed to retrieve the resource URI");
        } catch (Exception e) {
            throw new IllegalStateException("Exception caught while reading the given resource: " + path, e);
        }
    }

    public static FlinkTestScenario loadTest(String path) {
        String json = loadResource(path);
        return jsonToObject(json, FlinkTestScenario.class);
    }

    public static JsonNode jsonToTree(String json) {
        try {
            return mapper.readTree(json);
        } catch (Exception e) {
            e.printStackTrace();
            return mapper.createObjectNode();
        }
    }

    public static <T> T jsonToObject(String json, Class<T> outclass) {
        try {
            return mapper.readValue(json, outclass);
        } catch (Exception e) {
            e.printStackTrace();
            return null;
        }
    }
}
