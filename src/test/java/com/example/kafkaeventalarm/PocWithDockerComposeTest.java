package com.example.kafkaeventalarm;


import java.io.File;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;
import org.testcontainers.containers.DockerComposeContainer;
import org.testcontainers.containers.wait.strategy.Wait;

import lombok.extern.slf4j.Slf4j;

@SpringBootTest
@Slf4j
public class PocWithDockerComposeTest {

    @SuppressWarnings("rawtypes")
    private static final DockerComposeContainer CONTAINER = new DockerComposeContainer(new File("src/test/resources/docker-compose-with-ui.yml"));

    @BeforeAll
    public static void setUpClass() {
        // wait after ksqldb to be ready
        CONTAINER.waitingFor("ksqldb-server", Wait.forLogMessage(".*INFO Server up and running.*\\s", 1));
        //CONTAINER.withTailChildContainers(true);
        CONTAINER.start();
    }

    @AfterAll
    public static void cleanup() {
        CONTAINER.close();
    }

    @AfterEach
    public void DROP() {

    }

    @Test
    public void poc() throws Exception {
        log.debug("poc");

    }


}

