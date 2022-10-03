package com.github.zorzr.test.mongo;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import de.flapdoodle.embed.mongo.MongodExecutable;
import de.flapdoodle.embed.mongo.MongodStarter;
import de.flapdoodle.embed.mongo.config.Defaults;
import de.flapdoodle.embed.mongo.config.ImmutableMongodConfig;
import de.flapdoodle.embed.mongo.config.MongodConfig;
import de.flapdoodle.embed.mongo.config.Net;
import de.flapdoodle.embed.mongo.distribution.Version;
import de.flapdoodle.embed.mongo.packageresolver.Command;
import de.flapdoodle.embed.process.config.RuntimeConfig;
import de.flapdoodle.embed.process.config.process.ProcessOutput;
import de.flapdoodle.embed.process.runtime.Network;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.InetAddress;

public class MongoLocalInstance {
    private static final Logger logger = LoggerFactory.getLogger(MongoLocalInstance.class);
    private final ImmutableMongodConfig executableConfig;
    private final MongodExecutable mongodExecutable;

    public MongoLocalInstance(ImmutableMongodConfig executableConfig, RuntimeConfig runtimeConfig) {
        MongodStarter starter = MongodStarter.getInstance(runtimeConfig);
        this.mongodExecutable = starter.prepare(executableConfig);
        this.executableConfig = executableConfig;
    }

    public static MongoLocalInstance defaultInstance() {
        ImmutableMongodConfig executableConfig = defaultExecutableConfig();
        RuntimeConfig runtimeConfig = defaultRuntimeConfig();
        return new MongoLocalInstance(executableConfig, runtimeConfig);
    }

    public static ImmutableMongodConfig defaultExecutableConfig() {
        try {
            InetAddress localhost = Network.getLocalHost();
            int port = Network.freeServerPort(localhost);

            return MongodConfig.builder()
                    .version(Version.Main.PRODUCTION)
                    .net(new Net(localhost.getHostName(), port, Network.localhostIsIPv6()))
                    .build();
        } catch (Exception e) {
            logger.error("Exception caught while looking for a localhost free port", e);
            throw new IllegalStateException(e);
        }
    }

    public static RuntimeConfig defaultRuntimeConfig() {
        return Defaults.runtimeConfigFor(Command.MongoD, logger)
                .processOutput(ProcessOutput.silent())
                .build();
    }

    public void start() {
        try {
            mongodExecutable.start();
        } catch (IOException e) {
            logger.error("Exception caught while starting MongoDB instance", e);
            throw new UncheckedIOException(e);
        }
    }

    public void close() {
        mongodExecutable.stop();
    }

    public String getConnectionString() {
        Net net = executableConfig.net();
        return String.format("mongodb://%s:%d", net.getBindIp(), net.getPort());
    }

    public MongoClient getMongoClient() {
        String connectionString = getConnectionString();
        return MongoClients.create(connectionString);
    }
}
