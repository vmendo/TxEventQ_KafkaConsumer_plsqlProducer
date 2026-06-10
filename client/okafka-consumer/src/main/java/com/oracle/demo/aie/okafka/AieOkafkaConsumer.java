package com.oracle.demo.aie.okafka;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.oracle.okafka.clients.consumer.KafkaConsumer;

import java.io.IOException;
import java.io.Reader;
import java.io.Writer;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.nio.charset.StandardCharsets;
import java.nio.file.DirectoryStream;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.nio.file.attribute.BasicFileAttributes;
import java.nio.file.attribute.PosixFilePermission;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Properties;
import java.util.Set;

public final class AieOkafkaConsumer {
    private static final String DEFAULT_CONFIG_FILE = "config/consumer.properties";
    private static final DateTimeFormatter TIMESTAMP_FORMAT =
            DateTimeFormatter.ISO_OFFSET_DATE_TIME.withZone(ZoneOffset.UTC);

    private static final Set<String> APP_PROPERTY_NAMES = new HashSet<>(Arrays.asList(
            "topic.name",
            "max.messages",
            "poll.timeout.ms",
            "idle.exit.after.seconds",
            "print.headers",
            "runtime.db.user"
    ));

    private AieOkafkaConsumer() {
    }

    public static void main(String[] args) throws Exception {
        Options options = Options.parse(args);
        if (options.help) {
            printUsage();
            return;
        }

        Properties fileProperties = loadProperties(options.configPath);
        String topic = firstNonBlank(options.topic, fileProperties.getProperty("topic.name"), "AIE_EVENTS");
        int maxMessages = parseInt(firstNonBlank(options.maxMessages, fileProperties.getProperty("max.messages"), "0"), "max.messages");
        int pollTimeoutMs = parseInt(firstNonBlank(options.pollTimeoutMs, fileProperties.getProperty("poll.timeout.ms"), "3000"), "poll.timeout.ms");
        int idleExitAfterSeconds = parseInt(firstNonBlank(options.idleExitAfterSeconds, fileProperties.getProperty("idle.exit.after.seconds"), "0"), "idle.exit.after.seconds");
        boolean printHeaders = Boolean.parseBoolean(firstNonBlank(options.printHeaders, fileProperties.getProperty("print.headers"), "true"));

        Properties consumerProperties = kafkaProperties(fileProperties);
        applyEnvironmentOverrides(consumerProperties);
        if (options.groupId != null && !options.groupId.isBlank()) {
            consumerProperties.setProperty("group.id", options.groupId);
        }

        Path runtimeTnsAdmin = null;
        if (Boolean.parseBoolean(firstNonBlank(env("OKAFKA_USE_EXISTING_TNS_ADMIN"), "false"))) {
            String configuredTnsAdmin = consumerProperties.getProperty("oracle.net.tns_admin");
            System.setProperty("oracle.net.tns_admin", configuredTnsAdmin);
        } else {
            String runtimeUser = firstNonBlank(
                    env("OKAFKA_USER"),
                    env("OKAFKA_USERNAME"),
                    fileProperties.getProperty("runtime.db.user"),
                    "AIE"
            );
            String runtimePassword = env("OKAFKA_PASSWORD");
            if (runtimePassword == null || runtimePassword.isBlank()) {
                throw new IllegalArgumentException("Set OKAFKA_PASSWORD before running the consumer.");
            }
            runtimeTnsAdmin = prepareRuntimeTnsAdmin(consumerProperties, runtimeUser, runtimePassword);
        }

        System.out.println("AIE OKafka consumer");
        System.out.println("  topic       : " + topic);
        System.out.println("  group.id    : " + consumerProperties.getProperty("group.id"));
        System.out.println("  tns.alias   : " + consumerProperties.getProperty("tns.alias"));
        System.out.println("  service     : " + consumerProperties.getProperty("oracle.service.name"));
        System.out.println("  config      : " + options.configPath.toAbsolutePath().normalize());
        System.out.println("  max.messages: " + (maxMessages == 0 ? "unlimited" : maxMessages));
        System.out.println("  idle.timeout: " + (idleExitAfterSeconds == 0 ? "disabled" : idleExitAfterSeconds + "s"));
        System.out.println();

        int consumed = 0;
        Instant lastRecordAt = Instant.now();
        Instant lastIdleNoticeAt = Instant.EPOCH;

        try (KafkaConsumer<String, String> okafkaConsumer = new KafkaConsumer<>(consumerProperties)) {
            Consumer<String, String> consumer = okafkaConsumer;
            printDatabaseIdentity(okafkaConsumer);

            try {
                List<PartitionInfo> partitions = consumer.partitionsFor(topic);
                System.out.println("Topic metadata partitions: " + partitions);
            } catch (RuntimeException e) {
                System.out.println("Topic metadata partitions: not available through partitionsFor() in this OKafka release.");
            }

            consumer.subscribe(Collections.singletonList(topic), new LoggingRebalanceListener());
            System.out.println("Subscribed. Waiting for records...");
            if (idleExitAfterSeconds == 0) {
                System.out.println("Idle timeout is disabled. Press Ctrl+C to stop the consumer.");
            }

            while (maxMessages == 0 || consumed < maxMessages) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(pollTimeoutMs));

                if (records.isEmpty()) {
                    Instant now = Instant.now();
                    long idleSeconds = Duration.between(lastRecordAt, now).getSeconds();
                    if (idleExitAfterSeconds > 0 || Duration.between(lastIdleNoticeAt, now).getSeconds() >= 30) {
                        String stopHint = idleExitAfterSeconds == 0 ? " Press Ctrl+C to stop." : "";
                        System.out.println("No records fetched. Idle for " + idleSeconds + "s." + stopHint);
                        lastIdleNoticeAt = now;
                    }
                    if (idleExitAfterSeconds > 0 && idleSeconds >= idleExitAfterSeconds) {
                        System.out.println("Idle timeout reached. Exiting.");
                        break;
                    }
                    continue;
                }

                lastRecordAt = Instant.now();
                lastIdleNoticeAt = Instant.EPOCH;
                for (ConsumerRecord<String, String> record : records) {
                    consumed++;
                    printRecord(consumed, record, printHeaders);
                    if (maxMessages > 0 && consumed >= maxMessages) {
                        break;
                    }
                }

                consumer.commitSync();
                System.out.println("Committed batch. Total consumed: " + consumed);
            }
        } finally {
            deleteQuietly(runtimeTnsAdmin);
        }

        System.out.println("Consumer finished. Total consumed: " + consumed);
    }

    private static void printDatabaseIdentity(KafkaConsumer<String, String> consumer) {
        try {
            Connection connection = consumer.getDBConnection();
            try (Statement statement = connection.createStatement();
                 ResultSet resultSet = statement.executeQuery(
                         "select user, sys_context('USERENV','SERVICE_NAME') service_name from dual")) {
                if (resultSet.next()) {
                    System.out.println("  db user     : " + resultSet.getString(1));
                    System.out.println("  db service  : " + resultSet.getString(2));
                }
            }
        } catch (Exception e) {
            System.out.println("  db identity : unavailable (" + e.getMessage() + ")");
        }
    }

    private static Properties loadProperties(Path configPath) throws IOException {
        if (!Files.isRegularFile(configPath)) {
            throw new IllegalArgumentException("Config file not found: " + configPath.toAbsolutePath().normalize());
        }

        Properties properties = new Properties();
        try (Reader reader = Files.newBufferedReader(configPath, StandardCharsets.UTF_8)) {
            properties.load(reader);
        }
        return properties;
    }

    private static Properties kafkaProperties(Properties source) {
        Properties properties = new Properties();
        for (String name : source.stringPropertyNames()) {
            if (!APP_PROPERTY_NAMES.contains(name)) {
                properties.setProperty(name, source.getProperty(name));
            }
        }
        return properties;
    }

    private static void applyEnvironmentOverrides(Properties properties) {
        override(properties, "bootstrap.servers", "OKAFKA_BOOTSTRAP_SERVERS");
        override(properties, "oracle.service.name", "OKAFKA_SERVICE_NAME");
        override(properties, "oracle.net.tns_admin", "OKAFKA_TNS_ADMIN");
        override(properties, "tns.alias", "OKAFKA_TNS_ALIAS");
        override(properties, "security.protocol", "OKAFKA_SECURITY_PROTOCOL");
        override(properties, "group.id", "OKAFKA_GROUP_ID");
    }

    private static void override(Properties properties, String propertyName, String envName) {
        String value = env(envName);
        if (value != null && !value.isBlank()) {
            properties.setProperty(propertyName, value);
        }
    }

    private static Path prepareRuntimeTnsAdmin(Properties consumerProperties, String user, String password) throws IOException {
        String configuredTnsAdmin = consumerProperties.getProperty("oracle.net.tns_admin");
        if (configuredTnsAdmin == null || configuredTnsAdmin.isBlank()) {
            throw new IllegalArgumentException("oracle.net.tns_admin is required.");
        }

        Path sourceTnsAdmin = Paths.get(configuredTnsAdmin).toAbsolutePath().normalize();
        if (!Files.isDirectory(sourceTnsAdmin)) {
            throw new IllegalArgumentException("oracle.net.tns_admin does not point to a directory: " + sourceTnsAdmin);
        }

        Path runtimeTnsAdmin = Files.createTempDirectory("aie-okafka-tns-");
        setOwnerOnlyPermissions(runtimeTnsAdmin, true);

        try (DirectoryStream<Path> stream = Files.newDirectoryStream(sourceTnsAdmin)) {
            for (Path sourceFile : stream) {
                if (Files.isRegularFile(sourceFile)) {
                    Path targetFile = runtimeTnsAdmin.resolve(sourceFile.getFileName().toString());
                    Files.copy(sourceFile, targetFile, StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.COPY_ATTRIBUTES);
                    setOwnerOnlyPermissions(targetFile, false);
                }
            }
        }

        String walletLocation = "(SOURCE=(METHOD=FILE)(METHOD_DATA=(DIRECTORY=" + runtimeTnsAdmin + ")))";
        Properties ojdbcProperties = new Properties();
        ojdbcProperties.setProperty("oracle.net.wallet_location", walletLocation);
        ojdbcProperties.setProperty("user", user);
        ojdbcProperties.setProperty("password", password);

        Path runtimeOjdbcProperties = runtimeTnsAdmin.resolve("ojdbc.properties");
        try (Writer writer = Files.newBufferedWriter(
                runtimeOjdbcProperties,
                StandardCharsets.UTF_8,
                StandardOpenOption.CREATE,
                StandardOpenOption.TRUNCATE_EXISTING,
                StandardOpenOption.WRITE)) {
            ojdbcProperties.store(writer, "Generated at runtime by AieOkafkaConsumer. Do not keep this file.");
        }
        setOwnerOnlyPermissions(runtimeOjdbcProperties, false);

        consumerProperties.setProperty("oracle.net.tns_admin", runtimeTnsAdmin.toString());
        System.setProperty("oracle.net.tns_admin", runtimeTnsAdmin.toString());
        System.setProperty("oracle.net.wallet_location", walletLocation);
        Runtime.getRuntime().addShutdownHook(new Thread(() -> deleteQuietly(runtimeTnsAdmin)));
        return runtimeTnsAdmin;
    }

    private static void setOwnerOnlyPermissions(Path path, boolean directory) {
        try {
            Set<PosixFilePermission> permissions = directory
                    ? new HashSet<>(Arrays.asList(
                    PosixFilePermission.OWNER_READ,
                    PosixFilePermission.OWNER_WRITE,
                    PosixFilePermission.OWNER_EXECUTE))
                    : new HashSet<>(Arrays.asList(
                    PosixFilePermission.OWNER_READ,
                    PosixFilePermission.OWNER_WRITE));
            Files.setPosixFilePermissions(path, permissions);
        } catch (UnsupportedOperationException | IOException ignored) {
            // Non-POSIX filesystems are acceptable for local demo execution.
        }
    }

    private static void printRecord(int count, ConsumerRecord<String, String> record, boolean printHeaders) {
        String timestamp = record.timestamp() > 0
                ? TIMESTAMP_FORMAT.format(Instant.ofEpochMilli(record.timestamp()))
                : "n/a";

        System.out.printf(Locale.ROOT,
                "%n[%04d] topic=%s partition=%d offset=%d timestamp=%s%n",
                count,
                record.topic(),
                record.partition(),
                record.offset(),
                timestamp);
        System.out.println("key  : " + nullSafe(record.key()));
        System.out.println("value: " + nullSafe(record.value()));

        if (printHeaders) {
            for (Header header : record.headers()) {
                String value = header.value() == null ? "" : new String(header.value(), StandardCharsets.UTF_8);
                System.out.println("header: " + header.key() + "=" + value);
            }
        }
    }

    private static void deleteQuietly(Path root) {
        if (root == null || !Files.exists(root)) {
            return;
        }
        try {
            Files.walkFileTree(root, new SimpleFileVisitor<Path>() {
                @Override
                public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
                    Files.deleteIfExists(file);
                    return FileVisitResult.CONTINUE;
                }

                @Override
                public FileVisitResult postVisitDirectory(Path dir, IOException exc) throws IOException {
                    Files.deleteIfExists(dir);
                    return FileVisitResult.CONTINUE;
                }
            });
        } catch (IOException ignored) {
            // Best effort cleanup. The runtime directory only contains a temporary wallet copy.
        }
    }

    private static int parseInt(String value, String label) {
        try {
            return Integer.parseInt(value);
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException(label + " must be an integer. Value: " + value, e);
        }
    }

    private static String firstNonBlank(String... values) {
        for (String value : values) {
            if (value != null && !value.isBlank()) {
                return value;
            }
        }
        return null;
    }

    private static String env(String name) {
        return System.getenv(name);
    }

    private static String nullSafe(String value) {
        return value == null ? "" : value;
    }

    private static void printUsage() {
        System.out.println("Usage: AieOkafkaConsumer [options]");
        System.out.println();
        System.out.println("Options:");
        System.out.println("  --config=<file>                 Consumer properties file. Default: " + DEFAULT_CONFIG_FILE);
        System.out.println("  --topic=<name>                  Topic to subscribe to. Default from config.");
        System.out.println("  --group-id=<name>               Consumer group id override.");
        System.out.println("  --max-messages=<n>              Stop after n records. Use 0 for unlimited.");
        System.out.println("  --poll-timeout-ms=<n>           Poll timeout in milliseconds.");
        System.out.println("  --idle-timeout-seconds=<n>      Stop after n idle seconds. Use 0 to disable.");
        System.out.println("  --print-headers=<true|false>    Print Kafka headers.");
        System.out.println("  --help                          Show this help.");
        System.out.println();
        System.out.println("Environment:");
        System.out.println("  OKAFKA_PASSWORD                 Required database password.");
        System.out.println("  OKAFKA_USER                     Optional database user override. Default: AIE.");
        System.out.println("  OKAFKA_TNS_ADMIN                Optional wallet directory override.");
        System.out.println("  OKAFKA_TNS_ALIAS                Optional TNS alias override.");
        System.out.println("  OKAFKA_GROUP_ID                 Optional consumer group override.");
    }

    private static final class LoggingRebalanceListener implements ConsumerRebalanceListener {
        @Override
        public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
            System.out.println("Partitions revoked: " + partitions);
        }

        @Override
        public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
            System.out.println("Partitions assigned: " + partitions);
            if (isPlaceholderAssignment(partitions)) {
                System.out.println(
                        "Only the placeholder partition was assigned. OKafka rebalancing can take a few " +
                                "minutes; keep the consumer running. If real partitions never appear, run " +
                                "scripts/04_diagnose_okafka_topic.sql before resetting the topic."
                );
            }
        }
    }

    private static boolean isPlaceholderAssignment(Collection<TopicPartition> partitions) {
        if (partitions == null || partitions.isEmpty()) {
            return false;
        }

        for (TopicPartition partition : partitions) {
            if (partition.partition() >= 0) {
                return false;
            }
        }
        return true;
    }

    private static final class Options {
        private Path configPath = Paths.get(DEFAULT_CONFIG_FILE);
        private String topic;
        private String groupId;
        private String maxMessages;
        private String pollTimeoutMs;
        private String idleExitAfterSeconds;
        private String printHeaders;
        private boolean help;

        private static Options parse(String[] args) {
            Options options = new Options();
            for (String arg : args) {
                if ("--help".equals(arg) || "-h".equals(arg)) {
                    options.help = true;
                } else if (arg.startsWith("--config=")) {
                    options.configPath = Paths.get(value(arg));
                } else if (arg.startsWith("--topic=")) {
                    options.topic = value(arg);
                } else if (arg.startsWith("--group-id=")) {
                    options.groupId = value(arg);
                } else if (arg.startsWith("--max-messages=")) {
                    options.maxMessages = value(arg);
                } else if (arg.startsWith("--poll-timeout-ms=")) {
                    options.pollTimeoutMs = value(arg);
                } else if (arg.startsWith("--idle-timeout-seconds=")) {
                    options.idleExitAfterSeconds = value(arg);
                } else if (arg.startsWith("--print-headers=")) {
                    options.printHeaders = value(arg);
                } else {
                    throw new IllegalArgumentException("Unknown argument: " + arg);
                }
            }
            return options;
        }

        private static String value(String arg) {
            int pos = arg.indexOf('=');
            if (pos < 0 || pos == arg.length() - 1) {
                throw new IllegalArgumentException("Expected --name=value format. Argument: " + arg);
            }
            return arg.substring(pos + 1);
        }
    }
}
