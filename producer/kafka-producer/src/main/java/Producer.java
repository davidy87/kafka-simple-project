import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.sql.*;
import java.util.*;

public class Producer {
    // Global variables

    // For Kafka
    private static final String TOPIC_NAME = "test";
    private static final String BOOTSTRAP_SERVER = "34.219.58.191:9092";

    // For JDBC
    private static final String DB_URL = "jdbc:postgresql://localhost:5432/my_spotify_playlist";
    private static final String DB_USER = "david";
    private static final String DB_PWD = "testpass123!";


    public static void main(String[] args) throws SQLException, ClassNotFoundException {
        Properties configs = new Properties();
        configs.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVER);
        configs.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        configs.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        KafkaProducer<String, String> producer = new KafkaProducer<>(configs);
        queryData(connectDB(), producer);
    }


    /*
        Connect to the Postgres DB
     */
    private static Connection connectDB() throws SQLException, ClassNotFoundException {
        System.out.println("loading driver");
        Class.forName("org.postgresql.Driver");
        System.out.println("driver loaded");

        System.out.println("Connecting to DB");
        // Local machine
        Connection conn = DriverManager.getConnection(DB_URL, DB_USER, DB_PWD);
        System.out.println("Connected to DB");

        return conn;
    }


    /*
        Query all data from Postgres DB and send them to Kafka topic.
        This is just for practicing Kafka producer. Need to modify data streaming procedure.
     */
    private static void queryData(Connection conn, KafkaProducer<String, String> producer) throws SQLException {
        System.out.println("Querying table");
        String selectString = "SELECT * FROM songs";
        Statement stmt = conn.createStatement();

        ResultSet rs = stmt.executeQuery(selectString);
        ResultSetMetaData rsmd = rs.getMetaData();
        int colNum = rsmd.getColumnCount();

        while (rs.next()) {
            String row = "";

            for (int i = 1; i <= colNum; i++) {
                if (i > 1)
                    row += ", ";

                row += rs.getString(i);
            }

            ProducerRecord<String, String> record = new ProducerRecord<>(TOPIC_NAME, row);

            try {
                producer.send(record);
                System.out.println("Send to " + TOPIC_NAME + " | data : " + row);
                Thread.sleep(1000);
            } catch (Exception e) {
                System.out.println(e);
            }
        }

        stmt.close();
    }
}
