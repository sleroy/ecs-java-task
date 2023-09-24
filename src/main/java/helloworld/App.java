package helloworld;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.InetAddress;
import java.net.URL;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.stream.Collectors;

import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.PutItemRequest;

import com.amazonaws.services.lambda.runtime.events.APIGatewayProxyResponseEvent;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import software.amazon.awssdk.http.apache.ApacheHttpClient;
import software.amazon.awssdk.services.secretsmanager.SecretsManagerClient;
import software.amazon.awssdk.services.secretsmanager.model.GetSecretValueRequest;
import software.amazon.awssdk.services.secretsmanager.model.GetSecretValueResponse;
import software.amazon.awssdk.services.secretsmanager.model.SecretsManagerException;
import software.amazon.awssdk.services.ssm.SsmClient;
import software.amazon.awssdk.services.ssm.model.GetParameterRequest;
import software.amazon.awssdk.services.ssm.model.GetParameterResponse;
import software.amazon.awssdk.services.ssm.model.SsmException;
import software.amazon.jdbc.PropertyDefinition;
import software.amazon.awssdk.services.rds.RdsClient;
import software.amazon.awssdk.services.rds.RdsUtilities;
import software.amazon.awssdk.services.rds.model.GenerateAuthenticationTokenRequest;
import software.amazon.awssdk.services.rds.model.RdsException;

class Film {
    public Integer film_id;
    public String title;
    public String description;
    public String rating;
    public Integer release_year;
    public Integer length;

    public Integer language_id;
    public Integer rental_duration;
    public Integer rental_rate;
    public Integer replacement_cost;
    public LocalDateTime last_update;
    public String special_features;
    public String fulltext;
}

/**
 * Handler for requests to Lambda function.
 */
public class App {

    // Configuration parameters for the generation of the IAM Database
    private static final int RDS_INSTANCE_PORT = 5432;
    private static final String SECRET_NAME = "prod/aurora-postgres-cluster-1";
    private static final String REGION_NAME = "us-east-1";

    public static final String SSM_CLUSTER_ENDPOINT = "/rds/aurorapostgres/cluster_endpoint";

    public static final String FILM_SELECT_ALL = "SELECT * FROM FILM";
    private static final String DYNAMO_FILM_TABLE = "film_table";

    private final DynamoDbAsyncClient ddb;
    private final SsmClient ssmClient;
    private SecretsManagerClient secretClient;

    public App() {
        ddb = DynamoDbAsyncClient.builder()
                .build();
        ssmClient = SsmClient.builder()
                .httpClientBuilder(ApacheHttpClient.builder())
                .build();
        secretClient = SecretsManagerClient.builder()
                .httpClientBuilder(ApacheHttpClient.builder())
                .build();
    }

    public static void main(String[] args) {
        new App().run();
    }

    public void run() {

        ObjectMapper objectMapper = new ObjectMapper();
        objectMapper.findAndRegisterModules();

        /*
         * https://www.postgresqltutorial.com/postgresql-getting-started/postgresql-
         * sample-database/
         */
        // Obtain cluster Writer endpoint : /rds/aurorapostgres/cluster_endpoint

        // Initi clients

        Map<String, Object> jsonResponse = new HashMap<>();

        Map<String, String> headers = new HashMap<>();
        headers.put("Content-Type", "application/json");
        headers.put("X-Custom-Header", "application/json");

        APIGatewayProxyResponseEvent response = new APIGatewayProxyResponseEvent()
                .withHeaders(headers);
        try {
            String auroraEndpoint = getParaValue(ssmClient, SSM_CLUSTER_ENDPOINT);
            jsonResponse.put("endpoint", auroraEndpoint);

            InetAddress clusterAddress = java.net.InetAddress.getByName(auroraEndpoint);
            System.out.println("DNS is resolved " + clusterAddress.getHostAddress());
            boolean reachable = clusterAddress.isReachable(3000);
            System.out.println("Is host reachable? " + reachable);

            ObjectNode credentials = getSecretValue(objectMapper, secretClient, SECRET_NAME);
            jsonResponse.put("credentials", credentials.get("host"));
            jsonResponse.put("username", credentials.get("username").textValue());

            List<Film> films = new ArrayList<>();
            try (Connection connection = getDBConnectionUsingPassword(credentials, auroraEndpoint);) {
                films.addAll(getFilms(connection));
                // Add location
                final String pageContents = this.getPageContents("https://checkip.amazonaws.com");
                jsonResponse.put("location", pageContents);
                jsonResponse.put("films", films);

            }
            storeFilmsIntoDynamoDB(films);
            System.out.println(objectMapper.writer().writeValueAsString(jsonResponse));
       

        } catch (Exception e) {
            e.printStackTrace();
                
        }
    }

    public static List<Film> getFilms(Connection connection) throws SQLException {
        var films = new ArrayList<Film>();
        try (PreparedStatement pS = connection.prepareStatement(FILM_SELECT_ALL);) {
            try (ResultSet rs = pS.executeQuery();) {
                while (rs.next()) {
                    var f = new Film();
                    f.description = rs.getString("description");
                    f.title = rs.getString("title");
                    f.film_id = rs.getInt("film_id");
                    f.rating = rs.getString("rating");
                    f.release_year = rs.getInt("release_year");
                    f.length = rs.getInt("length");
                    f.language_id = rs.getInt("language_id");
                    f.rental_duration = rs.getInt("rental_duration");
                    f.rental_rate = rs.getInt("rental_rate");
                    f.replacement_cost = rs.getInt("replacement_cost");
                    f.last_update = rs.getTimestamp("last_update").toLocalDateTime();
                    f.special_features = rs.getString("special_features");
                    f.fulltext = rs.getString("fulltext");
                    films.add(f);
                }
            }
        }
        return films;
    }

    public void storeFilmsIntoDynamoDB(List<Film> films) throws IOException {

        try {
            for (Film film : films) {
                HashMap<String, AttributeValue> item_values = new HashMap<String, AttributeValue>();


                item_values.put("description", AttributeValue.builder().s(film.description).build());
                item_values.put("title", AttributeValue.builder().s(film.title).build());
                item_values.put("film_id", AttributeValue.builder().s(film.film_id.toString()).build());
                item_values.put("rating", AttributeValue.builder().s(film.rating).build());
                item_values.put("release_year", AttributeValue.builder().n(film.release_year.toString()).build());
                item_values.put("length", AttributeValue.builder().n(film.length.toString()).build());
                item_values.put("language_id", AttributeValue.builder().n(film.language_id.toString()).build());
                item_values.put("rental_duration", AttributeValue.builder().n(film.rental_duration.toString()).build());
                item_values.put("rental_rate", AttributeValue.builder().n(film.rental_rate.toString()).build());
                item_values.put("replacement_cost", AttributeValue.builder().n(film.replacement_cost.toString()).build());
                item_values.put("last_update_time", AttributeValue.builder().s(film.last_update.toString()).build());
                item_values.put("special_features", AttributeValue.builder().s(film.special_features).build());
                item_values.put("fulltext", AttributeValue.builder().s(film.fulltext).build());
                PutItemRequest request = PutItemRequest.builder()
                        .tableName(DYNAMO_FILM_TABLE)
                        .item(item_values)
                        .build();

                ddb.putItem(request);
            }
        } catch (Exception e) {
            e.printStackTrace();
            throw new IOException("Error while storing elements into dynamodb", e);
        }
    }

    private String getPageContents(String address) throws IOException {
        URL url = new URL(address);
        try (BufferedReader br = new BufferedReader(new InputStreamReader(url.openStream()))) {
            return br.lines().collect(Collectors.joining(System.lineSeparator()));
        }
    }

    /**
     * This method returns a connection to the db instance authenticated using
     * password
     * Database Authentication
     * https://github.com/awslabs/aws-postgresql-jdbc
     * 
     * @return
     * @throws Exception
     */
    private static Connection getDBConnectionUsingPassword(ObjectNode credentials, String clusterEndpoint)
            throws Exception {
        // Configuring connection properties for the underlying JDBC driver.
        String userName = credentials.get("username").textValue();
        String password = credentials.get("password").textValue();

        final Properties properties = new Properties();
        properties.setProperty(PropertyDefinition.USER.name, userName);

        properties.setProperty("password", password);
        properties.setProperty("iamRegion", REGION_NAME);
        properties.setProperty("loginTimeout", "1000");
        // Configuring connection properties for the JDBC Wrapper.
        properties.setProperty(PropertyDefinition.PLUGINS.name, "auroraConnectionTracker,failover,efm");
        properties.setProperty(PropertyDefinition.LOG_UNCLOSED_CONNECTIONS.name, "true");
        String jdbcURL = "jdbc:aws-wrapper:postgresql://" + clusterEndpoint + ":" + RDS_INSTANCE_PORT
                + "/dvdrental?ssl=true&sslmode=require";
        System.out.println("JDBC Endpoint is " + jdbcURL);
        return DriverManager.getConnection(jdbcURL, properties);
    }

    /**
     * This method returns a connection to the db instance authenticated using IAM
     * Database Authentication
     * https://github.com/awslabs/aws-postgresql-jdbc
     * 
     * @return
     * @throws Exception
     */
    private static Connection getDBConnectionUsingIam(ObjectNode credentials, String clusterEndpoint)
            throws Exception {
        // Configuring connection properties for the underlying JDBC driver.
        String userName = credentials.get("username").textValue();
        String password = generateAuthToken(userName, clusterEndpoint);

        final Properties properties = new Properties();
        properties.setProperty(PropertyDefinition.USER.name, userName);
        // properties.setProperty("password", credentials.get("password").textValue());
        properties.setProperty("password", password);
        properties.setProperty("iamRegion", REGION_NAME);
        properties.setProperty("loginTimeout", "1000");
        // Configuring connection properties for the JDBC Wrapper.
        properties.setProperty(PropertyDefinition.PLUGINS.name, "auroraConnectionTracker,failover,efm,iam");
        properties.setProperty(PropertyDefinition.LOG_UNCLOSED_CONNECTIONS.name, "true");
        String jdbcURL = "jdbc:aws-wrapper:postgresql://" + clusterEndpoint + ":" + RDS_INSTANCE_PORT
                + "/dvdrental?ssl=true&sslmode=require";
        System.out.println("JDBC Endpoint is " + jdbcURL);
        return DriverManager.getConnection(jdbcURL, properties);
    }

    /**
     * This method generates the IAM Auth Token.
     * An example IAM Auth Token would look like follows:
     * btusi123.cmz7kenwo2ye.rds.cn-north-1.amazonaws.com.cn:3306/?Action=connect&DBUser=iamtestuser&X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Date=20171003T010726Z&X-Amz-SignedHeaders=host&X-Amz-Expires=899&X-Amz-Credential=AKIAPFXHGVDI5RNFO4AQ%2F20171003%2Fcn-north-1%2Frds-db%2Faws4_request&X-Amz-Signature=f9f45ef96c1f770cdad11a53e33ffa4c3730bc03fdee820cfdf1322eed15483b
     * 
     * @return
     */
    private static String generateAuthToken(String userName, String clusterEndpoint) throws IOException {
        RdsClient rdsClient = RdsClient.builder()
                .region(Region.US_EAST_1)
                .build();
        RdsUtilities utilities = rdsClient.utilities();
        try {
            GenerateAuthenticationTokenRequest tokenRequest = GenerateAuthenticationTokenRequest.builder()
                    .username(userName)
                    .port(5432)
                    .hostname(clusterEndpoint)
                    .build();

            return utilities.generateAuthenticationToken(tokenRequest);

        } catch (RdsException e) {
            throw new IOException("Cannot generate an RDS token", e);
        }
    }

    public static String getParaValue(SsmClient ssmClient, String paraName) throws IOException {

        try {
            GetParameterRequest parameterRequest = GetParameterRequest.builder()
                    .name(paraName)
                    .build();

            GetParameterResponse parameterResponse = ssmClient.getParameter(parameterRequest);
            System.out.println("The parameter value is " + parameterResponse.parameter().value());
            return parameterResponse.parameter().value();

        } catch (SsmException e) {
            throw new IOException("Cannot obtain the parameter " + paraName);
        }
    }

    public ObjectNode getSecretValue(ObjectMapper objectMapper, SecretsManagerClient secretsClient,
            String secretName)
            throws IOException {

        try {
            GetSecretValueRequest valueRequest = GetSecretValueRequest.builder()
                    .secretId(secretName)
                    .build();

            GetSecretValueResponse valueResponse = secretsClient.getSecretValue(valueRequest);
            String secret = valueResponse.secretString();
            System.out.println(secret);
            return (ObjectNode) objectMapper.readTree(secret);

        } catch (SecretsManagerException e) {
            throw new IOException(e.awsErrorDetails().errorMessage());
        }
    }
}
