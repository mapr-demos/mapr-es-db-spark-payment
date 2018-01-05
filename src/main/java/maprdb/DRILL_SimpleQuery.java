package maprdb;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;

/**
 * Sample JDBC based application to obtain details of all businesses that have
 * at least 100 reviews and a rating greater than 3.
 */
public class DRILL_SimpleQuery {

    public static String JDBC_DRIVER = "org.apache.drill.jdbc.Driver";

    /**
     * Can specify connection URL in 2 ways. 1. Connect to Zookeeper -
     * "jdbc:drill:zk=<hostname/host-ip>:5181/drill/<cluster-name>-drillbits" 2.
     * Connect to Drillbit - "jdbc:drill:drillbit=<hostname>"
     */
    private static String DRILL_JDBC_URL = "jdbc:drill:zk=maprdemo:5181/drill/maprdemo.mapr.io-drillbits";

    public static void main(String[] args) {
        String tableName = "/mapr/maprdemo.mapr.io/apps/payments";
        if (args.length == 1) {
            tableName = args[0];

        } else {
            System.out.println("Using hard coded parameters unless you specify the file and topic. <file topic>   ");
        }

        try {
            Class.forName(JDBC_DRIVER);
            //Username and password have to be provided to obtain connection.
            //Ensure that the user provided is present in the cluster / sandbox
            Connection connection = DriverManager.getConnection(DRILL_JDBC_URL, "mapr", "");

            Statement statement = connection.createStatement();
            System.out.println("Top 10 physician specialties by total payments");
            final String sql = "select physician_specialty,sum(amount) as total from dfs.`" + tableName + "` group by physician_specialty order by total desc limit 10";
            System.out.println("Query: " + sql);

            ResultSet result = statement.executeQuery(sql);

            while (result.next()) {
                System.out.println("{\"physician_specialty\": \"" + result.getString(1) + "\", "
                        + "\"total\": " + result.getString(2) + "}");
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
