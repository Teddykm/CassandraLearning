package com.cassandra;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;

public class CassandraApplication {

    private static  Cluster cluster;
    private static Session session;

    public static void main(String[] args ) {
    // Connect to the cluster and keyspace "demo"
        cluster = Cluster.builder()
                .addContactPoint("127.0.0.1")
                .build();
        session = cluster.connect("demo");

    //Insert one record into the users table
        session.execute("INSERT INTO users(lastname, age, city, email, firstname) VALUES ('Jones', 35, 'Austin', 'bob@example.com', 'Bob')");

    //Use select to get the user we just entered
        ResultSet result = session.execute("SELECT * FROM users WHERE lastname='Jones'");
        for (Row row : result) {
            System.out.format("%s %d\n", row.getString("firstname"), row.getInt("age"));

        }
    //Update the same user with a new age
        session.execute("UPDATE users SET age=36 WHERE lastname='Jones'");
    //Select updated value of user
        result=session.execute("SELECT * FROM users");
        for (Row row : result) {
            System.out.format("%d\n", row.getInt("age"));
        }
    //Delete from users table
        session.execute("DELETE FROM users WHERE lastname='Jones'");
    //Show that the entry has been deleted
        result=session.execute("SELECT * FROM users");

        for(Row row : result) {
            System.out.format("%s %s %s %s %d\n", row.getString("firstname"), row.getString("lastname"), row.getString("email"), row.getString("city"), row.getInt("age"));
        }
    //clean up the connection by closing it
    cluster.close();

    }

}
