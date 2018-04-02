package com.walirian.karkoub;

import java.io.IOException;
import java.sql.*;

import java.util.logging.Logger;

public class HiveInteractions {


    //private  static String driverName = "com.cloudera.hive.jdbc3.HS2Driver";
    private  static String driverName = "org.apache.hive.jdbc.HiveDriver";
    private static final Logger logger = Logger.getLogger("com.walirian.karkoub.hiveClient");


    public static void main(String[] args) throws SQLException, ClassNotFoundException,IOException {


        //set jdbc hive driver
        Class.forName(driverName);
        //connect to hive
        Connection con = DriverManager.getConnection("jdbc:hive2://cloudera.training.walirian.com:10000/elwali_yelp;ssl=false","username","password");
        Statement stmt = con.createStatement();

     // show tables
        String sqlShowTables = "show tables " ;
        ResultSet res = stmt.executeQuery(sqlShowTables);
        while(res.next()){
            System.out.println(res.getString(1)+ "\t"   );
        }
        logger.info("showed tables");

        // Create new table
        String sqlCreateTable = "create table ".concat("helloHive (message String) STORED AS PARQUET");
        stmt.executeQuery(sqlCreateTable);
        logger.info("new table created");


        // show tables contents
        String sqlSelect = "SELECT * from tip limit 5";

        logger.info("showing table content");
        ResultSet res2 = stmt.executeQuery(sqlSelect);
        while (res2.next()){
            System.out.println(res2.getString(2));
        }





        res2.close();
        //res.close();
        con.close();




    }

}
