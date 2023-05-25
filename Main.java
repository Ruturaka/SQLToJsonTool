package com.confluxsys.demo;

import java.io.File;
import java.sql.*;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

public class Main {
    public static void main(String[] params) throws SQLException, ClassNotFoundException {
        params = new String[]{"exportConnections", "src/test/resources/tool.conf", "SQLToJSONTool/src/test/resources/auth0-connections-import-schema.json"};

        if (params == null || params.length != 3) {
            throw new IllegalArgumentException("Please specify all the required parameters.");
        }
        Config configg = ConfigFactory.parseFile(new File(params[1])); //TODO read the file name from parameters. load configurations.

        System.out.println("config: "+configg);
        String driver = configg.getString("driverClassName");
        String jdbcURL = configg.getString("databaseURL");
        String sqlQuery = configg.getString("exportUsers.sqlQuery");
        String queryParameters = configg.getString("exportUsers.queryParameters.param1");
        Integer sqlBatchSize = configg.getInt("sqlBatchSize");
        String username= configg.getString("username");
        String password= configg.getString("password");

        Map<String, Object> obj= new HashMap<>();

        CompanyDaoImp c= new CompanyDaoImp(driver, jdbcURL, username, password);

        List<Map<String, Object>> data= c.lookupInBatches(sqlQuery, obj,sqlBatchSize);
        System.out.println("data: "+data);

        System.out.println("Operation done successfully");
    }
}
