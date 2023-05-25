package com.confluxsys.demo;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;
import java.util.*;


public class CompanyDaoImp implements ICompany{

    private static final Logger logger = LoggerFactory.getLogger(CompanyDaoImp.class);
    private final String dbDriverClassName;
    private final String url;
    private final String username;
    private final String password;

    CompanyDaoImp(String driver, String jdbcUrl, String username, String password) throws ClassNotFoundException, SQLException {
        this.dbDriverClassName = driver;
        this.url = jdbcUrl;
        this.username = username;
        this.password = password;
    }

    public Connection getConnection() {
        Connection connection=null;
        try {
            Class.forName (dbDriverClassName);
            connection = DriverManager.getConnection(url, username, password);
        }
        catch (SQLException e){
            logger.info("Error occured while establishing connection: "+e.getMessage());
        }
        catch (ClassNotFoundException e) {
            logger.info(e.getMessage());
        }
        logger.info("Connected to Postgres server");
        return connection;
    }

    @Override
    public List<Map<String, Object>> lookup(String sqlQuery, Map<String, Object> namedParameters, Integer batchSize) {
        List<Map<String, Object>> data=null;
        Connection connection=getConnection();
        if (batchSize > 0);
           // data = getData(connection, sqlQuery, batchSize);
        else
            data = getData(connection);
        //logger.info("Data: "+data);
        return data;

        //batch size = 100  db rows: 357
        // 100 100 100 57
    }

    @Override
    public List<Map<String, Object>> lookupInBatches(String sqlQuery, Map<String, Object> namedParameters, Integer batchSize) {
        List<Map<String, Object>> data= new ArrayList<>();
        //Iterator<Map<String, Object>> data = null;

        Connection connection=getConnection();
        if (batchSize > 0) {
            data = getData(connection, sqlQuery, batchSize);
            System.out.println("data from resultList: "+data);
        }
        return data;
    }

    public List<Map<String, Object>> getData(Connection connection, String sqlQuery, Integer batchSize){


        int offset = 0, counter = 0;
        List<Map<String, Object>> data;
        do {
            String queryy = sqlQuery + " OFFSET " + offset + " ROWS FETCH NEXT " + batchSize + " ROWS ONLY ";
            data = extractData(connection, queryy);
            System.out.println("data in do: "+data);
            offset += batchSize;
            counter++;
        } while (data.size() == batchSize);
        logger.info("the loop executed:" + counter +" times");
        closeConnection(connection);
        return data;
    }
    public List<Map<String, Object>> getData(Connection conn) {
        List<Map<String,Object>> data= new ArrayList<>();
        PreparedStatement p=null;
        ResultSet bres=null;
        String sqlQuery="SELECT * FROM users";
        try {
            p = conn.prepareStatement(sqlQuery);
            bres = p.executeQuery();
            while (bres.next()) {
                Map<String, Object> row = new HashMap<>(bres.getMetaData().getColumnCount());
                for (int i = 1; i <= bres.getMetaData().getColumnCount(); i++)
                    row.put(bres.getMetaData().getColumnName(i), bres.getObject(i));
                data.add(row);
            }

        }
        catch (SQLException e){
            logger.info("Error occurred while fetching data: "+e.getMessage());
        }
        finally {
            closeResultSet(bres);
            closestmnt(p);
            closeConnection(conn);
        }
        return data;
    }

    List<Map<String, Object>> extractData(Connection connection, String query){
        List<Map<String, Object>> data = new ArrayList<>();
        Iterator<Map<String, Object>> resultList;
        PreparedStatement p = null;
        ResultSet bres = null;
        try {
            p = connection.prepareStatement(query);
            bres = p.executeQuery();
            data = new ArrayList<>();
            while (bres.next()) {
                Map<String, Object> row = new HashMap<>(bres.getMetaData().getColumnCount());
                for (int i = 1; i <= bres.getMetaData().getColumnCount(); i++) {
                    row.put(bres.getMetaData().getColumnName(i), bres.getObject(i));
                }
                data.add(row);
                resultList = data.iterator();
                while (resultList.hasNext()) {
                    System.out.println(resultList.next());
                    data.add(resultList.next());
                }
            }
        }
        catch (SQLException e){
            logger.info("Message: " + e.getMessage());
            logger.info("Error occurred while fetching data.");
        }
        finally {
            closeResultSet(bres);
            closestmnt(p);
            //closeConnection(connection);
        }
        return data;
    }
    public void closeResultSet(ResultSet rs){
        try {
            if (rs != null)
                rs.close();
        }
        catch (SQLException e){
            logger.info("Error in closing ResultSet: "+e.getMessage());
        }
    }
    public void closestmnt(PreparedStatement p){
        try{
            if(p!=null)
                p.close();
        }
        catch (SQLException e){
            logger.info("Error in closing preparedStatement: "+e.getMessage());
        }
    }
    public void closeConnection(Connection connection) {
        try {
            if (connection != null) {
                connection.close();
                logger.info("connection closed");
            }
        } catch (SQLException e) {
            logger.info("Error occured while closing connection" + e.getMessage());
        }
    }
}

