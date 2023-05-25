package com.confluxsys.demo;

import java.util.Iterator;
import java.util.List;
import java.util.Map;

public interface ICompany {
    public List<Map<String, Object>> lookup(String sqlQuery, Map<String, Object> namedParameters, Integer batchSize);

    public List<Map<String, Object>> lookupInBatches(String sqlQuery, Map<String, Object> namedParameters, Integer batchSize);

}
