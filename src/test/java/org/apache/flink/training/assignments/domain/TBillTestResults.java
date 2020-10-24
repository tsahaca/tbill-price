package org.apache.flink.training.assignments.domain;

import org.apache.flink.api.java.tuple.Tuple3;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.time.LocalDate;
import java.util.HashMap;
import java.util.Map;

public class TBillTestResults {

    private static final Logger LOG = LoggerFactory.getLogger(TBillTestResults.class);

    public Map<String, TBillTestRecord> resultMap = new HashMap<>();

    public  static TBillTestResults ofResource(String resource) {
        try (BufferedReader reader = new BufferedReader(new InputStreamReader(TBillTestResults.class.getResourceAsStream(resource)))) {
            TBillTestResults results = new TBillTestResults();
            String line = null;
            while ((line = reader.readLine()) != null) {
                results.parseString(line);
            }
            return results;
        } catch (Exception e) {
            LOG.error("Exception: {}", e);
        }
        return null;
    }

    private void parseString(String line) {

        String[] tokens = line.split(",");
        if (tokens.length != 5) {
            throw new RuntimeException("Invalid record: " + line);
        }

        String key = TBillRate.getKey(LocalDate.parse(tokens[0]).atStartOfDay());

        resultMap.computeIfAbsent(key, k -> new TBillTestRecord());

       //tokens[1].length() > 0 && Boolean.parseBoolean(tokens[1]);

        resultMap.computeIfPresent(key, (k, r)-> {
            try {
                r.returns.add(Tuple3.of(LocalDate.parse(tokens[0]).atStartOfDay(),
                        tokens[2].length() > 0 ? Double.parseDouble(tokens[2]) : 0.0,
                        (tokens[1].length() > 0 && Boolean.parseBoolean(tokens[1]))
                        ));
                r.average = tokens[3].length() > 0 ? Double.parseDouble(tokens[3]) : 0.0;
                r.volatility = tokens[4].length() > 0 ? Double.parseDouble(tokens[4]) : 0.0;
                return r;
            } catch (NumberFormatException nfe) {
                throw new RuntimeException("Invalid record: " + line, nfe);
            }
        });
    }
}
