
package com.ser.linkedData.services;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import java.util.HashMap;
import java.util.List;

@Service
public class FederatedQueryService {

    @Autowired
    QueryExecutorService queryExecutorService;


    public List<String> fetchResults(HashMap<String, String> namedEntitiesHashMap, List<String> entities, String question, String questionType) {

        HashMap<String, String> covidRootToCol = new HashMap<>();
//        covidRootToCol.put("id", "CovidCId");
        covidRootToCol.put("confirm", "CovidConfirmed");
//        covidRootToCol.put("country", "CovidCountry");
        covidRootToCol.put("death", "CovidDeaths");
        covidRootToCol.put("recover", "CovidRecovered");
        covidRootToCol.put("active", "CovidActive");

        HashMap<String, String> happinessRootToCol = new HashMap<>();
        happinessRootToCol.put("corruption", "HappIdxCorruption");
        happinessRootToCol.put("freedom", "HappIdxFreedom");
        happinessRootToCol.put("gdp", "HappIdxGDP");
        happinessRootToCol.put("capita", "HappIdxGDP");
        happinessRootToCol.put("generosity", "HappIdxGenerosity");
        happinessRootToCol.put("health", "HappIdxHealth");
        happinessRootToCol.put("id", "HappIdxHId");
        happinessRootToCol.put("rank", "HappIdxOverallRank");
        happinessRootToCol.put("score", "HappIdxScore");
        happinessRootToCol.put("social", "HappIdxSocialSupp");
//        happinessRootToCol.put("country", "HappIdxCountry");

        String order = "DESC";
        if (question.contains("lowest") || question.contains("minimum")) {
            order = "ASC";
        }

        String[] questions = question.split("and");
        String leftQuestion = questions[0];
        String rightQuestion = questions[1];
        String leftDataset = "";
        String rightDataset = "";
        String leftColName = "";
        String rightColName = "";
        String leftCountry = "";
        String rightCountry = "";
        if (leftQuestion.contains("covid")) {
            leftDataset = "covid";
            leftColName = getColName(leftQuestion, covidRootToCol);
            leftCountry = "CovidCountry";
            rightDataset = "happiness";
            rightColName = getColName(rightQuestion, happinessRootToCol);
            rightCountry = "HappIdxCountry";
        } else {
            rightDataset = "covid";
            rightColName = getColName(rightQuestion, covidRootToCol);
            rightCountry = "CovidCountry";
            leftCountry = "HappIdxCountry";
            leftDataset = "happiness";
            leftColName = getColName(leftQuestion, happinessRootToCol);

        }

        System.out.println("leftDataset=" + leftDataset + " leftColName=" + leftColName + " rightDataset=" + rightDataset + " rightColName=" + rightColName);
        if (leftDataset.equals("covid")) {
            return queryExecutorService.executeFederatedCovidTemplateQuery(leftCountry, leftColName, rightCountry, rightColName, order);
        } else {
            return queryExecutorService.executeFederatedHappinessTemplateQuery(rightCountry, rightColName, leftCountry, leftColName, order);
        }
    }

    String getColName(String question, HashMap<String, String> hashMap) {
        question = question.toLowerCase();
        for (String key : hashMap.keySet()) {
            if (question.contains(key)) {
                return hashMap.get(key);
            }
        }
        return "?var";
    }
}
