package com.ser.linkedData.services;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.HashMap;
import java.util.List;

@Service
public class HappinessIndexQueryService {

    @Autowired
    QueryExecutorService queryExecutorService;

    public List<String> fetchResults(HashMap<String, String> namedEntitiesHashMap, List<String> entities, String question, String questionType) {

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
        happinessRootToCol.put("country", "HappIdxCountry");

        String colName = "";
        String typeOfNamedEntity = "";
        String namedEntityString = "";
        String typeOfNamedEntity2 = "";
        String namedEntityString2 = "";

        for (String key : happinessRootToCol.keySet()) {
            for (String entity : entities) {
                entity = entity.toLowerCase();
                if (entity.contains(key)) {
                    colName = happinessRootToCol.get(key);
                    break;
                }
            }
        }

        if(namedEntitiesHashMap.containsKey("country")){
            typeOfNamedEntity = "HappIdxCountry";
            namedEntityString = namedEntitiesHashMap.get("country");
        }

        System.out.println(colName+"  -- "+typeOfNamedEntity);

        if(questionType.equals("entity")) {
            return queryExecutorService.executeSecondTemplateQuery(typeOfNamedEntity, colName, namedEntityString);
        }
        else if(questionType.equals("boolean")) {
            if(namedEntitiesHashMap.containsKey("number"))
            {
                namedEntityString2 = namedEntitiesHashMap.get("number");
                typeOfNamedEntity2 = colName;
            }
            System.out.println(""+ typeOfNamedEntity + " 2"+namedEntityString+" 3"+ typeOfNamedEntity2+  " 4"+ namedEntityString2);
            return queryExecutorService.executeBooleanFirstHappIdxTemplateQuery(typeOfNamedEntity, namedEntityString, typeOfNamedEntity2, namedEntityString2);

        } else {
            HashMap<String, String> happinessRootToOrderCol = new HashMap<>();
            happinessRootToOrderCol.put("corruption", "HappIdxCorruption");
            happinessRootToOrderCol.put("freedom", "HappIdxFreedom");
            happinessRootToOrderCol.put("gdp", "HappIdxGDP");
            happinessRootToOrderCol.put("capita", "HappIdxGDP");
            happinessRootToOrderCol.put("generosity", "HappIdxGenerosity");
            happinessRootToOrderCol.put("health", "HappIdxHealth");
            happinessRootToOrderCol.put("id", "HappIdxHId");
            happinessRootToOrderCol.put("rank", "HappIdxOverallRank");
            happinessRootToOrderCol.put("score", "HappIdxScore");
            happinessRootToOrderCol.put("social", "HappIdxSocialSupp");

            HashMap<String, String> happinessRootToSubjectCol = new HashMap<>();
            happinessRootToSubjectCol.put("country", "HappIdxCountry");

            String orderColName = "HappIdxScore";
            String subjectColName = "HappIdxCountry";

            System.out.println(entities);
            for (String key : happinessRootToOrderCol.keySet()) {
                for (String entity : entities) {
                    if (entity.contains(key)) {
                        orderColName = happinessRootToOrderCol.get(key);
                        break;
                    }
                }
            }

            for (String key : happinessRootToSubjectCol.keySet()) {
                for (String entity : entities) {
                    if (entity.contains(key)) {
                        subjectColName = happinessRootToSubjectCol.get(key);
                        break;
                    }
                }
            }

            String prefix = "PREFIX dbo: <http://www.semanticweb.org/avikh/ontologies/2020/10/untitled-ontology-42#>\n";

            if (question.toLowerCase().contains("highest") || question.toLowerCase().contains("top") ||
                    question.toLowerCase().contains("greatest") || question.toLowerCase().contains("best") ||
                    question.toLowerCase().contains("most")){
                if(namedEntitiesHashMap.containsKey("country")){
                    typeOfNamedEntity = "HappIdxCountry";
                    namedEntityString = namedEntitiesHashMap.get("country");
                    return queryExecutorService.executeMaxTemplateTwoQuery(orderColName, subjectColName, typeOfNamedEntity, namedEntityString, prefix);
                } else {
                    return queryExecutorService.executeMaxTemplateOneQuery(orderColName, subjectColName, prefix);
                }

            } else if (question.toLowerCase().contains("lowest") || question.toLowerCase().contains("bottom") ||
                    question.toLowerCase().contains("least") || question.toLowerCase().contains("smallest")) {
                if(namedEntitiesHashMap.containsKey("country")){
                    typeOfNamedEntity = "HappIdxCountry";
                    namedEntityString = namedEntitiesHashMap.get("country");
                    return queryExecutorService.executeMinTemplateTwoQuery(orderColName, subjectColName, typeOfNamedEntity, namedEntityString, prefix);
                } else {
                    return queryExecutorService.executeMinTemplateOneQuery(orderColName, subjectColName, prefix);
                }

            }
        }

        return null;
    }

}