package com.ser.linkedData.services;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.io.FileNotFoundException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

@Service
public class DemographicsQueryService {

    @Autowired
    QueryExecutorService queryExecutorService;
    public List<String> fetchResults(HashMap<String, String> namedEntitiesHashMap, List<String> entities, String question, String questionType) throws FileNotFoundException {

        HashMap<String, String> demoRootToCol = new HashMap<>();
        demoRootToCol.put("id", "DemographyDId");
        demoRootToCol.put("country", "DemographyCountry");
        demoRootToCol.put("population", "DemographyPopulation");
        demoRootToCol.put("density", "DemographyDensity");
        demoRootToCol.put("area", "DemographyLandArea");
        demoRootToCol.put("migrant", "DemographyMigrants");
        demoRootToCol.put("share", "DemographyWorldShare");

        String colName = "";
        String typeOfNamedEntity = "DemographyCountry";
        String namedEntityString = "";
        String typeOfNamedEntity2 = "";
        String namedEntityString2 = "";

        question = question.toLowerCase();

        for (String key : demoRootToCol.keySet()) {
            for (String entity : entities) {
                if (entity.contains(key)) {
                    colName = demoRootToCol.get(key);
                    break;
                }
            }
        }

        if(namedEntitiesHashMap.containsKey("country")){
            typeOfNamedEntity = "DemographyCountry";
            namedEntityString = namedEntitiesHashMap.get("country");
        }

        System.out.println(colName+"  -- "+typeOfNamedEntity+" namedEntityString="+namedEntityString);
        if(questionType.equals("entity")) {
            return queryExecutorService.executeFirstTemplateQuery(typeOfNamedEntity, colName, namedEntityString);
        } else if(questionType.equals("list")) {
            typeOfNamedEntity = "DemographyCountry";
            if(namedEntitiesHashMap.containsKey("number"))
            {
                namedEntityString2 = namedEntitiesHashMap.get("number");
                typeOfNamedEntity2 = colName;
                if (question.contains("density") || question.contains("densities"))
                    typeOfNamedEntity2 = "DemographyDensity";
            }
            else {
                namedEntityString2 = namedEntitiesHashMap.get("country");
                typeOfNamedEntity2 = demoRootToCol.get("country");
            }
            if(question.contains("greater") || question.contains("more")) {
                return queryExecutorService.executeGreaterThanListQuery(typeOfNamedEntity, typeOfNamedEntity2, namedEntityString2);
            } else if (question.contains("lesser") || question.contains("less")) {
                return queryExecutorService.executeLesserThanListQuery(typeOfNamedEntity, typeOfNamedEntity2, namedEntityString2);
            } else {
                return queryExecutorService.executeListTemplateQuery(typeOfNamedEntity, typeOfNamedEntity2, namedEntityString2);
            }
        } else if(questionType.equals("boolean")){
            if(namedEntitiesHashMap.containsKey("number"))
            {
                namedEntityString2 = namedEntitiesHashMap.get("number");
                typeOfNamedEntity2 = colName;
            }
            System.out.println(""+ typeOfNamedEntity + " 2"+namedEntityString+" 3"+ typeOfNamedEntity2+  " 4"+ namedEntityString2);
            return queryExecutorService.executeBooleanFirstTemplateQuery(typeOfNamedEntity, namedEntityString, typeOfNamedEntity2, namedEntityString2);

        } else {
            HashMap<String, String> demoRootToOrderCol = new HashMap<>();
            demoRootToOrderCol.put("population", "DemographyPopulation");
            demoRootToOrderCol.put("density", "DemographyDensity");
            demoRootToOrderCol.put("area", "DemographyLandArea");
            demoRootToOrderCol.put("migrant", "DemographyMigrants");
            demoRootToOrderCol.put("share", "DemographyWorldShare");

            HashMap<String, String> demoRootToSubjectCol = new HashMap<>();
            demoRootToSubjectCol.put("country", "DemographyCountry");

            String orderColName = "DemographyPopulation";
            String subjectColName = "DemographyCountry";

            System.out.println(entities);
            for (String key : demoRootToOrderCol.keySet()) {
                for (String entity : entities) {
                    if (entity.contains(key)) {
                        orderColName = demoRootToOrderCol.get(key);
                        break;
                    }
                }
            }

            for (String key : demoRootToSubjectCol.keySet()) {
                for (String entity : entities) {
                    if (entity.contains(key)) {
                        subjectColName = demoRootToSubjectCol.get(key);
                        break;
                    }
                }
            }

            String prefix = "PREFIX dbo: <http://www.semanticweb.org/avikh/ontologies/2020/10/demography#>\n";

            if (question.toLowerCase().contains("highest") || question.toLowerCase().contains("top") ||
                    question.toLowerCase().contains("greatest") || question.toLowerCase().contains("best") ||
                    question.toLowerCase().contains("most")){
                if(namedEntitiesHashMap.containsKey("country")){
                    typeOfNamedEntity = "DemographyCountry";
                    namedEntityString = namedEntitiesHashMap.get("country");
                    return queryExecutorService.executeMaxTemplateTwoQuery(orderColName, subjectColName, typeOfNamedEntity, namedEntityString, prefix);
                } else {
                    return queryExecutorService.executeMaxTemplateOneQuery(orderColName, subjectColName, prefix);
                }

            } else if (question.toLowerCase().contains("lowest") || question.toLowerCase().contains("bottom") ||
                    question.toLowerCase().contains("least") || question.toLowerCase().contains("smallest")) {
                if(namedEntitiesHashMap.containsKey("country")){
                    typeOfNamedEntity = "DemographyCountry";
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
