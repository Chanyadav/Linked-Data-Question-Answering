package com.ser.linkedData.services;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.io.FileNotFoundException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;


@Service
public class CovidQueryService {

    @Autowired
    QueryExecutorService queryExecutorService;

    public List<String> fetchResults(HashMap<String, String> namedEntitiesHashMap, List<String> entities, String question, String questionType) throws FileNotFoundException {
        HashMap<String, List<String>> map = new HashMap<>();

        HashMap<String,String> covidRootToCol = new HashMap<>();
        covidRootToCol.put("id", "CovidCId");
        covidRootToCol.put("confirmed", "CovidConfirmed");
        covidRootToCol.put("country", "CovidCountry");
        covidRootToCol.put("death", "CovidDeaths");
        covidRootToCol.put("recovered", "CovidRecovered");
        covidRootToCol.put("active", "CovidActive");

        String colName = "";
        String typeOfNamedEntity = "";
        String namedEntityString = "";
        String typeOfNamedEntity2 = "";
        String namedEntityString2 = "";

        question = question.toLowerCase();
        for (String key : covidRootToCol.keySet()) {
            if(key.equals("id"))
                continue;
            if(question.contains(key)){
                colName = covidRootToCol.get(key);
                break;
            }
        }

        if(namedEntitiesHashMap.containsKey("country")){
            typeOfNamedEntity = "CovidCountry";
            namedEntityString = namedEntitiesHashMap.get("country");
        }

        System.out.println("Col Name = "+colName+"  namedEntityString "+namedEntityString);
        if(questionType.equals("entity")) {
            return queryExecutorService.executeFirstTemplateQuery(typeOfNamedEntity, colName, namedEntityString);
        } else if(questionType.equals("list")) {
            typeOfNamedEntity = "CovidCountry";
            if(namedEntitiesHashMap.containsKey("number"))
            {
                namedEntityString2 = namedEntitiesHashMap.get("number");
                typeOfNamedEntity2 = colName;
            }
            else {
                namedEntityString2 = namedEntitiesHashMap.get("country");
                typeOfNamedEntity2 = covidRootToCol.get("country");
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
            System.out.println("Covid boolean--> 1"+ typeOfNamedEntity + " 2"+namedEntityString+" 3"+ typeOfNamedEntity2+  " 4"+ namedEntityString2);
            return queryExecutorService.executeBooleanFirstTemplateQuery(typeOfNamedEntity, namedEntityString, typeOfNamedEntity2, namedEntityString2);

        } else {
            HashMap<String, String> covidRootToOrderCol = new HashMap<>();
            covidRootToOrderCol.put("id", "CovidCId");
            covidRootToOrderCol.put("confirmed", "CovidConfirmed");
            covidRootToOrderCol.put("death", "CovidDeaths");
            covidRootToOrderCol.put("recovered", "CovidRecovered");
            covidRootToOrderCol.put("active", "CovidActive");

            HashMap<String, String> covidRootToSubjectCol = new HashMap<>();
            covidRootToSubjectCol.put("country", "CovidCountry");

            String orderColName = "CovidConfirmed";
            String subjectColName = "CovidCountry";

            System.out.println(entities);
            for (String key : covidRootToOrderCol.keySet()) {
                for (String entity : entities) {
                    if (entity.contains(key)) {
                        orderColName = covidRootToOrderCol.get(key);
                        break;
                    }
                }
            }

            for (String key : covidRootToSubjectCol.keySet()) {
                for (String entity : entities) {
                    if (entity.contains(key)) {
                        subjectColName = covidRootToSubjectCol.get(key);
                        break;
                    }
                }
            }

            String prefix = "PREFIX dbo: <http://www.semanticweb.org/avikh/ontologies/2020/10/demography#>\n";

            if (question.toLowerCase().contains("highest") || question.toLowerCase().contains("top") ||
                    question.toLowerCase().contains("greatest") || question.toLowerCase().contains("best") ||
                    question.toLowerCase().contains("most")){
                if(namedEntitiesHashMap.containsKey("country")){
                    typeOfNamedEntity = "CovidCountry";
                    namedEntityString = namedEntitiesHashMap.get("country");
                    return queryExecutorService.executeMaxTemplateTwoQuery(orderColName, subjectColName, typeOfNamedEntity, namedEntityString, prefix);
                } else {
                    return queryExecutorService.executeMaxTemplateOneQuery(orderColName, subjectColName, prefix);
                }

            } else if (question.toLowerCase().contains("lowest") || question.toLowerCase().contains("bottom") ||
                    question.toLowerCase().contains("least") || question.toLowerCase().contains("smallest")) {
                if(namedEntitiesHashMap.containsKey("country")){
                    typeOfNamedEntity = "CovidCountry";
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
