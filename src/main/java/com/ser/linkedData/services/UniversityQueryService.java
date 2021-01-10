package com.ser.linkedData.services;

import org.apache.jena.query.QueryExecution;
import org.apache.jena.query.QueryExecutionFactory;
import org.apache.jena.query.ResultSet;
import org.apache.jena.query.ResultSetFormatter;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.io.FileNotFoundException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;

@Service
public class UniversityQueryService {

    @Autowired
    QueryExecutorService queryExecutorService;
    public List<String> fetchResults(HashMap<String, String> namedEntitiesHashMap, List<String> entities, String question,String questionType) throws FileNotFoundException {
        List<String> resultsString = new ArrayList<>();
        HashMap<String, List<String>> map = new HashMap<>();
        List<String> list = new ArrayList<>();
        list.add("UniUId");
        list.add("UniWorldRank");
        list.add("UniInstitution");
        list.add("UniCountry");
        list.add("UniPublications");
        list.add("UniCitations");
        list.add("UniPatents");
        list.add("UniScore");

        HashMap<String, String> uniRootToCol = new HashMap<>();
        uniRootToCol.put("id", "UniUId");
        uniRootToCol.put("rank", "UniWorldRank");
        uniRootToCol.put("university", "UniInstitution");
        uniRootToCol.put("country", "UniCountry");
        uniRootToCol.put("located","UniCountry");
        uniRootToCol.put("publication", "UniPublications");
        uniRootToCol.put("citation", "UniCitations");
        uniRootToCol.put("patent", "UniPatents");
        uniRootToCol.put("score", "UniScore");

        String colName = "";
        String typeOfNamedEntity = "";
        String namedEntityString = "";

        for (String key : uniRootToCol.keySet()) {
            for (String entity : entities) {
                if (entity.contains(key)) {
                    colName = uniRootToCol.get(key);
                    break;
                }
            }
        }

        //Add the entities to be detected here
        String typeOfNamedEntity2 = "";
        String namedEntityString2 = "";
        List<String>entitiesList = new ArrayList<>(Arrays.asList("located"));
        if(colName.equals(""))
        {
            for(String i: entitiesList)
            {
                if(question.contains(i))
                {
                    colName = uniRootToCol.get(i);
                    break;
                }
            }
        }


        if(namedEntitiesHashMap.containsKey("university")){
            typeOfNamedEntity = "UniInstitution";
            namedEntityString = namedEntitiesHashMap.get("university");
        }



        System.out.println(colName+"  -- "+typeOfNamedEntity+" namedEntityString="+namedEntityString);

        if(questionType.equals("entity")) {
            return queryExecutorService.executeFirstTemplateQuery(typeOfNamedEntity, colName, namedEntityString);
        }

        if(questionType.equals("list")) {
            typeOfNamedEntity = "UniInstitution";
            if(namedEntitiesHashMap.containsKey("number"))
            {
                namedEntityString2 = namedEntitiesHashMap.get("number");
                typeOfNamedEntity2 = colName;
            }
            else {
                namedEntityString2 = namedEntitiesHashMap.get("country");
                typeOfNamedEntity2 = uniRootToCol.get("country");
            }
            if(question.contains("greater") || question.contains("more")) {
                return queryExecutorService.executeGreaterThanListQuery(typeOfNamedEntity, typeOfNamedEntity2, namedEntityString2);
            } else if (question.contains("lesser") || question.contains("less")) {
                return queryExecutorService.executeLesserThanListQuery(typeOfNamedEntity, typeOfNamedEntity2, namedEntityString2);
            } else {
                return queryExecutorService.executeListTemplateQuery(typeOfNamedEntity, typeOfNamedEntity2, namedEntityString2);
            }

        }
        else if(questionType.equals("boolean")) {

            if(namedEntitiesHashMap.containsKey("number"))
            {
                namedEntityString2 = namedEntitiesHashMap.get("number");
                typeOfNamedEntity2 = colName;
            }
            else {
                namedEntityString2 = namedEntitiesHashMap.get("country");
                typeOfNamedEntity2 = uniRootToCol.get("country");

//                for (String key : namedEntitiesHashMap.keySet()) {
//                    if (colNameType1.equals("")) {
//                        if ()
//                            colNameType1 = uniRootToCol.get(key);
//                        colNameType1String = namedEntitiesHashMap.get(key);
//                    } else {
//                        colNameType2 = uniRootToCol.get(key);
//                        colNameType2String = namedEntitiesHashMap.get(key);
//                    }

                }

            System.out.println( typeOfNamedEntity + " "+namedEntityString+" "+ typeOfNamedEntity2+  " "+ namedEntityString2);


            return queryExecutorService.executeBooleanFirstTemplateQuery(typeOfNamedEntity, namedEntityString, typeOfNamedEntity2, namedEntityString2);
        }
        else{
            HashMap<String, String> uniRootToOrderCol = new HashMap<>();
            uniRootToOrderCol.put("publication", "UniPublications");
            uniRootToOrderCol.put("citation", "UniCitations");
            uniRootToOrderCol.put("patent", "UniPatents");
            uniRootToOrderCol.put("score", "UniScore");

            HashMap<String, String> uniRootToSubjectCol = new HashMap<>();
            uniRootToSubjectCol.put("country", "UniCountry");
            uniRootToSubjectCol.put("located","UniCountry");

            String orderColName = "UniWorldRank";
            String subjectColName = "UniInstitution";

            System.out.println(entities);
            for (String key : uniRootToOrderCol.keySet()) {
                for (String entity : entities) {
                    if (entity.contains(key)) {
                        orderColName = uniRootToOrderCol.get(key);
                        break;
                    }
                }
            }

            for (String key : uniRootToSubjectCol.keySet()) {
                for (String entity : entities) {
                    if (entity.contains(key)) {
                        subjectColName = uniRootToSubjectCol.get(key);
                        break;
                    }
                }
            }

            String prefix = "PREFIX dbo: <http://www.semanticweb.org/avikh/ontologies/2020/10/demography#>\n";

            if (question.toLowerCase().contains("highest") || question.toLowerCase().contains("top") ||
                question.toLowerCase().contains("greatest") || question.toLowerCase().contains("best") ||
                question.toLowerCase().contains("most")){
                if(namedEntitiesHashMap.containsKey("country")){
                    typeOfNamedEntity = "UniCountry";
                    namedEntityString = namedEntitiesHashMap.get("country");
                    return queryExecutorService.executeMaxTemplateTwoQuery(orderColName, subjectColName, typeOfNamedEntity, namedEntityString, prefix);
                } else {
                    return queryExecutorService.executeMaxTemplateOneQuery(orderColName, subjectColName, prefix);
                }

            } else if (question.toLowerCase().contains("lowest") || question.toLowerCase().contains("bottom") ||
                    question.toLowerCase().contains("least") || question.toLowerCase().contains("smallest")){
                if(namedEntitiesHashMap.containsKey("country")){
                    typeOfNamedEntity = "UniCountry";
                    namedEntityString = namedEntitiesHashMap.get("country");
                    return queryExecutorService.executeMinTemplateTwoQuery(orderColName, subjectColName, typeOfNamedEntity, namedEntityString, prefix);
                } else {
                    return queryExecutorService.executeMinTemplateOneQuery(orderColName, subjectColName, prefix);
                }

            }

        }

        return resultsString;
    }
}