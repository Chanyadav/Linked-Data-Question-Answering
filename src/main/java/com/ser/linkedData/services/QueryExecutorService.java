package com.ser.linkedData.services;

import org.apache.jena.query.*;
import org.apache.jena.rdf.model.RDFNode;
import org.springframework.stereotype.Service;

import java.io.FileNotFoundException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

@Service
public class QueryExecutorService {

    String serviceEndpoint = "http://18.188.58.48:3030/Country/sparql";

    private List<String> buildListResultsString(Iterator<QuerySolution> results, String varName) {
        List<String> resultsString = new ArrayList<>();
        for ( ; results.hasNext() ; )
        {
            QuerySolution soln = results.next();
            RDFNode rdfNode = soln.get(varName);
            resultsString.add(rdfNode.toString());
            System.out.println(soln.get(varName));
        }
        System.out.println(resultsString);
        return resultsString;
    }

    private List<String> buildResultsString(Iterator<QuerySolution> results, String varName) {
        List<String> resultsString = new ArrayList<>();
        if (results.hasNext())
        {
            QuerySolution soln = results.next();
            RDFNode rdfNode = soln.get(varName);
            resultsString.add(rdfNode.toString());
            System.out.println(soln.get(varName));
        }
        System.out.println(resultsString);
        return resultsString;
    }

    private List<String> buildFederatedResultsString(Iterator<QuerySolution> results, String varName1, String varName2) {
        List<String> resultsString = new ArrayList<>();
        if (results.hasNext())
        {
            QuerySolution soln = results.next();
            RDFNode rdfNode1 = soln.get(varName1);
            RDFNode rdfNode2 = soln.get(varName2);
            resultsString.add(rdfNode1.toString());
            resultsString.add(rdfNode2.toString());
        }
        System.out.println(resultsString);
        return resultsString;
    }

    private List<String> buildBooleanResultsString(Iterator<QuerySolution> results, String varName) {
        List<String> resultsString = new ArrayList<>();
        if (results.hasNext())
        {
            resultsString.add("True");
        } else {
            resultsString.add("False");
        }
        System.out.println(resultsString);
        return resultsString;
    }


    public List<String> executeFirstTemplateQuery(String typeOfNamedEntity, String colName, String namedEntityString) {

        String query = "PREFIX dbo: <http://www.semanticweb.org/avikh/ontologies/2020/10/demography#>\n" +
                "SELECT DISTINCT ?var \n" +
                "WHERE {\n" +
                "?class dbo:"+typeOfNamedEntity+" '"+namedEntityString+"'.\n" +
                "?class dbo:"+colName+" ?var" +
                "}\n" ;

        QueryExecution queryExecution = QueryExecutionFactory.sparqlService(serviceEndpoint, query);
        Iterator<QuerySolution> results = queryExecution.execSelect() ;
        List<String> resultsString = buildResultsString(results, "var");
        queryExecution.close();
        System.out.println(query);
        return resultsString;


//                String serviceEndpoint = "http://18.188.58.48:3030/sampleDataset/query";
//                String query = "PREFIX dbo: <http://www.semanticweb.org/avikh/ontologies/2020/10/demography#>\n" +
//                        "SELECT DISTINCT ?var \n" +
//                        "WHERE {\n" +
//                        "  ?a ?rdfType ?b\n" +
//                        "?class dbo:UniInstitution 'Arizona State University'.\n" +
//                        "?class dbo:UniWorldRank ?var" +
//                        "}\n" ;
//                QueryExecution queryExecution = QueryExecutionFactory.sparqlService(serviceEndpoint, query);
//                ResultSet resultSet = queryExecution.execSelect();
//                ResultSetFormatter.out(System.out, resultSet);

//                String serviceEndpoint = "http://18.188.58.48:3030/Country/sparql";
//                String query = "SELECT ?a ?rdfType ?b\n" +
//                        "WHERE {\n" +
//                        "  ?a ?rdfType ?b\n" +
//                        "}\n" +
//                        "LIMIT 25";
//                QueryExecution queryExecution = QueryExecutionFactory.sparqlService(serviceEndpoint, query);
//                ResultSet resultSet = queryExecution.execSelect();
//                ResultSetFormatter.out(System.out, resultSet);

//                    String wordList[] =
//                        {       "university", "universities",
//                                "publications", "published", "publish",
//                                "citations", "citation", "patented", "patents",
//                                "drafty", "draftsman" };
//                PorterStemmer porterStemmer = new PorterStemmer();
//                for (String word : wordList) {
//                    String stem = porterStemmer.stem(word);
//                    System.out.println("The stem of " + word + " is " + stem);
//                }
    }

    public List<String> executeSecondTemplateQuery(String typeOfNamedEntity, String colName, String namedEntityString) {

        String query = "PREFIX dbo: <http://www.semanticweb.org/avikh/ontologies/2020/10/untitled-ontology-42#>\n" +
                "SELECT DISTINCT ?var \n" +
                "WHERE {\n" +
                "?class dbo:" + typeOfNamedEntity + " '" + namedEntityString + "'.\n" +
                "?class dbo:" + colName + " ?var" +
                "}\n";

        QueryExecution queryExecution = QueryExecutionFactory.sparqlService(serviceEndpoint, query);
        Iterator<QuerySolution> results = queryExecution.execSelect() ;
        List<String> resultsString = buildResultsString(results, "var");
        queryExecution.close();
        return resultsString;
    }

    public List<String> executeBooleanFirstTemplateQuery(String typeOfNamedEntity, String typeOfNamedEntityString, String typeOfNamedEntity2, String typeOfNamedEntityString2){
        String query = "PREFIX dbo: <http://www.semanticweb.org/avikh/ontologies/2020/10/demography#>\n" +
                "SELECT DISTINCT ?class \n" +
                "WHERE {\n" +
                "?class dbo:" + typeOfNamedEntity + " '" + typeOfNamedEntityString + "'.\n" +
                "?class dbo:" + typeOfNamedEntity2 + "'"+typeOfNamedEntityString2 +"'"+
                "}\n";

        QueryExecution queryExecution = QueryExecutionFactory.sparqlService(serviceEndpoint, query);
        Iterator<QuerySolution> results = queryExecution.execSelect() ;
        List<String> resultsString = buildBooleanResultsString(results, "class");
        queryExecution.close();
        return resultsString;
    }

    public List<String> executeBooleanFirstHappIdxTemplateQuery(String typeOfNamedEntity, String typeOfNamedEntityString, String typeOfNamedEntity2, String typeOfNamedEntityString2){
        String query = "PREFIX dbo: <http://www.semanticweb.org/avikh/ontologies/2020/10/untitled-ontology-42#>\n" +
                "SELECT DISTINCT ?class \n" +
                "WHERE {\n" +
                "?class dbo:" + typeOfNamedEntity + " '" + typeOfNamedEntityString + "'.\n" +
                "?class dbo:" + typeOfNamedEntity2 + "'"+typeOfNamedEntityString2 +"'"+
                "}\n";

        QueryExecution queryExecution = QueryExecutionFactory.sparqlService(serviceEndpoint, query);
        Iterator<QuerySolution> results = queryExecution.execSelect() ;
        List<String> resultsString = buildBooleanResultsString(results, "class");
        queryExecution.close();
        return resultsString;
    }

    public List<String> executeMaxTemplateOneQuery(String orderColName, String subjectColName, String prefix) {
        System.out.println("orderColName: " + orderColName + ", subjectColName: " + subjectColName);
        String orderFlag = "DESC";
        if(orderColName.toLowerCase().contains("rank")){
            orderFlag = "ASC";
        }
        String query = prefix +
                "PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>\n" +
                "SELECT DISTINCT ?sub \n" +
                "WHERE {\n" +
                "?class dbo:" + orderColName + " ?var.\n" +
                "?class dbo:" + subjectColName + " ?sub.\n" +
                "}\n" +
                "ORDER BY " + orderFlag + "(xsd:integer(?var))\n" +
                "LIMIT 1\n";
        System.out.println(query);

        QueryExecution queryExecution = QueryExecutionFactory.sparqlService(serviceEndpoint, query);
        Iterator<QuerySolution> results = queryExecution.execSelect() ;
        List<String> resultsString = buildResultsString(results, "sub");
        queryExecution.close();
        return resultsString;
    }

    public List<String> executeMaxTemplateTwoQuery(String orderColName, String subjectColName, String typeOfNamedEntity, String namedEntityString, String prefix) {
        System.out.println("orderColName: " + orderColName + ", subjectColName: " + subjectColName + ", typeOfNamedEntity: " + typeOfNamedEntity + ", namedEntityString: " + namedEntityString);
        String orderFlag = "DESC";
        if(orderColName.toLowerCase().contains("rank")){
            orderFlag = "ASC";
        }
        String query = prefix +
                "PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>\n" +
                "SELECT DISTINCT ?sub \n" +
                "WHERE {\n" +
                "?class dbo:" + orderColName + " ?var.\n" +
                "?class dbo:" + subjectColName + " ?sub.\n" +
                "?class dbo:" + typeOfNamedEntity + " '" + namedEntityString + "'.\n" +
                "}\n" +
                "ORDER BY " + orderFlag + "(xsd:integer(?var))\n" +
                "LIMIT 1\n";
        System.out.println(query);

        QueryExecution queryExecution = QueryExecutionFactory.sparqlService(serviceEndpoint, query);
        Iterator<QuerySolution> results = queryExecution.execSelect() ;
        List<String> resultsString = buildResultsString(results, "sub");
        queryExecution.close();
        return resultsString;
    }

    public List<String> executeMinTemplateOneQuery(String orderColName, String subjectColName, String prefix) {
        System.out.println("orderColName: " + orderColName + ", subjectColName: " + subjectColName);
        String orderFlag = "ASC";
        if(orderColName.toLowerCase().contains("rank")){
            orderFlag = "DESC";
        }
        String query = prefix +
                "PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>\n" +
                "SELECT DISTINCT ?sub \n" +
                "WHERE {\n" +
                "?class dbo:" + orderColName + " ?var.\n" +
                "?class dbo:" + subjectColName + " ?sub.\n" +
                "}\n" +
                "ORDER BY " + orderFlag + "(xsd:integer(?var))\n" +
                "LIMIT 1\n";
        System.out.println(query);

        QueryExecution queryExecution = QueryExecutionFactory.sparqlService(serviceEndpoint, query);
        Iterator<QuerySolution> results = queryExecution.execSelect() ;
        List<String> resultsString = buildResultsString(results, "sub");
        queryExecution.close();
        return resultsString;
    }

    public List<String> executeMinTemplateTwoQuery(String orderColName, String subjectColName, String typeOfNamedEntity, String namedEntityString, String prefix) {
        System.out.println("orderColName: " + orderColName + ", subjectColName: " + subjectColName + ", typeOfNamedEntity: " + typeOfNamedEntity + ", namedEntityString:" + namedEntityString);
        String orderFlag = "ASC";
        if (orderColName.toLowerCase().contains("rank")) {
            orderFlag = "DESC";
        }
        String query = prefix +
                "PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>\n" +
                "SELECT DISTINCT ?sub \n" +
                "WHERE {\n" +
                "?class dbo:" + orderColName + " ?var.\n" +
                "?class dbo:" + subjectColName + " ?sub.\n" +
                "?class dbo:" + typeOfNamedEntity + " '" + namedEntityString + "'.\n" +
                "}\n" +
                "ORDER BY " + orderFlag + "(xsd:integer(?var))\n" +
                "LIMIT 1\n";
        System.out.println(query);

        QueryExecution queryExecution = QueryExecutionFactory.sparqlService(serviceEndpoint, query);
        Iterator<QuerySolution> results = queryExecution.execSelect() ;
        List<String> resultsString = buildResultsString(results, "sub");
        queryExecution.close();
        return resultsString;
    }

    public List<String> executeListTemplateQuery(String typeOfNamedEntity, String typeOfNamedEntity2, String namedEntityString2) throws FileNotFoundException {
        String query = "PREFIX dbo: <http://www.semanticweb.org/avikh/ontologies/2020/10/demography#>\n" +
                "SELECT DISTINCT ?var \n" +
                "WHERE {\n" +
                "?class dbo:"+typeOfNamedEntity2+" '"+namedEntityString2+"'.\n" +
                "?class dbo:"+typeOfNamedEntity+" ?var" +
                "}\n" ;
        System.out.println(query);

        QueryExecution queryExecution = QueryExecutionFactory.sparqlService(serviceEndpoint, query);
        Iterator<QuerySolution> results = queryExecution.execSelect() ;
        List<String> resultsString = buildListResultsString(results, "var");
        queryExecution.close();
        return resultsString;

//        List<String> result = new ArrayList<String>();
////        while(resultSet.hasNext())
//        {
//            QuerySolution querySolution = resultSet.nextSolution();
//            RDFNode rdfNode = querySolution.get("uni");
//            if(rdfNode != null){
//                result.add(rdfNode.toString());
//            }
//        }
//        PrintWriter pw=new PrintWriter(new File("result.html"));
//        pw.print("<html><body bgcolor=\"#EAE6F5\">");
//        pw.print("<h2 align=center><font color=\"#FF00FF\">SPARQL RESULT</font></h2>");
//        pw.print("<table border=1 align=\"center\">");
//        pw.print("<tr>");
//        for(int i=0;i<l.size();i++)
//            pw.print("<th bgcolor=\"#FFA500\"><font size=6>"+l.get(i)+"</font></th>");
//        pw.write("</tr>");
//        pw.print("<tbody bgcolor=\"#C0C0C0\">");
//        while(resultSet.hasNext())
//        {
//            QuerySolution qs=resultSet.nextSolution();
//            pw.print("<tr>");
//            for(int i=0;i<l.size();i++)
//            {
//                val=qs.get(l.get(i).toString()).toString();
//                pw.print("<td>"+val+"</td>");
//            }
//            pw.print("</tr>");
//        }
//        pw.print("</tbody></table>");
//        pw.print("</body></html>");
//        pw.close();
    }

    public List<String> executeLesserThanListQuery(String typeOfNamedEntity, String typeOfNamedEntity2, String namedEntityString2) {
        String query = "PREFIX dbo: <http://www.semanticweb.org/avikh/ontologies/2020/10/demography#>\n" +
                "PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>\n" +
                "SELECT DISTINCT ?var \n" +
                "WHERE {\n" +
                "?class dbo:"+typeOfNamedEntity+" ?var .\n" +
                "?class dbo:"+typeOfNamedEntity2+" ?p .\n" +
                "FILTER(xsd:integer(?p)<"+namedEntityString2+")\n" +
                "}\n" ;
        System.out.println(query);

        QueryExecution queryExecution = QueryExecutionFactory.sparqlService(serviceEndpoint, query);
        Iterator<QuerySolution> results = queryExecution.execSelect() ;
        List<String> resultsString = buildListResultsString(results, "var");
        queryExecution.close();
        return resultsString;
    }

    public List<String> executeGreaterThanListQuery(String typeOfNamedEntity, String typeOfNamedEntity2, String namedEntityString2) {
        String query = "PREFIX dbo: <http://www.semanticweb.org/avikh/ontologies/2020/10/demography#>\n" +
                "PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>\n" +
                "SELECT DISTINCT ?var \n" +
                "WHERE {\n" +
                "?class dbo:"+typeOfNamedEntity+" ?var .\n" +
                "?class dbo:"+typeOfNamedEntity2+" ?p .\n" +
                "FILTER(xsd:integer(?p)>"+namedEntityString2+")\n" +
                "}\n" ;
        System.out.println(query);

        QueryExecution queryExecution = QueryExecutionFactory.sparqlService(serviceEndpoint, query);
        Iterator<QuerySolution> results = queryExecution.execSelect() ;
        List<String> resultsString = buildListResultsString(results, "var");
        queryExecution.close();
        return resultsString;
    }

    public List<String> executeFederatedCovidTemplateQuery(String covidCountry, String covidCol, String happCountry, String happCol, String order) {

        String query = "PREFIX dbo: <http://www.semanticweb.org/avikh/ontologies/2020/10/demography#>\n" +
                "PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>\n" +
                "PREFIX dbo1: <http://www.semanticweb.org/avikh/ontologies/2020/10/untitled-ontology-42#>\n" +
                "SELECT ?country ?"+happCol+" \n" +
                "WHERE {\n" +
                "SERVICE <http://18.188.58.48:3030/Happinessindex/query>{\n" +
                "?hId dbo1:"+happCountry+" ?country.\n" +
                "?hId dbo1:"+ happCol +" ?"+happCol+"\n" +
                "}\n" +
                "SERVICE <http://18.188.58.48:3030/Country/sparql>{\n" +
                "?cId dbo:"+ covidCol+" ?x.\n" +
                "?cId dbo:"+ covidCountry +" ?country\n" +
                "}\n" +
                "}\n" +
                "ORDER BY "+order+"(xsd:integer(?x)) LIMIT 1";
        QueryExecution queryExecution = QueryExecutionFactory.sparqlService(serviceEndpoint, query);
        Iterator<QuerySolution> results = queryExecution.execSelect() ;
        List<String> resultsString = buildFederatedResultsString(results, "country", happCol);
        queryExecution.close();
        return resultsString;
    }

    public List<String> executeFederatedHappinessTemplateQuery(String covidCountry, String covidCol, String happCountry, String happCol, String order) {
        String query = "PREFIX dbo: <http://www.semanticweb.org/avikh/ontologies/2020/10/demography#>\n" +
                "PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>\n" +
                "PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>\n" +
                "PREFIX dbo1: <http://www.semanticweb.org/avikh/ontologies/2020/10/untitled-ontology-42#>\n" +
                "PREFIX xsd:   <http://www.w3.org/2001/XMLSchema#>\n" +
                "SELECT ?country ?"+covidCol+"\n" +
                "WHERE {\n" +
                "SERVICE <http://18.188.58.48:3030/Country/sparql>{\n" +
                "?cId dbo:"+covidCol+" ?"+covidCol+".\n" +
                "?cId dbo:"+covidCountry+" ?country\n" +
                "}\n" +
                "SERVICE <http://18.188.58.48:3030/Happinessindex/query>{\n" +
                "    ?hId dbo1:"+happCol+" ?freedom.\n" +
                "    ?hId dbo1:"+happCountry+" ?country.\n" +
                "}\n" +
                "}\n" +
                "ORDER BY "+order+"(xsd:decimal(?freedom)) LIMIT 1";
        QueryExecution queryExecution = QueryExecutionFactory.sparqlService(serviceEndpoint, query);
        Iterator<QuerySolution> results = queryExecution.execSelect() ;
        List<String> resultsString = buildFederatedResultsString(results, "country", covidCol);
        queryExecution.close();
        return resultsString;
    }


}