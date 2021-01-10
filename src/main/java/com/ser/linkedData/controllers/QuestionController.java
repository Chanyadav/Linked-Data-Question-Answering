package com.ser.linkedData.controllers;

import com.ser.linkedData.services.*;
import com.textrazor.AnalysisException;
import com.textrazor.annotations.Word;
import opennlp.tools.postag.POSModel;
import opennlp.tools.postag.POSSample;
import opennlp.tools.postag.POSTaggerME;
import opennlp.tools.tokenize.WhitespaceTokenizer;
import org.apache.jena.query.*;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.servlet.ModelAndView;

import java.io.IOException;
import java.io.InputStream;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.textrazor.TextRazor;
import com.textrazor.annotations.Entity;
import com.textrazor.annotations.AnalyzedText;


@RestController
public class QuestionController {
	@Autowired
    UniversityQueryService universityService;

	@Autowired
    CovidQueryService covidQueryService;

	@Autowired
    DemographicsQueryService demographicsQueryService;

	@Autowired
    HappinessIndexQueryService happinessIndexQueryService;

    @Autowired
    FederatedQueryService federatedQueryService;

    @GetMapping("/askQuestion")
	public ModelAndView showLoginPage(Model model) {
        model.addAttribute("message", "sssss");
        return new ModelAndView("welcomePage");
    }

    @PostMapping("/getAnswer")
    public ModelAndView getAnswer(@RequestParam("question") String question, Model model) throws Exception {
        List<String> resultsString = new ArrayList<>();
        String serviceEndpoint = "http://18.188.58.48:3030/sampleDataset/query";
        try {
            if(question != null) {
                String questionType = "";
                // get entities and dataset values
                List<Object> obj = getEntities(question);
                List<String> entities = (List<String>) obj.get(0);
                List<String> namedEntities = (List<String>) obj.get(1);
                HashMap<String, String> namedEntitiesHashMap = (HashMap<String, String>) obj.get(2);
                String dataset = "";
                if(namedEntitiesHashMap.containsKey("university"))
                    dataset = "university";
                else
                    dataset = getDatasetName(question.toLowerCase());
                System.out.println("Entities found are = " + String.join(" ", entities));
                //System.out.println("Named Entities found are = " + String.join(" ", namedEntities));
                System.out.println("Named Entities in HashMap found are = "+namedEntitiesHashMap);
                System.out.println("Dataset = " + dataset);

                questionType = getQuestionType(question);
                switch (dataset) {
                    case "federated": {
                        resultsString = federatedQueryService.fetchResults(namedEntitiesHashMap, entities, question, questionType);
                        break;
                    }
                    case "university": {
                        resultsString = universityService.fetchResults(namedEntitiesHashMap, entities, question, questionType);
                        break;
                    }
                    case "covid": {
                        resultsString = covidQueryService.fetchResults(namedEntitiesHashMap, entities, question, questionType);
                        break;
                    }
                    case "demographics": {
                        resultsString = demographicsQueryService.fetchResults(namedEntitiesHashMap, entities, question, questionType);
                        break;
                    }
                    case "happiness": {
                        resultsString = happinessIndexQueryService.fetchResults(namedEntitiesHashMap, entities, question, questionType);
                        break;
                    }
                }
                ModelAndView modelAndView = new ModelAndView("displayAnswer");
                modelAndView.addObject("results", resultsString);
                return modelAndView;
            } else {
                model.addAttribute("message", "Invalid Input");
                return new ModelAndView("displayAnswer");
            }
        } catch (Exception e) {
            System.err.println(e.getStackTrace());
            model.addAttribute("message", "Internal Server Error");
            return new ModelAndView("displayAnswer");
        }

    }

    public String getQuestionType(String question)
    {
        question = question.toLowerCase();
        List<String> boolList = new ArrayList<>(Arrays.asList("is","are","does"));
        List<String> countList = new ArrayList<>(Arrays.asList("top","highest","lowest","maximum","minimum"));
        if(question.startsWith("List") || question.startsWith("list")) {
            return "list";
        }

        for(String str:boolList){
            if(question.startsWith(str))
                return "boolean";
        }
        for(String i: question.split(" "))
        {
            if(countList.contains(i))
            {
                return "count";
            }
        }

        return "entity";
    }
    public void constructQuery(String dataset, List<String> namedEntities,List<String> entities)
    {
        switch(dataset)
        {
            case "country":
            {
                HashMap<String, List<String>> map = new HashMap<>();
                List<String> list = new ArrayList<>();
                list.add("CountryName");
                list.add("CountryAlpha3");
                map.put("country",list);

                generateCountryQuery(dataset, map, entities, namedEntities);
            }
            case "university":
            {

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
                    uniRootToCol.put("publication", "UniPublications");
                    uniRootToCol.put("citation", "UniCitations");
                    uniRootToCol.put("patent", "UniPatents");
                    uniRootToCol.put("score", "UniScore");

                    String colName = "";
                    String typeOfNamedEntity = "";
                    for (String key : uniRootToCol.keySet()) {
                        for (String entity : entities) {
                            if (entity.contains(key)) {
                                colName = uniRootToCol.get(key);
                                break;
                            }
                        }
                    }

                    for(String namedEntity : namedEntities){
                        System.out.println("UNI NamedEnt = "+namedEntity);
                        if(namedEntity.contains("University") || namedEntity.contains("Institute") || namedEntity.contains("College")){
                            typeOfNamedEntity = "UniInstitution";
                        }
                    }

                    System.out.println(colName+"  -- "+typeOfNamedEntity);




                String serviceEndpoint = "http://18.188.58.48:3030/Country/sparql";
                String query = "PREFIX dbo: <http://www.semanticweb.org/avikh/ontologies/2020/10/demography#>\n" +
                        "SELECT DISTINCT ?var \n" +
                        "WHERE {\n" +
                        "?class dbo:"+typeOfNamedEntity+" 'Arizona State University'.\n" +
                        "?class dbo:"+colName+" ?var" +
                        "}\n" ;
                QueryExecution queryExecution = QueryExecutionFactory.sparqlService(serviceEndpoint, query);
                ResultSet resultSet = queryExecution.execSelect();
                ResultSetFormatter.out(System.out, resultSet);


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


                    map.put("university",list);
                    generateUniversityQuery(dataset,map, entities, namedEntities);
                    break;
            }
            case("suicide"):
            {

                HashMap<String, List<String>> map = new HashMap<>();
                List<String> list = new ArrayList<>();
                list.add("");
                list.add("");
                list.add("");
                list.add("");
                map.put("suicide",list);

                generateSuicideQuery(dataset,map, entities, namedEntities);
                break;
            }
            case("covid"):
            {

                HashMap<String, List<String>> map = new HashMap<>();
                List<String> list = new ArrayList<>();
                list.add("CovidCId");
                list.add("CovidConfirmed");
                list.add("CovidCountry");
                list.add("CovidDeaths");
                list.add("CovidRecovered");
                list.add("CovidActive");


                HashMap<String,String> covidRootToCol = new HashMap<>();
                covidRootToCol.put("id", "CovidCId");
                covidRootToCol.put("confirmed", "CovidConfirmed");
                covidRootToCol.put("country", "CovidCountry");
                covidRootToCol.put("death", "CovidDeaths");
                covidRootToCol.put("recovered", "CovidRecovered");
                covidRootToCol.put("active", "CovidActive");

                String colName = "";
                String typeOfNamedEntity = "CovidCountry";
                for (String key : covidRootToCol.keySet()) {
                    for (String entity : entities) {
                        if (entity.contains(key)) {
                            colName = covidRootToCol.get(key);
                            break;
                        }
                    }
                }


                map.put("covid",list);
                generateCovidQuery(dataset,map, entities, namedEntities);
                break;
            }

            case("demographics"):
            {
                HashMap<String, List<String>> map = new HashMap<>();
                List<String> list = new ArrayList<>();
                list.add("DemographyDId");
                list.add("DemographyCountry");
                list.add("DemographyPopulation");
                list.add("DemographyDensity");
                list.add("DemographyLandArea");
                list.add("DemographyMigrants");
                list.add("DemographyWorldShare");

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
                for (String key : demoRootToCol.keySet()) {
                    for (String entity : entities) {
                        if (entity.contains(key)) {
                            colName = demoRootToCol.get(key);
                            break;
                        }
                    }
                }


                map.put("demographics",list);
                generateDemographicsQuery(dataset,map, entities, namedEntities);
                break;
            }


        }
    }

    public String generateCountryQuery(String dataset, HashMap<String,List<String>> map, List<String> entities, List<String>namedEntities)
    {
        return "";
    }
    public String generateUniversityQuery(String dataset, HashMap<String,List<String>> map, List<String> entities, List<String>namedEntities)
    {

        return "";
    }
    public String generateSuicideQuery(String dataset, HashMap<String,List<String>> map, List<String> entities, List<String>namedEntities)
    {
        return "";
    }
    public String generateCovidQuery(String dataset, HashMap<String,List<String>> map, List<String> entities, List<String>namedEntities)
    {
        return "";
    }
    public String generateDemographicsQuery(String dataset, HashMap<String,List<String>> map, List<String> entities, List<String>namedEntities)
    {
        return "";
    }

    public List<Object> getEntities(String question) throws IOException, AnalysisException {

	    List<String> entities = new ArrayList<String>();
	    List<String> namedEntities = new ArrayList<String>();

        TextRazor client = new TextRazor("be613ce99139af6a99aca20eb72c9c5e00dd8b332e41b84982872b1f");

        client.addExtractor("words");
        client.addExtractor("entities");

        AnalyzedText response = client.analyze(question);
        AnalyzedText response2 = response;

        HashMap<String, String> namedEntitiesHashMap = new HashMap<>();
        if (response.getResponse().getEntities() != null) {
            for (Entity entity : response.getResponse().getEntities()) {

                if(entity.getDBPediaTypes() != null) {

                    System.out.println("Matched Entity: " + entity.getEntityId() + " ->" + entity.getDBPediaTypes());
                    if (entity.getDBPediaTypes().contains("Country") || entity.getDBPediaTypes().contains("Place") ) {
                        namedEntitiesHashMap.put("country", entity.getEntityId());
                        if(entity.getEntityId().equals("United States"))
                        {
                            namedEntitiesHashMap.put("country", "United States of America");
                        }
                    }
                    if (entity.getDBPediaTypes().contains("University")) {

                        namedEntitiesHashMap.put("university", entity.getEntityId());

                    }
//                if(entity.getDBPediaTypes().contains("Number")) {
//                    namedEntitiesHashMap.put("number", entity.getEntityId());
//                    System.out.println("number found text razor = "+namedEntitiesHashMap.get("number"));
//                }
                    if(!namedEntitiesHashMap.containsKey("number")){

                        String noSpaceQuestion = question.replace(" ", "");
                        noSpaceQuestion = question.replace(" ", "");


                        Pattern regex = Pattern.compile("(\\d+(?:\\.\\d+)?)");
                        Matcher matcher = regex.matcher(question);
                        while(matcher.find()){
                            System.out.println("REGEX MATCH===="+matcher.group(1));
                            namedEntitiesHashMap.put("number", matcher.group(1));
                        }



//                    String numberOnly = noSpaceQuestion.replaceAll("[^0-9]", "");
//                    if(!numberOnly.equals("")){
//                        namedEntitiesHashMap.put("number", numberOnly);
//                    }
                    }
                }
            }
        }


        System.out.println("namedEntitiesHashMap = "+namedEntitiesHashMap);

        String lemmaQuestion = "";
        for(Word w : response2.getResponse().getWords()){
            lemmaQuestion= lemmaQuestion+" "+w.getLemma();

        }
        System.out.println("LemmaQuestion = "+lemmaQuestion);

        // Reading the pos tagger model from OpenNLP
	    InputStream io = this.getClass().getClassLoader().getResourceAsStream("en-pos-perceptron.bin");
        POSModel model = new POSModel(io);
        POSTaggerME tagger = new POSTaggerME(model);
        WhitespaceTokenizer whitespaceTokenizer= WhitespaceTokenizer.INSTANCE;

        // Creating tokens for the input question and create tags for tokens
        //String[] tokens = whitespaceTokenizer.tokenize(question);
        System.out.println("Question passed to NLP ="+question);
        String[] tokens = whitespaceTokenizer.tokenize(question);
        String[] tags = tagger.tag(tokens);
        POSSample sample = new POSSample(tokens, tags);

        System.out.println(sample.toString());

        // Extracting only entities - which are nouns in the sentence
        // And named Entities - which will be proper nouns
        // https://www.ling.upenn.edu/courses/Fall_2003/ling001/penn_treebank_pos.html -> more POS tags
        String[] taggedWords = sample.toString().split(" ");
        for (String s : taggedWords) {
            String[] split = s.split("_");
            if (split[1].matches("NNP") || split[1].matches("NNPS")) {
                namedEntities.add(split[0]);
            } else if(split[1].contains("NN")) {
                entities.add(split[0]);
            }
        }
        return Arrays.asList(entities, namedEntities, namedEntitiesHashMap);
    }

    public String getDatasetName(String query) {
        String dataset = "country";
        final String[] demographics = new String[]{"population", "density", "area", "migrants", "share"};
        final String[] happiness = new String[]{"happiness", "GDP", "gdp", "Gdp", "freedom", "generosity", "corruption", "expectancy", "score"};

        String originalQuestion = query;
        String[] andTypeQuestions = originalQuestion.split(" ");
        List<String> andTypelist = new ArrayList<>(Arrays.asList(andTypeQuestions));
        if(andTypelist.contains("and") || andTypelist.contains("And")){
            String[] parts = originalQuestion.split("and");
            String part1 = parts[0];
            String part2 = parts[1];
            String part1Dataset = getDatasetName(part1);
            String part2Dataset = getDatasetName(part2);
            System.out.println("Insided federated d1="+part1Dataset+" d2="+part2Dataset+" q1="+part1+" q2="+part2);
            if(!part1Dataset.equals(part2Dataset)){
                return "federated";
            }
        }

        if (Arrays.stream(demographics).anyMatch(query::contains)) {
            dataset = "demographics";
        } else if (query.contains("covid")) {
            dataset = "covid";
        } else if (query.contains("university") || query.contains("universities")) {
            dataset = "university";
        } else if (Arrays.stream(happiness).anyMatch(query::contains)) {
            dataset = "happiness";
        }
        return dataset;
    }
}

// URL: http://localhost:8080/springbootmvc/
