package xmldb;


//originally from : joins.C

import iterator.*;
import heap.*;
import global.*;
import java.io.*;
import java.util.*;
import java.lang.*;
import org.w3c.dom.Document;
import org.w3c.dom.Node;
import org.xml.sax.SAXException;
//import queryprocessing.Query;

import diskmgr.PCounter;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;

import static tests.TestDriver.FAIL;
import static tests.TestDriver.OK;

@SuppressWarnings("Duplicates")


class QueryPlan2 implements GlobalConst {

    static int projInc = 4;
    private boolean OK = true;
    private boolean FAIL = false;
    private Vector xmlTuples;
    static XMLParser xmlParser = new XMLParser();

    /** Constructor
     */
    public QueryPlan2() {

        //build XMLTuple table
        xmlTuples  = new Vector();


        boolean status = OK;
        int numTuples = xmlParser.listOfXMLObjects.size();
        int numTuplesAttrs = 2;

        for (int i =0; (i < numTuples); i++) {
            //fixed length record
            XMLInputTreeNode node = xmlParser.listOfXMLObjects.get(i);
            xmlTuples.addElement(new XMLTuple(node.interval.start, node.interval.end, node.interval.level, node.tagName));
        }



        String dbpath = "/tmp/"+System.getProperty("user.name")+".minibase.jointestdb";
        String logpath = "/tmp/"+System.getProperty("user.name")+".joinlog";

        String remove_cmd = "/bin/rm -rf ";
        String remove_logcmd = remove_cmd + logpath;
        String remove_dbcmd = remove_cmd + dbpath;
        String remove_joincmd = remove_cmd + dbpath;

        try {
            Runtime.getRuntime().exec(remove_logcmd);
            Runtime.getRuntime().exec(remove_dbcmd);
            Runtime.getRuntime().exec(remove_joincmd);
        }
        catch (IOException e) {
            System.err.println (""+e);
        }


        SystemDefs sysdef = new SystemDefs( dbpath, 10000, 10000, "Clock" );

        // creating the sailors relation
        AttrType [] Stypes = new AttrType[numTuplesAttrs];
        Stypes[0] = new AttrType (AttrType.attrInterval);
        Stypes[1] = new AttrType (AttrType.attrString);

        //SOS
        short [] Ssizes = new short [1];
        Ssizes[0] = 5; //first elt. is 30

        Tuple t = new Tuple();
        try {
            t.setHdr((short) numTuplesAttrs, Stypes, Ssizes);
        }
        catch (Exception e) {
            System.err.println("*** error in Tuple.setHdr() ***");
            status = FAIL;
            e.printStackTrace();
        }

        int size = t.size();

        // inserting the tuple into file "sailors"
        RID             rid;
        Heapfile        f = null;
        try {
            f = new Heapfile("test.in");
        }
        catch (Exception e) {
            System.err.println("*** error in Heapfile constructor ***");
            status = FAIL;
            e.printStackTrace();
        }

        t = new Tuple(size);
        try {
            t.setHdr((short) numTuplesAttrs, Stypes, Ssizes);
        }
        catch (Exception e) {
            System.err.println("*** error in Tuple.setHdr() ***");
            status = FAIL;
            e.printStackTrace();
        }

        for (int i=0; i<numTuples; i++) {
            try {

                t.setIntervalFld(1, ((XMLTuple)xmlTuples.elementAt(i)).interval);
                //System.out.println(((XMLTuple)xmlTuples.elementAt(i)).tagName + " " + i);
                t.setStrFld(2, ((XMLTuple)xmlTuples.elementAt(i)).tagName);

            }
            catch (Exception e) {
                System.err.println("*** Heapfile error in Tuple.setStrFld() ***");
                status = FAIL;
                e.printStackTrace();
            }

            try {
                rid = f.insertRecord(t.returnTupleByteArray());
            }
            catch (Exception e) {
                System.err.println("*** error in Heapfile.insertRecord() ***");
                status = FAIL;
                e.printStackTrace();
            }
        }
        // System.out.println(xmlParser.listOfXMLObjects.size());

        if (status != OK) {
            //bail out
            System.err.println ("*** Error creating relation for sailors");
            Runtime.getRuntime().exit(1);
        }

    }

    
    public void queryHandler(String path) throws InvalidRelation, TupleUtilsException, FileScanException, IOException {



        Map<String,String> tagMap = new HashMap<>();
        
        //File Scan Operations
        //Reading 
        File file = new File("/Users/akshayrao/git/dbmsiPhase2/javaminibase/src/xmldbTestXML/XMLQueryInputTest.txt");
        Scanner scan =new Scanner(file);
        
        //ArrayLists for storing tags and rules from input
        ArrayList<String> tags = new ArrayList<>();
        ArrayList<ArrayList<String>> rules = new ArrayList<ArrayList<String>>();


        //Scan numberoftags, tags and rules from file
        int numberOfTags = scan.nextInt();
        for(int i=0; i<numberOfTags; i++){
            String tag = scan.next();
            if(tag.length() > 5)
                tag = tag.substring(0,5);
            tags.add(tag);

        }
        int j = 0;
        while(scan.hasNext()){
            ArrayList<String> temp = new ArrayList<>();
            int leftTag = scan.nextInt();
            int rightTag = scan.nextInt();
            String relation = scan.next();
            
            temp.add(tags.get(leftTag-1));
            temp.add(tags.get(rightTag-1));
            temp.add(relation);
           
            rules.add(temp);

        }
        
        //Adding to reverse tags
        ArrayList<String> reversedtags = new ArrayList<>();
        for(int i= tags.size()-1; i >=0; i--){
            reversedtags.add(tags.get(i));
        }

        //Adding reversed rules
        ArrayList<ArrayList<String>> reversedRules = new ArrayList<ArrayList<String>>();
        for(int i = rules.size()-1; i >= 0; i--) {
            reversedRules.add(rules.get(i));
        }

        ArrayList<ArrayList<String>> sortedRules = XMLQueryParsing.getSortedRules(tags, rules);
        ArrayList<ArrayList<String>> reverseSortedRules = XMLQueryParsing.getReverseSortedRules(reversedtags, reversedRules);

        //Printing reversed sorted list
/*        for(int i=0; i<reverseSortedRules.size(); i++){
//            for(int k=0; k<reverseSortedRules.get(i).size(); k++){
//                System.out.print(reverseSortedRules.get(i).get(k) + " ");
//            }
//            System.out.println();
//        } */

        
        NestedLoopsJoins result = processQuery(reverseSortedRules, tagMap);



        //Display the results

        int iteasd = 0;
        boolean done = false;
        Tuple t = new Tuple();
        try{
            while(!done && result!=null){
                t = result.get_next();
                if(t == null){
                    done = true;
                    break;
                }
                iteasd++;
                byte[] tupleArray = t.getTupleByteArray();

                for(int k=1;k<=projInc;k++){
                    if(k%2 != 0)
                        System.out.print(" | Start = " + t.getIntervalFld(k).getStart() + " End = " + t.getIntervalFld(k).getEnd() + " Level = " + t.getIntervalFld(k).getLevel());
                    else
                        System.out.print(" TagName = " + t.getStrFld(k) );

                }
                System.out.println("\n");




            }
        } catch(Exception e){
            e.printStackTrace();
        }

        System.out.println("\nRecords  returned by Nested Loop: " + iteasd);

    }

    public NestedLoopsJoins processQuery(ArrayList<ArrayList<String>> rule, Map<String,String> tagMap){


        // Creating conditional expression for the first rule
        CondExpr[] exprInit = new CondExpr[4];
        exprInit[0] = new CondExpr();
        exprInit[1] = new CondExpr();
        exprInit[2] = new CondExpr();
        exprInit[3] = new CondExpr();

        Map<String,Integer> tagFieldMap = new HashMap<>();

        tagFieldMap.put(rule.get(0).get(0),1);
        tagFieldMap.put(rule.get(0).get(1),3);

        createCondInit(exprInit, rule.get(0).get(0), rule.get(0).get(1),rule.get(0).get(2));



        AttrType [] Ttypes = new AttrType[2];
        Ttypes[0] = new AttrType (AttrType.attrInterval);
        Ttypes[1] = new AttrType (AttrType.attrString);

        //SOS
        short [] Tsizes = new short[1];
        Tsizes[0] = 5; //first elt. is 30

        FldSpec [] Tprojection = new FldSpec[2];
        Tprojection[0] = new FldSpec(new RelSpec(RelSpec.outer), 1);
        Tprojection[1] = new FldSpec(new RelSpec(RelSpec.outer), 2);

        FldSpec [] firstProjection = new FldSpec[4];
        firstProjection[0] = new FldSpec(new RelSpec(RelSpec.outer), 1);
        firstProjection[1] = new FldSpec(new RelSpec(RelSpec.outer), 2);
        firstProjection[2] = new FldSpec(new RelSpec(RelSpec.innerRel), 1);
        firstProjection[3] = new FldSpec(new RelSpec(RelSpec.innerRel), 2);



        boolean status = OK;

        FileScan am = null;


        try {

            am  = new FileScan("test.in", Ttypes, Tsizes,
                    (short)2, (short)2,
                    Tprojection, null);
        }
        catch (Exception e) {
            e.printStackTrace();
        }

        // Setting up nested loop join for the first rule
        NestedLoopsJoins nlj =null;
        try {
            nlj = new NestedLoopsJoins(Ttypes, 2, Tsizes, Ttypes,2,Tsizes,10,am,
                    "test.in",exprInit,null,firstProjection,4 );
        }
        catch (Exception e) {
            System.err.println("*** join error in NestedLoop1 constructor ***");
            status = FAIL;
            System.err.println (""+e);
            e.printStackTrace();
        }

        if (status != OK) {
            //bail out
            System.err.println ("*** Error constructing NestedLoop1");
            Runtime.getRuntime().exit(1);
        }

        // Looping over the rule list


        boolean GT = true;
        NestedLoopsJoins prev = nlj;

        for(int i = 1;i < rule.size(); i++){

            // For each rule
            CondExpr[] expr = new CondExpr[3];
            expr[0] = new CondExpr();
            expr[1] = new CondExpr();
            expr[2] = new CondExpr();

            expr[0].next  = null;
            expr[0].type1 = new AttrType(AttrType.attrSymbol);
            expr[0].type2 = new AttrType(AttrType.attrSymbol);
            expr[0].flag = 1;

            expr[1].op    = new AttrOperator(AttrOperator.aopEQ);
            expr[1].next  = null;
            expr[1].type1 = new AttrType(AttrType.attrSymbol);
            expr[1].type2 = new AttrType(AttrType.attrString);

            expr[2] = null;

            String first = rule.get(i).get(0);
            String second = rule.get(i).get(1);
            String currRule = rule.get(i).get(2);

            if(tagFieldMap.containsKey(first)){
                GT = true;
                expr[0].operand1.symbol = new FldSpec(new RelSpec(RelSpec.outer),tagFieldMap.get(first));
                expr[0].operand2.symbol = new FldSpec (new RelSpec(RelSpec.innerRel),1);
                expr[1].operand1.symbol = new FldSpec(new RelSpec(RelSpec.innerRel),2);
                expr[1].operand2.string = second;
                tagFieldMap.put(second,projInc+1);
            }else if(tagFieldMap.containsKey(second)){
                GT = false;
                expr[0].operand1.symbol = new FldSpec(new RelSpec(RelSpec.outer),tagFieldMap.get(second));
                expr[0].operand2.symbol = new FldSpec (new RelSpec(RelSpec.innerRel),1);
                expr[1].operand1.symbol = new FldSpec(new RelSpec(RelSpec.innerRel),2);
                expr[1].operand2.string = first;
                tagFieldMap.put(first,projInc+1);

            }

            if(currRule.equals("AD")){
                if(GT){
                    expr[0].op    = new AttrOperator(AttrOperator.aopGT);
                }
                else{
                    expr[0].op    = new AttrOperator(AttrOperator.aopLT);
                }

            }else if(currRule.equals("PC")){
                if(GT){
                    expr[0].op    = new AttrOperator(AttrOperator.aopPC);
                }
                else{
                    expr[0].op    = new AttrOperator(AttrOperator.aopCP);
                }

            }




            //System.out.println("Inside:"+tagFieldMap);
            tagFieldMap.put(rule.get(0).get(0),1);


            AttrType [] LeftTtypes = new AttrType[projInc];
            for(int iter = 0; iter<projInc;iter=iter+2){
                LeftTtypes[iter] = new AttrType (AttrType.attrInterval);
                LeftTtypes[iter+1] = new AttrType (AttrType.attrString);
            }

            int leftSizeVal = i+1;
            short [] leftSizes = new short[leftSizeVal];
            for(int iter = 0; iter<leftSizeVal;iter++){
                leftSizes[iter] = 5;
            }


            FldSpec [] outProjection = new FldSpec[projInc+2];
            int k;

            if(!GT){
                for(k=0;k<projInc;k++){
                    //TODO verify if outer works always
                    outProjection[k] = new FldSpec(new RelSpec(RelSpec.outer), k+1);
                }
                outProjection[k] = new FldSpec(new RelSpec(RelSpec.innerRel), 1);
                outProjection[k+1] = new FldSpec(new RelSpec(RelSpec.innerRel), 2);


            }
            else{
                for(k=0;k<projInc;k++){
                    //TODO verify if outer works always
                    outProjection[k] = new FldSpec(new RelSpec(RelSpec.outer), k+1);
                }
                outProjection[k] = new FldSpec(new RelSpec(RelSpec.innerRel), 1);
                outProjection[k+1] = new FldSpec(new RelSpec(RelSpec.innerRel), 2);


//                outProjection[0] = new FldSpec(new RelSpec(RelSpec.outer), 1);
//                outProjection[1] = new FldSpec(new RelSpec(RelSpec.outer), 2);
//                for(k=2;k<projInc+2;k++){
//                    //TODO verify if outer works always
//                    outProjection[k] = new FldSpec(new RelSpec(RelSpec.innerRel), k+1);
//                }
//                for(k=0;k<projInc;k++){
//                    //TODO verify if outer works always
//                    outProjection[k] = new FldSpec(new RelSpec(RelSpec.outer), k+1);
//                }
//                outProjection[k] = new FldSpec(new RelSpec(RelSpec.innerRel), 1);
//                outProjection[k+1] = new FldSpec(new RelSpec(RelSpec.innerRel), 2);
            }

            NestedLoopsJoins nljLoop =null;
            try {
                nljLoop = new NestedLoopsJoins(LeftTtypes, projInc, leftSizes, Ttypes,2,Tsizes,10,prev,
                        "test.in",expr,null,outProjection,projInc+2 );
            }
            catch (Exception e) {
                System.err.println("*** join error in NestedLoop1 constructor ***");
                status = FAIL;
                System.err.println (""+e);
                e.printStackTrace();
            }

            if (status != OK) {
                //bail out
                System.err.println ("*** Error constructing NestedLoop1");
                Runtime.getRuntime().exit(1);
            }
            projInc = projInc+2;
            prev = nljLoop;

            if(i==rule.size()-1){
                return nljLoop;
            }



        }

        return nlj;
    }



    public void createCondInit(CondExpr[] expr, String first, String second, String rule){



        expr[0].next  = null;
        if(rule.equals("AD")){
            expr[0].op    = new AttrOperator(AttrOperator.aopGT);
        }else{
            expr[0].op    = new AttrOperator(AttrOperator.aopPC);
        }

        expr[0].type1 = new AttrType(AttrType.attrSymbol);
        expr[0].type2 = new AttrType(AttrType.attrSymbol);
        expr[0].operand1.symbol = new FldSpec(new RelSpec(RelSpec.outer),1);
        expr[0].operand2.symbol = new FldSpec (new RelSpec(RelSpec.innerRel),1);
        expr[0].flag = 1;


        expr[1].op    = new AttrOperator(AttrOperator.aopEQ);
        expr[1].type1 = new AttrType(AttrType.attrSymbol);
        expr[1].type2 = new AttrType(AttrType.attrString);
        expr[1].operand1.symbol = new FldSpec(new RelSpec(RelSpec.outer),2);
        expr[1].operand2.string = first;
        expr[1].next  = null;


        expr[2].op    = new AttrOperator(AttrOperator.aopEQ);
        expr[2].next  = null;
        expr[2].type1 = new AttrType(AttrType.attrSymbol);
        expr[2].type2 = new AttrType(AttrType.attrString);
        expr[2].operand1.symbol = new FldSpec(new RelSpec(RelSpec.innerRel),2);
        expr[2].operand2.string = second;

        expr[3] = null;


    }


    public static void main(String argv[]) throws ParserConfigurationException, IOException, SAXException, TupleUtilsException, FileScanException, InvalidRelation {

        DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();
        dbf.setValidating(false);
        DocumentBuilder db = dbf.newDocumentBuilder();
        Document doc = db.parse(new FileInputStream(new File("/Users/akshayrao/git/dbmsiPhase2/javaminibase/src/xmldbTestXML/sample_data2.xml")));
        String patternTreePath="/Users/akshayrao/git/dbmsiPhase2/javaminibase/src/xmldbTestXML/XMLQueryInputTest.txt";
        
        Node root = doc.getDocumentElement();

        xmlParser.build(root);
        xmlParser.preOrder(xmlParser.tree.root);
        xmlParser.BFSSetLevel();

        PCounter.initialize();
        QueryPlan2 xmlinsert = new QueryPlan2();
        xmlinsert.queryHandler(patternTreePath);        
        
        System.out.println("Read Counter = " + PCounter.rcounter);
        System.out.println("Write Counter = " + PCounter.wcounter);
        System.out.println("Total = " + (PCounter.rcounter + PCounter.wcounter));
    }
}



