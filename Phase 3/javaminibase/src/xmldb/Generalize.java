package xmldb;

import btree.BTreeFile;
import btree.IntervalKey;
import btree.StringKey;
import index.IndexScan;
import intervaltree.IntervalTreeFile;
import iterator.*;
import heap.*;
import global.*;
import java.io.*;
import java.util.*;
import java.lang.*;

import iterator.Iterator;
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


class Generalize implements GlobalConst {

    static int projInc = 4;
    private boolean OK = true;
    private boolean FAIL = false;
    private Vector xmlTuples;
    static XMLParser xmlParser = new XMLParser();
    IndexType b_index;
    IndexType i_index;

    /** Constructor
     */
    public Generalize() {

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



        SystemDefs sysdef = new SystemDefs(dbpath, 10000, 10000, "Clock");

        // creating the sailors relation
        AttrType[] Stypes = new AttrType[numTuplesAttrs];
        Stypes[0] = new AttrType(AttrType.attrInterval);
        Stypes[1] = new AttrType(AttrType.attrString);

        //SOS
        short[] Ssizes = new short[1];
        Ssizes[0] = 10; //first elt. is 30

        Tuple t = new Tuple();
        try {
            t.setHdr((short) numTuplesAttrs, Stypes, Ssizes);
        } catch (Exception e) {
            System.err.println("*** error in Tuple.setHdr() ***");
            status = FAIL;
            e.printStackTrace();
        }

        int size = t.size();

        // inserting the tuple into file "sailors"
        RID rid;
        Heapfile f = null;
        try {
            f = new Heapfile("test.in");
        } catch (Exception e) {
            System.err.println("*** error in Heapfile constructor ***");
            status = FAIL;
            e.printStackTrace();
        }

        t = new Tuple(size);
        try {
            t.setHdr((short) numTuplesAttrs, Stypes, Ssizes);
        } catch (Exception e) {
            System.err.println("*** error in Tuple.setHdr() ***");
            status = FAIL;
            e.printStackTrace();
        }

        for (int i = 0; i < numTuples; i++) {
            try {

                t.setIntervalFld(1, ((XMLTuple) xmlTuples.elementAt(i)).interval);
                System.out.println(((XMLTuple) xmlTuples.elementAt(i)).tagName + " " + i);
                t.setStrFld(2, ((XMLTuple) xmlTuples.elementAt(i)).tagName);

            } catch (Exception e) {
                System.err.println("*** Heapfile error in Tuple.setStrFld() ***");
                status = FAIL;
                e.printStackTrace();
            }

            try {
                rid = f.insertRecord(t.returnTupleByteArray());
            } catch (Exception e) {
                System.err.println("*** error in Heapfile.insertRecord() ***");
                status = FAIL;
                e.printStackTrace();
            }
        }

        Scan scan = null;

        try {
            scan = new Scan(f);
        } catch (Exception e) {
            status = FAIL;
            e.printStackTrace();
            Runtime.getRuntime().exit(1);
        }

        // create the index file
        IntervalTreeFile itf = null;
        BTreeFile btf = null;

        try {

            itf = new IntervalTreeFile("IntervalTreeIndex", AttrType.attrInterval, 12, 1/*delete*/);
            btf = new BTreeFile("BTreeIndex", AttrType.attrString, 10, 1/*delete*/);
        } catch (Exception e) {
            status = FAIL;
            e.printStackTrace();
            Runtime.getRuntime().exit(1);
        }

        System.out.println("BTreeIndex created successfully.\n");

        rid = new RID();
        IntervalType iKey = null;
        String tKey = null;
        Tuple temp = null;

        try {
            temp = scan.getNext(rid);
        } catch (Exception e) {
            status = FAIL;
            e.printStackTrace();
        }
        while (temp != null) {
            t.tupleCopy(temp);

            try {
                iKey = t.getIntervalFld(1);
                tKey = t.getStrFld(2);
            } catch (Exception e) {
                status = FAIL;
                e.printStackTrace();
            }

            try {
                itf.insert(new IntervalKey(iKey), rid);
                btf.insert(new StringKey(tKey), rid);
                //
            } catch (Exception e) {
                status = FAIL;
                e.printStackTrace();
            }

            try {
                temp = scan.getNext(rid);
            } catch (Exception e) {
                status = FAIL;
                e.printStackTrace();
            }
        }

        // close the file scan
        scan.closescan();

    }

    public void parseComplexPT(String path) throws IOException, InvalidRelation, TupleUtilsException, FileScanException {


        //Reading complex pattern tree file
        File file = new File(path);
        Scanner scan =new Scanner(file);
        boolean issecond = false;

        String filePath = path.substring(0,path.lastIndexOf("/")+1);


        String firstPT = scan.nextLine();
        firstPT = firstPT.substring(firstPT.indexOf(": ")+2);

        String secondPT = scan.nextLine();
        String [] operation = null;
        String nextline;
        String buffer;
        if(secondPT.indexOf("ptree") != -1 ){
            secondPT = secondPT.substring(secondPT.indexOf(": ")+2);
            issecond = true;
            nextline = scan.nextLine();
            nextline = nextline.substring(nextline.indexOf(": ")+2);
            operation = nextline.split(" ");
            buffer = scan.nextLine();
            buffer = buffer.substring(buffer.indexOf(": ")+2);
        } else if(secondPT.indexOf("operation") != -1){
            // secondPT will contain operation
            nextline = secondPT.substring(secondPT.indexOf(": ")+2);
            operation = nextline.split(" ");
            buffer = scan.nextLine();
            buffer = buffer.substring(buffer.indexOf(": ")+2);
        } else if(secondPT.indexOf("buf") != -1){
            operation = new String[1];
            operation[0] = "SEL";
            buffer = secondPT.substring(secondPT.indexOf(": ")+2);
        }
//         = scan.nextLine();
//        buffer = buffer.substring(buffer.indexOf(": ")+2);


        //ArrayLists for storing tags and rules from input
        Map<String,String> tagMap = new HashMap<>();
        ArrayList<String> tags = new ArrayList<>();
        ArrayList<ArrayList<String>> rules = new ArrayList<>();

        // Scan the first pattern tree

        File filePT1 = new File(filePath+firstPT+".txt");
        Scanner scanner = new Scanner(filePT1);

        //Scan numberoftags, tags and rules from file
        int numberOfTags = scanner.nextInt();
        for(int i=0; i<numberOfTags; i++){
            String tag = scanner.next();
            if(tag.length() > 5)
                tag = tag.substring(0,5);
            tags.add(tag);

        }
        int j = 0;

        while(scanner.hasNext()){
            ArrayList<String> temp = new ArrayList<>();
            String leftTag = scanner.next();
            String rightTag = scanner.next();
            String relation = scanner.next();

            temp.add(leftTag);
            temp.add(rightTag);
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


        ArrayList<String> tagInit = new ArrayList<>();
        for(int i =1;i<=numberOfTags;i++){
            tagInit.add(Integer.toString(i));
        }

        ArrayList<ArrayList<String>> sortedRules = XMLQueryParsing.getSortedRules(tagInit, rules);


        // Parse the second pattern tree
        ArrayList<ArrayList<String>> sortedRules2 = null;
        ArrayList<String> tags2 = null;

        if(issecond){
            //ArrayLists for storing tags and rules from input
            Map<String,String> tagMap2 = new HashMap<>();
            tags2 = new ArrayList<>();
            ArrayList<ArrayList<String>> rules2 = new ArrayList<>();

            // Scan the first pattern tree

            File filePT2 = new File(filePath+secondPT+".txt");
            Scanner scanner2 = new Scanner(filePT2);

            //Scan numberoftags, tags and rules from file
            int numberOfTags2 = scanner2.nextInt();
            for(int i=0; i<numberOfTags2; i++){
                String tag = scanner2.next();
                if(tag.length() > 5)
                    tag = tag.substring(0,5);
                tags2.add(tag);

            }

            while(scanner2.hasNext()){
                ArrayList<String> temp = new ArrayList<>();
                String leftTag = scanner2.next();
                String rightTag = scanner2.next();
                String relation = scanner2.next();

                temp.add(leftTag);
                temp.add(rightTag);
                temp.add(relation);

                rules2.add(temp);

            }

            //Adding to reverse tags
            ArrayList<String> reversedtags2 = new ArrayList<>();
            for(int i= tags2.size()-1; i >=0; i--){
                reversedtags2.add(tags2.get(i));
            }

            //Adding reversed rules
            ArrayList<ArrayList<String>> reversedRules2 = new ArrayList<ArrayList<String>>();
            for(int i = rules2.size()-1; i >= 0; i--) {
                reversedRules2.add(rules2.get(i));
            }


            ArrayList<String> tagInit2 = new ArrayList<>();
            for(int i =1;i<=numberOfTags2;i++){
                tagInit2.add(Integer.toString(i));
            }

            sortedRules2 = XMLQueryParsing.getSortedRules(tagInit2, rules2);
        }



        Iterator result = processIntervalQuery1(sortedRules, tags);

//        System.out.println("projInc: "+projInc);
//        int iteasd = 0;
//        boolean done = false;
//        Tuple t = new Tuple();
//        try{
//            while(!done && result!=null){
//                t = result.get_next();
//                if(t == null){
//                    done = true;
//                    break;
//                }
//                iteasd++;
//                byte[] tupleArray = t.getTupleByteArray();
//
//                for(int k=1;k<=projInc;k++){
//                    if(k%2 != 0)
//                        System.out.print(" | Start = " + t.getIntervalFld(k).getStart() + " End = " + t.getIntervalFld(k).getEnd() + " Level = " + t.getIntervalFld(k).getLevel());
//                    else
//                        System.out.print(" TagName = " + t.getStrFld(k) );
//                }
//                System.out.println("\n");
//
//
//            }
//        } catch(Exception e){
//            e.printStackTrace();
//        }
        int count = 0;
        boolean done = false;
        Tuple t = new Tuple();
        try{
            while(!done && result!=null){
                t = result.get_next();
                if(t == null){
                    done = true;
                    break;
                }
                count++;


                Heapfile f = new Heapfile("PAAAAAAAAAP");


                try {
                    f.insertRecord(t.returnTupleByteArray());
                } catch(Exception e) {
                    e.printStackTrace();
                }
                System.out.println("Result " + (count) + ":");
                System.out.print("\t");
                for(int k=1;k<=projInc;k++){
                    if(k%2 != 0)
                        System.out.print(" | Start = " + t.getIntervalFld(k).getStart() + " End = " + t.getIntervalFld(k).getEnd() + " Level = " + t.getIntervalFld(k).getLevel());
                    else
                        System.out.print(" TagName = " + t.getStrFld(k) + " |");
                }
                System.out.println();


            }
        } catch(Exception e){
            e.printStackTrace();
        }
        System.out.println("Count -  : " +count);

        System.out.println("Printing contents of file: ");
        ///----------------------------------------------------

        AttrType [] Stypes = new AttrType[projInc];
        for(int iter = 0; iter<projInc;iter=iter+2){
            Stypes[iter] = new AttrType (AttrType.attrInterval);
            Stypes[iter+1] = new AttrType (AttrType.attrString);
        }


        int sizeVal = projInc / 2;
        short [] Ssizes = new short[sizeVal];
        for(int iter = 0; iter<sizeVal;iter++){
            Ssizes[iter] = 10;
        }

        FldSpec [] outProjection1 = new FldSpec[projInc];
        for(int i=0; i<projInc; i++) {

            outProjection1[i] = new FldSpec(new RelSpec(RelSpec.outer),i+1);

        }

        FileScan sss = new FileScan("PAAAAAAAAAP", Stypes, Ssizes, (short)4,(short) 4, outProjection1, null);
        int iteasd = 0;
        boolean done1 = false;
        Tuple t1 = new Tuple();
        try{
            while(!done1 && scan!=null){
                t1 = sss.get_next();
                if(t1 == null){
                    done1 = true;
                    break;
                }
                iteasd++;
                byte[] tupleArray = t1.getTupleByteArray();

                for(int k=1;k<=projInc;k++){
                    if(k%2 != 0)
                        System.out.print(" | Start = " + t1.getIntervalFld(k).getStart() + " End = " + t1.getIntervalFld(k).getEnd() + " Level = " + t1.getIntervalFld(k).getLevel());
                    else
                        System.out.print(" TagName = " + t1.getStrFld(k) );
                }
                System.out.println("\n");


            }
        } catch(Exception e){
            e.printStackTrace();
        }


//        System.out.println("Count of records: "+iteasd);


    }
    public CustomNestedLoopsJoins processIntervalQuery1(ArrayList<ArrayList<String>> rule, ArrayList<String> tags){

        // Creating conditional expression for the first rule

        System.out.println(rule);
        System.out.println(tags);
        // check if first rule has *
        int firstIndex = Integer.parseInt(rule.get(0).get(0));
        int secondIndex = Integer.parseInt(rule.get(0).get(1));
        String initRule = rule.get(0).get(2);

        CondExpr[] exprInit = null;
        boolean leftStar = false;
        boolean rightStar = false;


        // Initialize variables for FileScan

        AttrType[] Stypes = new AttrType[2];
        Stypes[0] = new AttrType(AttrType.attrInterval);
        Stypes[1] = new AttrType(AttrType.attrString);


        short[] Ssizes = new short[1];
        Ssizes[0] = 10; //first elt. is 30

        FldSpec[] fileprojlist = new FldSpec[2];
        fileprojlist[0] = new FldSpec(new RelSpec(RelSpec.outer), 1);
        fileprojlist[1] = new FldSpec(new RelSpec(RelSpec.outer), 2);

        // Right Filter for FileScan

        CondExpr[] rightFilter =new CondExpr[2];

        rightFilter[0] = new CondExpr();
        rightFilter[0].op    = new AttrOperator(AttrOperator.aopEQ);
        rightFilter[0].type1 = new AttrType(AttrType.attrSymbol);
        rightFilter[0].type2 = new AttrType(AttrType.attrString);
        rightFilter[0].operand1.symbol = new FldSpec(new RelSpec(RelSpec.outer),2);
        rightFilter[0].operand2.string = tags.get(firstIndex-1);
        rightFilter[1] = null;

        if(tags.get(firstIndex-1).equals("*")){
            rightFilter = null;
        }


        FileScan fscan = null;

        try{
            fscan = new FileScan("test.in", Stypes, Ssizes,
                    (short)2, (short)2,
                    fileprojlist, rightFilter);
        }catch (Exception e){
            System.err.println(e);
        }



        // Setup index scan for all the rules

        //Set up CondExpr for A B AD using
        CondExpr[] select = new CondExpr[2];

        select[0] = new CondExpr();
        select[0].op = new AttrOperator(AttrOperator.aopEQ);
        select[0].type1 = new AttrType(AttrType.attrSymbol);
        select[0].type2 = new AttrType(AttrType.attrSymbol);
        select[0].operand1.symbol = new FldSpec(new RelSpec(RelSpec.outer), 1);
        select[0].operand2.symbol = new FldSpec(new RelSpec(RelSpec.innerRel), 1);
        select[0].flag = 1;
        select[0].next = select[1];

        select[1] = null;

        FldSpec[] projlist = new FldSpec[2];
        projlist[0] = new FldSpec(new RelSpec(RelSpec.outer), 1);
        projlist[1] = new FldSpec(new RelSpec(RelSpec.outer), 2);


        select = null;
        IndexScan iscan = null;
        try {
            iscan = new IndexScan(new IndexType(IndexType.I_Index), "test.in", "IntervalTreeIndex", Stypes, Ssizes, 2, 2, projlist, select, 1, false);
        } catch (Exception e) {

            e.printStackTrace();
        }


        // Setup Custom NestedLoopJoin for the first rule


        CondExpr[] rightFilternljInit =new CondExpr[2];

        rightFilternljInit[0] = new CondExpr();
        rightFilternljInit[0].op    = new AttrOperator(AttrOperator.aopEQ);
        rightFilternljInit[0].type1 = new AttrType(AttrType.attrSymbol);
        rightFilternljInit[0].type2 = new AttrType(AttrType.attrString);
        rightFilternljInit[0].operand1.symbol = new FldSpec(new RelSpec(RelSpec.outer),2);
        rightFilternljInit[0].operand2.string = tags.get(secondIndex-1);
        rightFilternljInit[1] = null;

        if(tags.get(secondIndex-1).equals("*")){
            rightFilternljInit = null;
        }

        FldSpec[] nljInitProjlist = new FldSpec[4];
        nljInitProjlist[0] = new FldSpec(new RelSpec(RelSpec.outer), 1);
        nljInitProjlist[1] = new FldSpec(new RelSpec(RelSpec.outer), 2);
        nljInitProjlist[2] = new FldSpec(new RelSpec(RelSpec.innerRel), 1);
        nljInitProjlist[3] = new FldSpec(new RelSpec(RelSpec.innerRel), 2);


        CustomNestedLoopsJoins nljInitial =null;
        int initCond = 0;
        if(initRule.equals("AD")){
            initCond = 1;
        }
        else{
            initCond = 2;
        }


        try {
            nljInitial = new CustomNestedLoopsJoins(Stypes, 2, Ssizes, Stypes,2,Ssizes,10,fscan,
                    iscan,select,rightFilternljInit,nljInitProjlist,4 ,1, initCond);
        }
        catch (Exception e) {
            System.err.println("*** join error in NestedLoop1 constructor ***");
//            status = FAIL;
            System.err.println (""+e);
            e.printStackTrace();
        }

        Map<String,Integer> tagFieldMap = new HashMap<>();

        tagFieldMap.put(rule.get(0).get(0),1);
        tagFieldMap.put(rule.get(0).get(1),3);

        boolean GT = true;
        CustomNestedLoopsJoins prev = nljInitial;


        for(int i =1;i<rule.size();i++){

            int condition = 0;

            CondExpr[] rightFilternlj = null;

            int first = Integer.parseInt(rule.get(i).get(0));
            int second = Integer.parseInt(rule.get(i).get(1));
            String currRule = rule.get(i).get(2);

            rightFilternlj = new CondExpr[2];
            rightFilternlj[0] = new CondExpr();
            rightFilternlj[1] = new CondExpr();


            rightFilternlj[0] = new CondExpr();
//            rightFilternlj[0].op    = new AttrOperator(AttrOperator.aopEQ);
//            rightFilternlj[0].type1 = new AttrType(AttrType.attrSymbol);
//            rightFilternlj[0].type2 = new AttrType(AttrType.attrString);
//            rightFilternlj[0].operand1.symbol = new FldSpec(new RelSpec(RelSpec.outer),2);
//            rightFilternlj[0].operand2.string = tags.get(second-1);
//            rightFilternlj[1] = null;



            int joinColumn = 0;
            if(tagFieldMap.containsKey(Integer.toString(first))){
                GT = true;
                joinColumn = tagFieldMap.get(Integer.toString(first));
                rightFilternlj[0].op    = new AttrOperator(AttrOperator.aopEQ);
                rightFilternlj[0].type1 = new AttrType(AttrType.attrSymbol);
                rightFilternlj[0].type2 = new AttrType(AttrType.attrString);
                rightFilternlj[0].operand1.symbol = new FldSpec(new RelSpec(RelSpec.outer),2);
                rightFilternlj[0].operand2.string = tags.get(second-1);
                rightFilternlj[1] = null;
                tagFieldMap.put(Integer.toString(second),projInc+1);
            }else if(tagFieldMap.containsKey(Integer.toString(second))){
                GT = false;
                joinColumn = tagFieldMap.get(Integer.toString(second));
                rightFilternlj[0].op    = new AttrOperator(AttrOperator.aopEQ);
                rightFilternlj[0].type1 = new AttrType(AttrType.attrSymbol);
                rightFilternlj[0].type2 = new AttrType(AttrType.attrString);
                rightFilternlj[0].operand1.symbol = new FldSpec(new RelSpec(RelSpec.outer),2);
                rightFilternlj[0].operand2.string = tags.get(first-1);
                rightFilternlj[1] = null;
                tagFieldMap.put(Integer.toString(first),projInc+1);
            }

            if(tags.get(second-1).equals("*")){
                rightFilternlj = null;
            }

            System.out.println("Current Rule"+currRule);
            if(currRule.equals("AD")){
                condition = 1;
                if(GT){
//                    expr[0].op    = new AttrOperator(AttrOperator.aopGT);
                    condition = 1;
                }
                else{
//                    expr[0].op    = new AttrOperator(AttrOperator.aopLT);
                    condition = 1;
                }

            }else if(currRule.equals("PC")){
                condition = 2;
                if(GT){
//                    expr[0].op    = new AttrOperator(AttrOperator.aopPC);
                    condition = 2;
                }
                else{
//                    expr[0].op    = new AttrOperator(AttrOperator.aopCP);
                    condition = 2;
                }

            }

            tagFieldMap.put(rule.get(0).get(0),1);


            AttrType [] LeftTtypes = new AttrType[projInc];
            for(int iter = 0; iter<projInc;iter=iter+2){
                LeftTtypes[iter] = new AttrType (AttrType.attrInterval);
                LeftTtypes[iter+1] = new AttrType (AttrType.attrString);
            }

            int leftSizeVal = i+1;
            short [] leftSizes = new short[leftSizeVal];
            for(int iter = 0; iter<leftSizeVal;iter++){
                leftSizes[iter] = 10;
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

            if(rightFilter==null){
                System.out.println("Right filters is null");
            }
            CustomNestedLoopsJoins nljLoop =null;
            try {
                nljLoop = new CustomNestedLoopsJoins(LeftTtypes, projInc, leftSizes, Stypes,2,Ssizes,10,prev,
                        iscan,select,rightFilternlj,outProjection,projInc+2, joinColumn, condition);
            }
            catch (Exception e) {
                System.err.println("*** join error in NestedLoop1 constructor ***");
                System.err.println (""+e);
                e.printStackTrace();
            }

            projInc = projInc+2;
            prev = nljLoop;

            if(i==rule.size()-1){
                return nljLoop;
            }

        }

        return nljInitial;


    }


    public static void main(String argv[]) throws ParserConfigurationException, IOException, SAXException, TupleUtilsException, FileScanException, InvalidRelation {

        DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();
        dbf.setValidating(false);
        DocumentBuilder db = dbf.newDocumentBuilder();
//        String inputFilePath = argv[0];
//        Document doc = db.parse(new FileInputStream(new File(inputFilePath)));//"/home/user/DBMSI/Group8Phase2/sourcecode/javaminibase/src/xmldbTestXML/sample_data2.xml")));
        //String patternTreePath="/home/user/DBMSI/Group8Phase2/sourcecode/javaminibase/src/xmldbTestXML/XMLQueryInput.txt";
//        String patternTreePath = argv[1];

        Document doc = db.parse(new FileInputStream("/home/user/Desktop/AfterChanges/dbmsiPhase2/javaminibase/src/xmldbTestXML/sample_data.xml"));
        String patternTreePath = "/home/user/Desktop/AfterChanges/dbmsiPhase2/javaminibase/src/xmldbTestXML/complextestQuery";

        Node root = doc.getDocumentElement();

        xmlParser.build(root);
        xmlParser.preOrder(xmlParser.tree.root);
        xmlParser.BFSSetLevel();

        PCounter.initialize();
        Generalize  xmlinsert = new Generalize();
        xmlinsert.parseComplexPT(patternTreePath);

        System.out.println("Read Counter = " + PCounter.rcounter);
        System.out.println("Write Counter = " + PCounter.wcounter);
        System.out.println("Total = " + (PCounter.rcounter + PCounter.wcounter));
    }
}


