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


class IndexIntervalXY implements GlobalConst {

    static int projInc = 4;
    private boolean OK = true;
    private boolean FAIL = false;
    private Vector xmlTuples;
    static XMLParser xmlParser = new XMLParser();
    IndexType b_index;
    IndexType i_index;

    /** Constructor
     */
    public IndexIntervalXY() {

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

    public void parseComplexPT(String path) throws IOException, InvalidRelation, FileScanException, TupleUtilsException {


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



        processIntervalQuery1();


    }
    public void processIntervalQuery1() throws InvalidRelation, TupleUtilsException, FileScanException, IOException {


        AttrType[] Stypes = new AttrType[2];
        Stypes[0] = new AttrType(AttrType.attrInterval);
        Stypes[1] = new AttrType(AttrType.attrString);


        short[] Ssizes = new short[1];
        Ssizes[0] = 10; //first elt. is 30

        FldSpec[] projlist = new FldSpec[2];
        projlist[0] = new FldSpec(new RelSpec(RelSpec.outer), 1);
        projlist[1] = new FldSpec(new RelSpec(RelSpec.outer), 2);

        FldSpec[] fileprojlist = new FldSpec[2];
        fileprojlist[0] = new FldSpec(new RelSpec(RelSpec.outer), 1);
        fileprojlist[1] = new FldSpec(new RelSpec(RelSpec.outer), 2);

        AttrType[] ResultTypes = new AttrType[4];
        ResultTypes[0] = new AttrType(AttrType.attrInterval);
        ResultTypes[1] = new AttrType(AttrType.attrString);
        ResultTypes[2] = new AttrType(AttrType.attrInterval);
        ResultTypes[3] = new AttrType(AttrType.attrString);

        short[] resultSizes = new short[2];
        resultSizes[0] = 10;
        resultSizes[1] = 10;

        FldSpec[] resultProjlist = new FldSpec[4];
        resultProjlist[0] = new FldSpec(new RelSpec(RelSpec.outer), 1);
        resultProjlist[1] = new FldSpec(new RelSpec(RelSpec.outer), 2);
        resultProjlist[2] = new FldSpec(new RelSpec(RelSpec.innerRel), 1);
        resultProjlist[3] = new FldSpec(new RelSpec(RelSpec.innerRel), 2);

//        int attrOperator;
//        if("PC".equals(op)) {
//            attrOperator = AttrOperator.aopCP;
//        } else {
//            attrOperator = AttrOperator.aopAD;
//        }
//

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

        // File Scan

        CondExpr[] rightFilter =new CondExpr[2];

        rightFilter[0] = new CondExpr();
        rightFilter[0].op    = new AttrOperator(AttrOperator.aopEQ);
        rightFilter[0].type1 = new AttrType(AttrType.attrSymbol);
        rightFilter[0].type2 = new AttrType(AttrType.attrString);
        rightFilter[0].operand1.symbol = new FldSpec(new RelSpec(RelSpec.outer),2);
        rightFilter[0].operand2.string = "Entry";
        rightFilter[1] = null;


        CondExpr[] rightFilternlj =new CondExpr[2];

        rightFilternlj[0] = new CondExpr();
        rightFilternlj[0].op    = new AttrOperator(AttrOperator.aopEQ);
        rightFilternlj[0].type1 = new AttrType(AttrType.attrSymbol);
        rightFilternlj[0].type2 = new AttrType(AttrType.attrString);
        rightFilternlj[0].operand1.symbol = new FldSpec(new RelSpec(RelSpec.outer),2);
        rightFilternlj[0].operand2.string = "Ref";
        rightFilternlj[1] = null;

        FileScan am = null;


        try {

            am  = new FileScan("test.in", Stypes, Ssizes,
                    (short)2, (short)2,
                    fileprojlist, rightFilter);
        }
        catch (Exception e) {
            e.printStackTrace();
        }


        // start index scan

        select = null;
        IndexScan iscan = null;
        try {
            iscan = new IndexScan(new IndexType(IndexType.I_Index), "test.in", "IntervalTreeIndex", Stypes, Ssizes, 2, 2, projlist, select, 1, false);
        } catch (Exception e) {

            e.printStackTrace();
        }

        CustomNestedLoopsJoins result =null;
        try {
            result = new CustomNestedLoopsJoins(Stypes, 2, Ssizes, Stypes,2,Ssizes,10,am,
                    iscan,select,rightFilternlj,resultProjlist,4 ,1,1);
        }
        catch (Exception e) {
            System.err.println("*** join error in NestedLoop1 constructor ***");
//            status = FAIL;
            System.err.println (""+e);
            e.printStackTrace();
        }

        AttrType[] Stypes2 = new AttrType[4];
        Stypes2[0] = new AttrType(AttrType.attrInterval);
        Stypes2[1] = new AttrType(AttrType.attrString);
        Stypes2[2] = new AttrType(AttrType.attrInterval);
        Stypes2[3] = new AttrType(AttrType.attrString);


        short[] Ssizes2 = new short[2];
        Ssizes2[0] = 10; //first elt. is 30
        Ssizes2[1] = 10; //first elt. is 30


        FldSpec[] resultProjlist2 = new FldSpec[6];
        resultProjlist2[0] = new FldSpec(new RelSpec(RelSpec.outer), 1);
        resultProjlist2[1] = new FldSpec(new RelSpec(RelSpec.outer), 2);
        resultProjlist2[2] = new FldSpec(new RelSpec(RelSpec.outer), 3);
        resultProjlist2[3] = new FldSpec(new RelSpec(RelSpec.outer), 4);
        resultProjlist2[4] = new FldSpec(new RelSpec(RelSpec.innerRel), 1);
        resultProjlist2[5] = new FldSpec(new RelSpec(RelSpec.innerRel), 2);


//        CondExpr[] rightFilternlj2 =new CondExpr[2];
//
//        rightFilternlj[0] = new CondExpr();
//        rightFilternlj[0].op    = new AttrOperator(AttrOperator.aopEQ);
//        rightFilternlj[0].type1 = new AttrType(AttrType.attrSymbol);
//        rightFilternlj[0].type2 = new AttrType(AttrType.attrString);
//        rightFilternlj[0].operand1.symbol = new FldSpec(new RelSpec(RelSpec.outer),2);
//        rightFilternlj[0].operand2.string = "Autho";
//        rightFilternlj[1] = null;




        CustomNestedLoopsJoins result2 =null;
        select = null;
        try {
            result2 = new CustomNestedLoopsJoins(Stypes2, 4, Ssizes2, Stypes,2,Ssizes,10,result,
                    iscan,select,null,resultProjlist2,6 , 3,2);
        }
        catch (Exception e) {
            System.err.println("*** join error in NestedLoop1 constructor ***");
//            status = FAIL;
            System.err.println (""+e);
            e.printStackTrace();
        }






//        int iteasd = 0;
//        boolean done = false;
//        Tuple t = new Tuple();
//        try{
//            while(!done && result2!=null){
//                t = result2.get_next();
//                if(t == null){
//                    done = true;
//                    break;
//                }
//                iteasd++;
//                byte[] tupleArray = t.getTupleByteArray();
//
//                for(int k=1;k<=6;k++){
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
//
//
//        System.out.println("Count of records: "+iteasd);

        int count = 0;
        boolean done = false;
        Tuple t = new Tuple();
        projInc =6;
        try{
            while(!done && result2!=null){
                t = result2.get_next();
                if(t == null){
                    done = true;
                    break;
                }
                count++;


                Heapfile f = new Heapfile("zero");


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

//        AttrType [] Stypes = new AttrType[projInc];
//        for(int iter = 0; iter<projInc;iter=iter+2){
//            Stypes[iter] = new AttrType (AttrType.attrInterval);
//            Stypes[iter+1] = new AttrType (AttrType.attrString);
//        }


//        int sizeVal = projInc / 2;
//        short [] Ssizes = new short[sizeVal];
//        for(int iter = 0; iter<sizeVal;iter++){
//            Ssizes[iter] = 5;
//        }

        FldSpec [] outProjection1 = new FldSpec[projInc];
        for(int i=0; i<projInc; i++) {

            outProjection1[i] = new FldSpec(new RelSpec(RelSpec.outer),i+1);

        }

        FileScan sss = new FileScan("zero", Stypes, Ssizes, (short)2,(short) 2, outProjection1, null);
        int iteasd = 0;
        boolean done1 = false;
        Tuple t1 = new Tuple();
        try{
            while(!done1 && sss!=null){
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


//        System.out.println("Crappy display");
//
//        IntervalType intTest = new IntervalType(6, 11, 1);
//
//
//        select = new CondExpr[3];
//
//        select[0] = new CondExpr();
//        select[0].op = new AttrOperator(AttrOperator.aopAD);
//        select[0].type1 = new AttrType(AttrType.attrSymbol);
//        select[0].type2 = new AttrType(AttrType.attrInterval);
//        select[0].operand1.symbol = new FldSpec(new RelSpec(RelSpec.outer), 1);
//        select[0].operand2.interval = intTest;
//        select[0].flag = 1;
//        select[0].next = select[1];
//
//        select[1] = null;
//        select[2] = null;
//
//
//
//
////        select = null;
//        IndexScan iscan1 = null;
//        try {
//            iscan1 = new IndexScan(new IndexType(IndexType.I_Index), "test.in", "IntervalTreeIndex", Stypes, Ssizes, 2, 2, projlist, select, 1, false);
//        } catch (Exception e) {
//
//            e.printStackTrace();
//        }
//
//
////        int iteasd = 0;
//        done = false;
//
//        t = new Tuple();
//        try{
//            while(!done && iscan1!=null){
//                t = iscan1.get_next();
//                if(t == null){
//                    done = true;
//                    break;
//                }
//                iteasd++;
//                byte[] tupleArray = t.getTupleByteArray();
//
//                for(int k=1;k<=2;k++){
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
//
//
//












        return;
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
        IndexIntervalXY  xmlinsert = new IndexIntervalXY();
        xmlinsert.parseComplexPT(patternTreePath);

        System.out.println("Read Counter = " + PCounter.rcounter);
        System.out.println("Write Counter = " + PCounter.wcounter);
        System.out.println("Total = " + (PCounter.rcounter + PCounter.wcounter));
    }
}


