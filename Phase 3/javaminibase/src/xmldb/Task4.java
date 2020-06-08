package xmldb;


import btree.BTreeFile;
import intervaltree.IntervalTreeFile;
import iterator.*;
import heap.*;
import global.*;
import java.io.*;
import java.util.*;
import java.lang.*;
import org.w3c.dom.Document;
import org.w3c.dom.Node;
import org.xml.sax.SAXException;

import diskmgr.PCounter;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;

import static tests.TestDriver.FAIL;
import static tests.TestDriver.OK;

@SuppressWarnings("Duplicates")


class Task4 implements GlobalConst {

    static int projInc = 4;
    private boolean OK = true;
    private boolean FAIL = false;
    private Vector xmlTuples;
    static XMLParser xmlParser = new XMLParser();

    /** Constructor
     */
    public Task4() {

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

        // creating the relation
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

    public void parseComplexPT(String path) throws FileNotFoundException {


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
//        ArrayList<ArrayList<String>> reverseSortedRules = XMLQueryParsing.getReverseSortedRules(reversedtags, reversedRules);


//        ArrayList<ArrayList<String>> sortedRuleswithTag = new ArrayList<>();
//        for(int i=0; i< sortedRules.size();i++){
//            ArrayList<String> temp = new ArrayList<>();
//            temp.add(tags.get(Integer.parseInt(sortedRules.get(i).get(0))-1));
//            temp.add(tags.get(Integer.parseInt(sortedRules.get(i).get(1))-1));
//            temp.add(sortedRules.get(i).get(2));
//
//            sortedRuleswithTag.add(temp);
//
//        }

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





        // Final rules are in sortedRuleswithTag

//         Perform complex query processing
        try{
            switch(operation[0]){
                case "SEL":
                    scanRelation(sortedRules,tags);
                    break;
                case "CP":
                    cartesianProductJoin(sortedRules,tags,sortedRules2,tags2);
                    break;
                case "TJ":
                    tagJoin(sortedRules,tags,sortedRules2,tags2,Integer.parseInt(operation[1]),Integer.parseInt(operation[2]));
                    break;
                case "NJ":
                    nodeJoin(sortedRules,tags,sortedRules2,tags2,Integer.parseInt(operation[1]),Integer.parseInt(operation[2]));
                    break;
                case "SRT":
                    tagSort(sortedRules,tags, Integer.parseInt(operation[1])*2);
                    break;
                case "GRP":
                    tagGroup(sortedRules,tags,Integer.parseInt(operation[1])*2);
                    break;
            }
        }catch(Exception e){
            System.err.println(e);
        }
    }

    public void scanRelation(ArrayList<ArrayList<String>> sortedRules,ArrayList<String> tags)
            throws InvalidRelation, TupleUtilsException, FileScanException, IOException, NestedLoopException{
        // Scan results returned by NLJ

        NestedLoopsJoins result = processQuery(sortedRules, tags);

//        Display the results

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
                try{

                    Map<Integer,OutputNode> map = new HashMap<>();

                    OutputNode [] node = new OutputNode[projInc/2];
                    int k = 0;
                    for(int i=1;i<=projInc;i+=2){
                        node[k] = new OutputNode(t.getIntervalFld(i),t.getStrFld(i+1),i);
                        map.put(t.getIntervalFld(i).getStart(),node[k]);
                        k++;
                    }

                    TreeMap<Integer, OutputNode> treeMap = new TreeMap<>(map);
                    treeMap.putAll(map);

                    boolean init = true;
                    OutputClass root = null;
                    Stack<OutputClass> stack = new Stack<>();

                    for (Map.Entry<Integer, OutputNode> entry : treeMap.entrySet()){

//                    Stack<OutputClass> temp ;
                        if(init){
                            root = new OutputClass(entry.getValue());
                            init = false;
                            stack.push(root);
//                        temp.push(root);
                            continue;
                        }

                        OutputNode curr = entry.getValue();
                        Stack<OutputClass> temp = new Stack<>();
                        temp.addAll(stack);

                        while (!temp.isEmpty()){
                            OutputClass parent = temp.pop(); // root
                            if(curr.isChild(parent.getNode())){
                                curr.setSpace(parent.getNode().getSpace()+2);
                                OutputClass newChild = new OutputClass(curr);
                                parent.setChild(newChild);
                                stack.push(newChild);
                                break;
                            }
                            else{
                                continue;
                            }
                        }
                    }

                    // Printing Output

                    Stack<OutputClass> printStack = new Stack<>();
                    printStack.push(root);
                    while(!printStack.isEmpty()){
                        OutputClass entry = printStack.pop();
                        OutputNode entryNode = entry.getNode();
                        for(int s=0;s<entryNode.getSpace();s++)
                            System.out.print(" ");

                        System.out.print("--" +entryNode.getTagName() + " [" + entryNode.getStartValue() + ", "+ entryNode.getEndValue() + "]") ;
                        System.out.println();
                        if(entry.children != null){
                            for(int i =0;i<entry.children.size();i++)
                                printStack.push(entry.children.get(i));
                        }
                    }
                }
                catch(Exception e){
                    e.printStackTrace();
                }


            }
        } catch(Exception e){
            e.printStackTrace();
        }

        System.out.println("\nRecords  returned by File Scan: " + count);


    }

    public void cartesianProductJoin(ArrayList<ArrayList<String>> sortedRules,ArrayList<String> tags, ArrayList<ArrayList<String>> sortedRules2,ArrayList<String> tags2)
            throws InvalidRelation, TupleUtilsException, FileScanException, IOException, NestedLoopException {

        System.out.println(sortedRules);
        System.out.println(sortedRules2);

        NestedLoopsJoins result1 = processQuery(sortedRules, tags);
        int projInc1 = projInc;

        // Materialize result1
        materialize(result1,projInc1,"result1");
        projInc = 4;
        NestedLoopsJoins result2 = processQuery(sortedRules2, tags2);

        // Materialize result2
        int projInc2 = projInc;
        materialize(result2,projInc2,"result2");


        AttrType [] Stypes = new AttrType[projInc1];
        for(int iter = 0; iter<projInc1;iter=iter+2){
            Stypes[iter] = new AttrType (AttrType.attrInterval);
            Stypes[iter+1] = new AttrType (AttrType.attrString);
        }

        AttrType [] Rtypes = new AttrType[projInc2];
        for(int iter = 0; iter<projInc2;iter=iter+2){
            Rtypes[iter] = new AttrType (AttrType.attrInterval);
            Rtypes[iter+1] = new AttrType (AttrType.attrString);
        }

        int sizeVal = projInc1 / 2;
        short [] Ssizes = new short[sizeVal];
        for(int iter = 0; iter<sizeVal;iter++){
            Ssizes[iter] = 5;
        }

        int sizeVal2 = projInc2 / 2;
        short [] Rsizes = new short[sizeVal2];
        for(int iter = 0; iter<sizeVal2;iter++){
            Rsizes[iter] = 5;
        }

        FldSpec [] outProjection1 = new FldSpec[projInc1];
        for(int i=0; i<projInc1; i++) {

            outProjection1[i] = new FldSpec(new RelSpec(RelSpec.outer),i+1);

        }

        int totalProjInc = projInc1 + projInc2;

        FldSpec [] outProjection = new FldSpec[totalProjInc];
        for(int i=0; i<projInc1; i++) {
            outProjection[i] = new FldSpec(new RelSpec(RelSpec.outer),i+1);
        }
        for(int i=0; i<projInc2; i++) {

            outProjection[projInc1 + i] = new FldSpec(new RelSpec(RelSpec.innerRel),i+1);

        }

        /*
         * Set up FileScan for one relation
         */
        FileScan scan = new FileScan("result1", Stypes, Ssizes, (short)projInc1,(short) projInc1, outProjection1, null);


        /*
         * Set up NLJ
         */
        NestedLoopsJoins nlj = new NestedLoopsJoins(Stypes, projInc1, Ssizes, Rtypes, projInc2, Rsizes, 10, scan, "result2", null, null, outProjection, totalProjInc);


        System.out.println("\n\nCartesian Product Results:");
        int count = -1;
        boolean done = false;
        try {

            while(!done) {
                count++;
                Tuple t = nlj.get_next();
                if(t == null) {
                    done = true;
                    break;
                }


                // Store the left side results
                Map<Integer,OutputNode> map1 = new HashMap<>();

                OutputNode [] node1 = new OutputNode[projInc1/2];
                int k = 0;
                for(int i=1;i<=projInc1;i+=2){
                    node1[k] = new OutputNode(t.getIntervalFld(i),t.getStrFld(i+1),1);
                    map1.put(t.getIntervalFld(i).getStart(),node1[k]);
                    k++;
                }

                TreeMap<Integer, OutputNode> treeMap1 = new TreeMap<>(map1);
                treeMap1.putAll(map1);

                boolean init = true;
                OutputClass root1 = null;
                Stack<OutputClass> stack1 = new Stack<>();

                for (Map.Entry<Integer, OutputNode> entry : treeMap1.entrySet()){

//                    Stack<OutputClass> temp ;
                    if(init){
                        root1 = new OutputClass(entry.getValue());
                        init = false;
                        stack1.push(root1);
//                        temp.push(root);
                        continue;
                    }

                    OutputNode curr = entry.getValue();
                    Stack<OutputClass> temp = new Stack<>();
                    temp.addAll(stack1);

                    while (!temp.isEmpty()){
                        OutputClass parent = temp.pop(); // root
                        if(curr.isChild(parent.getNode())){
                            curr.setSpace(parent.getNode().getSpace()+2);
                            OutputClass newChild = new OutputClass(curr);
                            parent.setChild(newChild);
                            stack1.push(newChild);
                            break;
                        }
                        else{
                            continue;
                        }
                    }
                }

                // Store right side results


                Map<Integer,OutputNode> map2 = new HashMap<>();

                OutputNode [] node2 = new OutputNode[projInc2/2];
                int p=0;
                for(int i=projInc1+1;i<=projInc2+projInc1;i+=2){
                    node2[p] = new OutputNode(t.getIntervalFld(i),t.getStrFld(i+1),1);
                    map2.put(t.getIntervalFld(i).getStart(),node2[p]);
                    p++;
                }

                TreeMap<Integer, OutputNode> treeMap2 = new TreeMap<>(map2);
                treeMap2.putAll(map2);

                boolean init2 = true;
                OutputClass root2 = null;
                Stack<OutputClass> stack2 = new Stack<>();

                for (Map.Entry<Integer, OutputNode> entry : treeMap2.entrySet()){

//                    Stack<OutputClass> temp ;
                    if(init2){
                        root2 = new OutputClass(entry.getValue());
                        init2 = false;
                        stack2.push(root2);
//                        temp.push(root);
                        continue;
                    }

                    OutputNode curr = entry.getValue();
                    Stack<OutputClass> temp = new Stack<>();
                    temp.addAll(stack2);

                    while (!temp.isEmpty()){
                        OutputClass parent = temp.pop(); // root
                        if(curr.isChild(parent.getNode())){
                            curr.setSpace(parent.getNode().getSpace()+2);
                            OutputClass newChild = new OutputClass(curr);
                            parent.setChild(newChild);
                            stack2.push(newChild);
                            break;
                        }
                        else{
                            continue;
                        }
                    }
                }

                // Printing Output of cartesian product

                Stack<OutputClass> printStack = new Stack<>();

                System.out.println("\n--CP_root");
                printStack.push(root1);

                // Print left subtree
                while(!printStack.isEmpty()){
                    OutputClass entry = printStack.pop();
                    OutputNode entryNode = entry.getNode();
                    for(int s=0;s<entryNode.getSpace();s++)
                        System.out.print(" ");

                    System.out.print("--" +entryNode.getTagName() + " [" + entryNode.getStartValue() + ", "+ entryNode.getEndValue() + "]") ;
                    System.out.println();
                    if(entry.children != null){
                        for(int i =0;i<entry.children.size();i++)
                            printStack.push(entry.children.get(i));
                    }
                }

                // Print right subtree
                printStack.push(root2);
                while(!printStack.isEmpty()){
                    OutputClass entry = printStack.pop();
                    OutputNode entryNode = entry.getNode();
                    for(int s=0;s<entryNode.getSpace();s++)
                        System.out.print(" ");

                    System.out.print("--" +entryNode.getTagName() + " [" + entryNode.getStartValue() + ", "+ entryNode.getEndValue() + "]") ;
                    System.out.println();
                    if(entry.children != null){
                        for(int i =0;i<entry.children.size();i++)
                            printStack.push(entry.children.get(i));
                    }
                }
            }
        }catch(Exception e) {
            e.printStackTrace();
        }

        System.out.println("Records returned by Cartesian Product = " + count);

    }

    public void materialize(NestedLoopsJoins nlj, int size, String filename){
        int count = 0;
        boolean done = false;
        Tuple t = new Tuple();
        try{
            while(!done && nlj!=null){
                t = nlj.get_next();
                if(t == null){
                    done = true;
                    break;
                }
                count++;


                Heapfile f = new Heapfile(filename);


                try {
                    f.insertRecord(t.returnTupleByteArray());
                } catch(Exception e) {
                    e.printStackTrace();
                }
                System.out.println("Result " + (count) + ":");
                System.out.print("\t");
                for(int k=1;k<=size;k++){
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
        System.out.println("Count - "+ filename + " : " +count);
    }

    public void tagJoin(ArrayList<ArrayList<String>> sortedRules,ArrayList<String> tags, ArrayList<ArrayList<String>> sortedRules2,ArrayList<String> tags2, int left, int right)
            throws InvalidRelation, TupleUtilsException, FileScanException, IOException, NestedLoopException {

        System.out.println(sortedRules);
        System.out.println(sortedRules2);

        NestedLoopsJoins result1 = processQuery(sortedRules, tags);
        int projInc1 = projInc;

        // Materialize result1
        materialize(result1,projInc1,"result1");
        projInc = 4;
        NestedLoopsJoins result2 = processQuery(sortedRules2, tags2);

        // Materialize result2
        int projInc2 = projInc;
        materialize(result2,projInc2,"result2");


        AttrType [] Stypes = new AttrType[projInc1];
        for(int iter = 0; iter<projInc1;iter=iter+2){
            Stypes[iter] = new AttrType (AttrType.attrInterval);
            Stypes[iter+1] = new AttrType (AttrType.attrString);
        }

        AttrType [] Rtypes = new AttrType[projInc2];
        for(int iter = 0; iter<projInc2;iter=iter+2){
            Rtypes[iter] = new AttrType (AttrType.attrInterval);
            Rtypes[iter+1] = new AttrType (AttrType.attrString);
        }

        int sizeVal = projInc1 / 2;
        short [] Ssizes = new short[sizeVal];
        for(int iter = 0; iter<sizeVal;iter++){
            Ssizes[iter] = 5;
        }

        int sizeVal2 = projInc2 / 2;
        short [] Rsizes = new short[sizeVal2];
        for(int iter = 0; iter<sizeVal2;iter++){
            Rsizes[iter] = 5;
        }

        FldSpec [] outProjection1 = new FldSpec[projInc1];
        for(int i=0; i<projInc1; i++) {

            outProjection1[i] = new FldSpec(new RelSpec(RelSpec.outer),i+1);

        }
        int totalProjInc = projInc1 + projInc2;

        FldSpec [] outProjection = new FldSpec[totalProjInc];
        for(int i=0; i<projInc1; i++) {
            outProjection[i] = new FldSpec(new RelSpec(RelSpec.outer),i+1);
        }
        for(int i=0; i<projInc2; i++) {

            outProjection[projInc1 + i] = new FldSpec(new RelSpec(RelSpec.innerRel),i+1);

        }
        // Set up conditional expression for tag join

        CondExpr[] expr = new CondExpr[2];
        expr[0] = new CondExpr();
        expr[0].op    = new AttrOperator(AttrOperator.aopEQ);
        expr[0].type1 = new AttrType(AttrType.attrSymbol);
        expr[0].type2 = new AttrType(AttrType.attrSymbol);
        expr[0].operand1.symbol = new FldSpec(new RelSpec(RelSpec.innerRel),left*2);
        expr[0].operand2.symbol = new FldSpec(new RelSpec(RelSpec.outer), right*2);
        expr[0].next  = null;
        expr[1] = null;

        /*
         * Set up FileScan for one relation
         */
        FileScan scan = new FileScan("result1", Stypes, Ssizes, (short)projInc1,(short) projInc1, outProjection1, null);


        /*
         * Set up NLJ
         */
        NestedLoopsJoins nlj = new NestedLoopsJoins(Stypes, projInc1, Ssizes, Rtypes, projInc2, Rsizes, 10, scan, "result2", expr, null, outProjection, totalProjInc);


        System.out.println("\n\nTag Join Results:");
        int count = -1;
        String current = null;
        boolean done = false;
        try {

            while(!done) {
                count++;
                Tuple t = nlj.get_next();
                if(t == null) {
                    done = true;
                    break;
                }


                // Store the left side results
                Map<Integer,OutputNode> map1 = new HashMap<>();

                OutputNode [] node1 = new OutputNode[projInc1/2];
                int k = 0;
                for(int i=1;i<=projInc1;i+=2){
                    node1[k] = new OutputNode(t.getIntervalFld(i),t.getStrFld(i+1),1);
                    map1.put(t.getIntervalFld(i).getStart(),node1[k]);
                    current = t.getStrFld(left*2);
                    k++;
                }

                TreeMap<Integer, OutputNode> treeMap1 = new TreeMap<>(map1);
                treeMap1.putAll(map1);

                boolean init = true;
                OutputClass root1 = null;
                Stack<OutputClass> stack1 = new Stack<>();

                for (Map.Entry<Integer, OutputNode> entry : treeMap1.entrySet()){

//                    Stack<OutputClass> temp ;
                    if(init){
                        root1 = new OutputClass(entry.getValue());
                        init = false;
                        stack1.push(root1);
//                        temp.push(root);
                        continue;
                    }

                    OutputNode curr = entry.getValue();
                    Stack<OutputClass> temp = new Stack<>();
                    temp.addAll(stack1);

                    while (!temp.isEmpty()){
                        OutputClass parent = temp.pop(); // root
                        if(curr.isChild(parent.getNode())){
                            curr.setSpace(parent.getNode().getSpace()+2);
                            OutputClass newChild = new OutputClass(curr);
                            parent.setChild(newChild);
                            stack1.push(newChild);
                            break;
                        }
                        else{
                            continue;
                        }
                    }
                }

                // Store right side results


                Map<Integer,OutputNode> map2 = new HashMap<>();

                OutputNode [] node2 = new OutputNode[projInc2/2];
                int p=0;
                for(int i=1;i<=projInc2;i+=2){
                    node2[p] = new OutputNode(t.getIntervalFld(i),t.getStrFld(i+1),1);
                    map2.put(t.getIntervalFld(i).getStart(),node2[p]);
                    p++;
                }

                TreeMap<Integer, OutputNode> treeMap2 = new TreeMap<>(map2);
                treeMap2.putAll(map2);

                boolean init2 = true;
                OutputClass root2 = null;
                Stack<OutputClass> stack2 = new Stack<>();

                for (Map.Entry<Integer, OutputNode> entry : treeMap2.entrySet()){

//                    Stack<OutputClass> temp ;
                    if(init2){
                        root2 = new OutputClass(entry.getValue());
                        init2 = false;
                        stack2.push(root2);
//                        temp.push(root);
                        continue;
                    }

                    OutputNode curr = entry.getValue();
                    Stack<OutputClass> temp = new Stack<>();
                    temp.addAll(stack2);

                    while (!temp.isEmpty()){
                        OutputClass parent = temp.pop(); // root
                        if(curr.isChild(parent.getNode())){
                            curr.setSpace(parent.getNode().getSpace()+2);
                            OutputClass newChild = new OutputClass(curr);
                            parent.setChild(newChild);
                            stack2.push(newChild);
                            break;
                        }
                        else{
                            continue;
                        }
                    }
                }

                // Printing Output of cartesian product

                Stack<OutputClass> printStack = new Stack<>();

                System.out.println("\n--TJ_root_"+current);
                printStack.push(root1);

                // Print left subtree
                while(!printStack.isEmpty()){
                    OutputClass entry = printStack.pop();
                    OutputNode entryNode = entry.getNode();
                    for(int s=0;s<entryNode.getSpace();s++)
                        System.out.print(" ");

                    System.out.print("--" +entryNode.getTagName() + " [" + entryNode.getStartValue() + ", "+ entryNode.getEndValue() + "]") ;
                    System.out.println();
                    if(entry.children != null){
                        for(int i =0;i<entry.children.size();i++)
                            printStack.push(entry.children.get(i));
                    }
                }

                // Print right subtree
                printStack.push(root2);
                while(!printStack.isEmpty()){
                    OutputClass entry = printStack.pop();
                    OutputNode entryNode = entry.getNode();
                    for(int s=0;s<entryNode.getSpace();s++)
                        System.out.print(" ");

                    System.out.print("--" +entryNode.getTagName() + " [" + entryNode.getStartValue() + ", "+ entryNode.getEndValue() + "]") ;
                    System.out.println();
                    if(entry.children != null){
                        for(int i =0;i<entry.children.size();i++)
                            printStack.push(entry.children.get(i));
                    }
                }
            }
        }catch(Exception e) {
            e.printStackTrace();
        }

        System.out.println("Records returned by Tag Join = " + count);

    }

    public void nodeJoin(ArrayList<ArrayList<String>> sortedRules,ArrayList<String> tags, ArrayList<ArrayList<String>> sortedRules2,ArrayList<String> tags2, int left, int right)
            throws InvalidRelation, TupleUtilsException, FileScanException, IOException, NestedLoopException {

        System.out.println(sortedRules);
        System.out.println(sortedRules2);

        NestedLoopsJoins result1 = processQuery(sortedRules, tags);
        int projInc1 = projInc;

        // Materialize result1
        materialize(result1,projInc1,"result1");
        projInc = 4;
        NestedLoopsJoins result2 = processQuery(sortedRules2, tags2);

        // Materialize result2
        int projInc2 = projInc;
        materialize(result2,projInc2,"result2");


        AttrType [] Stypes = new AttrType[projInc1];
        for(int iter = 0; iter<projInc1;iter=iter+2){
            Stypes[iter] = new AttrType (AttrType.attrInterval);
            Stypes[iter+1] = new AttrType (AttrType.attrString);
        }

        AttrType [] Rtypes = new AttrType[projInc2];
        for(int iter = 0; iter<projInc2;iter=iter+2){
            Rtypes[iter] = new AttrType (AttrType.attrInterval);
            Rtypes[iter+1] = new AttrType (AttrType.attrString);
        }

        int sizeVal = projInc1/2;
        short [] Ssizes = new short[sizeVal];
        for(int iter = 0; iter<sizeVal;iter++){
            Ssizes[iter] = 5;
        }

        int sizeVal2 = projInc2/2;
        short [] Rsizes = new short[sizeVal2];
        for(int iter = 0; iter<sizeVal2;iter++){
            Rsizes[iter] = 5;
        }

        FldSpec [] outProjection1 = new FldSpec[projInc1];
        for(int i=0; i<projInc1; i++) {

            outProjection1[i] = new FldSpec(new RelSpec(RelSpec.outer),i+1);

        }

        int totalProjInc = projInc1 + projInc2;

        FldSpec [] outProjection = new FldSpec[totalProjInc];
        for(int i=0; i<projInc1; i++) {
            outProjection[i] = new FldSpec(new RelSpec(RelSpec.outer),i+1);
        }
        for(int i=0; i<projInc2; i++) {

            outProjection[projInc1 + i] = new FldSpec(new RelSpec(RelSpec.innerRel),i+1);

        }


        // Set up conditional expression for tag join

        CondExpr[] expr = new CondExpr[2];
        expr[0] = new CondExpr();
        expr[0].op    = new AttrOperator(AttrOperator.aopEQ);
        expr[0].type1 = new AttrType(AttrType.attrSymbol);
        expr[0].type2 = new AttrType(AttrType.attrSymbol);
        expr[0].operand1.symbol = new FldSpec(new RelSpec(RelSpec.innerRel),(left*2)-1);
        expr[0].operand2.symbol = new FldSpec(new RelSpec(RelSpec.outer),(right*2)-1);
        expr[0].next  = null;
        expr[0].flag = 1;
        expr[1] = null;

//        CondExpr[] expr = new CondExpr[2];
//        expr[0] = new CondExpr();
//        expr[0].op    = new AttrOperator(AttrOperator.aopEQ);
//        expr[0].type1 = new AttrType(AttrType.attrSymbol);
//        expr[0].type2 = new AttrType(AttrType.attrSymbol);
//        expr[0].operand1.symbol = new FldSpec(new RelSpec(RelSpec.innerRel),leftAttrribute);
//        expr[0].operand2.symbol = new FldSpec(new RelSpec(RelSpec.outer),rightAttribute);
//        expr[0].next  = null;
//        expr[0].flag = 1;
//        expr[1] = null;

        /*
         * Set up FileScan for one relation
         */
        FileScan scan = new FileScan("result1", Stypes, Ssizes, (short)projInc1,(short) projInc1, outProjection1, null);


        /*
         * Set up NLJ
         */
        NestedLoopsJoins nlj = new NestedLoopsJoins(Stypes, projInc1, Ssizes, Rtypes, projInc2, Rsizes, 10, scan, "result2", expr, null, outProjection, totalProjInc);

        System.out.println("\n\nNode Join Results:");
        int count = -1;
        IntervalType current = null;
        boolean done = false;
        try {

            while(!done) {
                count++;
                Tuple t = nlj.get_next();
                if(t == null) {
                    done = true;
                    break;
                }


                // Store the left side results
                Map<Integer,OutputNode> map1 = new HashMap<>();

                OutputNode [] node1 = new OutputNode[projInc1/2];
                int k = 0;
                for(int i=1;i<=projInc1;i+=2){
                    node1[k] = new OutputNode(t.getIntervalFld(i),t.getStrFld(i+1),1);
                    map1.put(t.getIntervalFld(i).getStart(),node1[k]);
                    current = t.getIntervalFld((left*2)-1);
                    k++;
                }

                TreeMap<Integer, OutputNode> treeMap1 = new TreeMap<>(map1);
                treeMap1.putAll(map1);

                boolean init = true;
                OutputClass root1 = null;
                Stack<OutputClass> stack1 = new Stack<>();

                for (Map.Entry<Integer, OutputNode> entry : treeMap1.entrySet()){

//                    Stack<OutputClass> temp ;
                    if(init){
                        root1 = new OutputClass(entry.getValue());
                        init = false;
                        stack1.push(root1);
//                        temp.push(root);
                        continue;
                    }

                    OutputNode curr = entry.getValue();
                    Stack<OutputClass> temp = new Stack<>();
                    temp.addAll(stack1);

                    while (!temp.isEmpty()){
                        OutputClass parent = temp.pop(); // root
                        if(curr.isChild(parent.getNode())){
                            curr.setSpace(parent.getNode().getSpace()+2);
                            OutputClass newChild = new OutputClass(curr);
                            parent.setChild(newChild);
                            stack1.push(newChild);
                            break;
                        }
                        else{
                            continue;
                        }
                    }
                }

                // Store right side results


                Map<Integer,OutputNode> map2 = new HashMap<>();

                OutputNode [] node2 = new OutputNode[projInc2/2];
                int p=0;
                for(int i=1;i<=projInc2;i+=2){
                    node2[p] = new OutputNode(t.getIntervalFld(i),t.getStrFld(i+1),1);
                    map2.put(t.getIntervalFld(i).getStart(),node2[p]);
                    p++;
                }

                TreeMap<Integer, OutputNode> treeMap2 = new TreeMap<>(map2);
                treeMap2.putAll(map2);

                boolean init2 = true;
                OutputClass root2 = null;
                Stack<OutputClass> stack2 = new Stack<>();

                for (Map.Entry<Integer, OutputNode> entry : treeMap2.entrySet()){

//                    Stack<OutputClass> temp ;
                    if(init2){
                        root2 = new OutputClass(entry.getValue());
                        init2 = false;
                        stack2.push(root2);
//                        temp.push(root);
                        continue;
                    }

                    OutputNode curr = entry.getValue();
                    Stack<OutputClass> temp = new Stack<>();
                    temp.addAll(stack2);

                    while (!temp.isEmpty()){
                        OutputClass parent = temp.pop(); // root
                        if(curr.isChild(parent.getNode())){
                            curr.setSpace(parent.getNode().getSpace()+2);
                            OutputClass newChild = new OutputClass(curr);
                            parent.setChild(newChild);
                            stack2.push(newChild);
                            break;
                        }
                        else{
                            continue;
                        }
                    }
                }

                // Printing Output of cartesian product

                Stack<OutputClass> printStack = new Stack<>();

                System.out.println("\n--NJ_root_["+current.getStart()+","+current.getEnd()+"]");
                printStack.push(root1);

                // Print left subtree
                while(!printStack.isEmpty()){
                    OutputClass entry = printStack.pop();
                    OutputNode entryNode = entry.getNode();
                    for(int s=0;s<entryNode.getSpace();s++)
                        System.out.print(" ");

                    System.out.print("--" +entryNode.getTagName() + " [" + entryNode.getStartValue() + ", "+ entryNode.getEndValue() + "]") ;
                    System.out.println();
                    if(entry.children != null){
                        for(int i =0;i<entry.children.size();i++)
                            printStack.push(entry.children.get(i));
                    }
                }

                // Print right subtree
                printStack.push(root2);
                while(!printStack.isEmpty()){
                    OutputClass entry = printStack.pop();
                    OutputNode entryNode = entry.getNode();
                    for(int s=0;s<entryNode.getSpace();s++)
                        System.out.print(" ");

                    System.out.print("--" +entryNode.getTagName() + " [" + entryNode.getStartValue() + ", "+ entryNode.getEndValue() + "]") ;
                    System.out.println();
                    if(entry.children != null){
                        for(int i =0;i<entry.children.size();i++)
                            printStack.push(entry.children.get(i));
                    }
                }
            }
        }catch(Exception e) {
            e.printStackTrace();
        }

        System.out.println("Records returned by Node Join = " + count);

    }


    public void tagSort(ArrayList<ArrayList<String>> sortedRules,ArrayList<String> tags,int sortCol ){

        int count = 0;
        boolean done = false;
        Tuple t = new Tuple();
        boolean status = OK;
        NestedLoopsJoins result = processQuery(sortedRules, tags);

//        Display the results

//        int iteasd = 0;
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
//
//        System.out.println("\nRecords  returned by Nested Loop: " + iteasd);
//        int sortCol = 8;
        System.out.println(projInc);


        AttrType [] Stypes = new AttrType[projInc];
        for(int i=0;i<projInc;i+=2){
            Stypes[i] = new AttrType (AttrType.attrInterval);
            Stypes[i+1] = new AttrType (AttrType.attrString);
        }

        short [] Ssizes = new short[projInc/2];
        for(int i=0;i<projInc/2;i++){
            Ssizes[i] = 5;
        }

        FldSpec [] outProjectionSort = new FldSpec[projInc];
        for(int i=0;i<projInc;i++){
            outProjectionSort[i] = new FldSpec(new RelSpec(RelSpec.outer),i+1);
        }

        FldSpec[] projlist = new FldSpec[projInc];
        RelSpec rel = new RelSpec(RelSpec.outer);
        for(int i=0;i<projInc;i++){
            projlist[i] = new FldSpec(rel, i+1);
        }

        TupleOrder[] order = new TupleOrder[2];
        order[0] = new TupleOrder(TupleOrder.Ascending);
        order[1] = new TupleOrder(TupleOrder.Descending);

        FileScan fscan = null;

        // try {
//            fscan = new FileScan("sortTemp.in", Stypes, Ssizes, (short) 8, 8, projlist, null);
//        }
//        catch (Exception e) {
//            status = FAIL;
//            e.printStackTrace();
//        }

        Sort sort = null;
        try {
            sort = new Sort(Stypes, (short) projInc, Ssizes, result, sortCol, order[0], 5, 1000);
//            sort = new Sort(Stypes, (short) 8, Ssizes, result, 1, order[0], 5, 1000);
        }
        catch (Exception e) {
            status = FAIL;
            e.printStackTrace();
        }

        count = 0;
        t = null;
        String outval = null;

        try {
            t = sort.get_next();
        }
        catch (Exception e) {
            status = FAIL;
            e.printStackTrace();
        }

        boolean flag = true;

        // Display the sorted output in a tree order
        while (t != null){
            try{

                Map<Integer,OutputNode> map = new HashMap<>();

                OutputNode [] node = new OutputNode[projInc/2];
                int k = 0;
                for(int i=1;i<=projInc;i+=2){
                    node[k] = new OutputNode(t.getIntervalFld(i),t.getStrFld(i+1),i);
                    map.put(t.getIntervalFld(i).getStart(),node[k]);
                    k++;
                }

                TreeMap<Integer, OutputNode> treeMap = new TreeMap<>(map);
                treeMap.putAll(map);

                boolean init = true;
                OutputClass root = null;
                Stack<OutputClass> stack = new Stack<>();

                for (Map.Entry<Integer, OutputNode> entry : treeMap.entrySet()){

//                    Stack<OutputClass> temp ;
                    if(init){
                        root = new OutputClass(entry.getValue());
                        init = false;
                        stack.push(root);
//                        temp.push(root);
                        continue;
                    }

                    OutputNode curr = entry.getValue();
                    Stack<OutputClass> temp = new Stack<>();
                    temp.addAll(stack);

                    while (!temp.isEmpty()){
                        OutputClass parent = temp.pop(); // root
                        if(curr.isChild(parent.getNode())){
                            curr.setSpace(parent.getNode().getSpace()+2);
                            OutputClass newChild = new OutputClass(curr);
                            parent.setChild(newChild);
                            stack.push(newChild);
                            break;
                        }
                        else{
                            continue;
                        }
                    }
                }

                // Printing Output

                Stack<OutputClass> printStack = new Stack<>();
                printStack.push(root);
                while(!printStack.isEmpty()){
                    OutputClass entry = printStack.pop();
                    OutputNode entryNode = entry.getNode();
                    for(int s=0;s<entryNode.getSpace();s++)
                        System.out.print(" ");

                    System.out.print("--" +entryNode.getTagName() + " [" + entryNode.getStartValue() + ", "+ entryNode.getEndValue() + "]") ;
                    System.out.println();
                    if(entry.children != null){
                        for(int i =0;i<entry.children.size();i++)
                            printStack.push(entry.children.get(i));
                    }
                }
            }
            catch(Exception e){
                e.printStackTrace();
            }
            count++;
            try {
                t = sort.get_next();
            }
            catch (Exception e) {
                status = FAIL;
                e.printStackTrace();
            }
            System.out.println();
        }
    }

    public void tagGroup(ArrayList<ArrayList<String>> sortedRules,ArrayList<String> tagMap, int groupCol){

        int count = 0;
        boolean done = false;
        Tuple t = new Tuple();
        boolean status = OK;
        NestedLoopsJoins result = processQuery(sortedRules, tagMap);
//        int groupCol = 8;

        AttrType [] Stypes = new AttrType[projInc];
        for(int i=0;i<projInc;i+=2){
            Stypes[i] = new AttrType (AttrType.attrInterval);
            Stypes[i+1] = new AttrType (AttrType.attrString);
        }

        short [] Ssizes = new short[projInc/2];
        for(int i=0;i<projInc/2;i++){
            Ssizes[i] = 5;
        }

        FldSpec [] outProjectionSort = new FldSpec[projInc];
        for(int i=0;i<projInc;i++){
            outProjectionSort[i] = new FldSpec(new RelSpec(RelSpec.outer),i+1);
        }

        FldSpec[] projlist = new FldSpec[projInc];
        RelSpec rel = new RelSpec(RelSpec.outer);
        for(int i=0;i<projInc;i++){
            projlist[i] = new FldSpec(rel, i+1);
        }

        TupleOrder[] order = new TupleOrder[2];
        order[0] = new TupleOrder(TupleOrder.Ascending);
        order[1] = new TupleOrder(TupleOrder.Descending);

        FileScan fscan = null;

        // try {
//            fscan = new FileScan("sortTemp.in", Stypes, Ssizes, (short) 8, 8, projlist, null);
//        }
//        catch (Exception e) {
//            status = FAIL;
//            e.printStackTrace();
//        }

        Sort sort = null;
        try {
            sort = new Sort(Stypes, (short) projInc, Ssizes, result, groupCol, order[0], 5, 1000);
        }
        catch (Exception e) {
            status = FAIL;
            e.printStackTrace();
        }

        count = 0;
        t = null;
        String outval = null;

        try {
            t = sort.get_next();
        }
        catch (Exception e) {
            status = FAIL;
            e.printStackTrace();
        }

        boolean flag = true;
        int currGroup = 1;
        String groupElem = null;
        String current = null;

        ArrayList<OutputClass> members = null;
        // Display the sorted output in a tree order
        while (t != null){
            try{

                Map<Integer,OutputNode> map = new HashMap<>();

                OutputNode [] node = new OutputNode[projInc/2];
//                for(int i=1;i<=projInc;i+=2){
//                    node[i] = new OutputNode(t.getIntervalFld(i),t.getStrFld(i+1),i);
//                    map.put(t.getIntervalFld(i).getStart(),node[i]);
//                    current = t.getStrFld(groupCol);
//                }

                int k = 0;
                for(int i=1;i<=projInc;i+=2){
//                    node[k] = new OutputNode(t.getIntervalFld(i),t.getStrFld(i+1),i+1);
                    node[k] = new OutputNode(t.getIntervalFld(i),t.getStrFld(i+1),2);
                    map.put(t.getIntervalFld(i).getStart(),node[k]);
                    current = t.getStrFld(groupCol);
                    k++;
                }

                TreeMap<Integer, OutputNode> treeMap = new TreeMap<>(map);
                treeMap.putAll(map);

                boolean init = true;
                OutputClass root = null;
                Stack<OutputClass> stack = new Stack<>();

                for (Map.Entry<Integer, OutputNode> entry : treeMap.entrySet()){

//                    Stack<OutputClass> temp ;
                    if(init){
                        root = new OutputClass(entry.getValue());
                        init = false;
                        stack.push(root);
//                        temp.push(root);
                        continue;
                    }

                    OutputNode curr = entry.getValue();
                    Stack<OutputClass> temp = new Stack<>();
                    temp.addAll(stack);

                    while (!temp.isEmpty()){
                        OutputClass parent = temp.pop(); // root
                        if(curr.isChild(parent.getNode())){
                            curr.setSpace(parent.getNode().getSpace()+2);
                            OutputClass newChild = new OutputClass(curr);
                            parent.setChild(newChild);
                            stack.push(newChild);
                            break;
                        }
                        else{
                            continue;
                        }
                    }
                }

                if(groupElem == null){
                    members = new ArrayList();
                    groupElem = current;
                    members.add(root);
                }else if(groupElem.equals(current)){
                    members.add(root);
                }else if(!groupElem.equals(current)){
                    // make changes here and display
                    if(groupElem!= null){
                        displayGroup(members,currGroup,groupElem);
                    }
                    currGroup += 1;
                    members = new ArrayList();
                    members.add(root);
                    groupElem = current;
                }

            }
            catch(Exception e){
                e.printStackTrace();
            }
            count++;
            try {
                t = sort.get_next();
            }
            catch (Exception e) {
                status = FAIL;
                e.printStackTrace();
            }
            System.out.println();
        }
        displayGroup(members,currGroup,groupElem);
    }

    public void displayGroup(ArrayList<OutputClass> group,int groupInd,String groupVal){

        System.out.println("group_"+groupInd+"_root");
        System.out.println(" --"+"grouping_list"); // set the spaces correctly
        System.out.println("    --"+groupVal); // set the spaces correctly
        System.out.println(" --"+"group_subroot"); // set the spaces correctly


        for(int i = 0;i<group.size();i++){
            Stack<OutputClass> printStack = new Stack<>();
            printStack.push(group.get(i));
            while(!printStack.isEmpty()){
                OutputClass entry = printStack.pop();
                OutputNode entryNode = entry.getNode();
                for(int s=0;s<entryNode.getSpace();s++)
                    System.out.print(" ");

                System.out.print("--" +entryNode.getTagName() + " [" + entryNode.getStartValue() + ", "+ entryNode.getEndValue() + "]") ;
                System.out.println();
                if(entry.children != null){
                    for(int p =0;p<entry.children.size();p++)
                        printStack.push(entry.children.get(p));
                }
            }
        }
    }

    public NestedLoopsJoins processQuery(ArrayList<ArrayList<String>> rule, ArrayList<String> tags){


        // Creating conditional expression for the first rule


        // check if first rule has *
        int firstIndex = Integer.parseInt(rule.get(0).get(0));
        int secondIndex = Integer.parseInt(rule.get(0).get(1));
        String initRule = rule.get(0).get(2);

        CondExpr[] exprInit = null;

        if(tags.get(firstIndex-1).equals("*") && tags.get(secondIndex-1).equals("*")){

            // Both tags are *

            exprInit = new CondExpr[2];
            exprInit[0] = new CondExpr();
            exprInit[1] = new CondExpr();

            exprInit[0].next  = null;
            if(initRule.equals("AD")){
                exprInit[0].op    = new AttrOperator(AttrOperator.aopGT);
            }else{
                exprInit[0].op    = new AttrOperator(AttrOperator.aopPC);
            }

            exprInit[0].type1 = new AttrType(AttrType.attrSymbol);
            exprInit[0].type2 = new AttrType(AttrType.attrSymbol);
            exprInit[0].operand1.symbol = new FldSpec(new RelSpec(RelSpec.outer),1);
            exprInit[0].operand2.symbol = new FldSpec (new RelSpec(RelSpec.innerRel),1);
            exprInit[0].flag = 1;

            exprInit[1] = null;

        }
        else if(tags.get(firstIndex-1).equals("*") || tags.get(secondIndex-1).equals("*")){

            exprInit = new CondExpr[3];
            exprInit[0] = new CondExpr();
            exprInit[1] = new CondExpr();
            exprInit[2] = new CondExpr();

            if(initRule.equals("AD")){
                exprInit[0].op    = new AttrOperator(AttrOperator.aopGT);
            }else{
                exprInit[0].op    = new AttrOperator(AttrOperator.aopPC);
            }

            exprInit[0].type1 = new AttrType(AttrType.attrSymbol);
            exprInit[0].type2 = new AttrType(AttrType.attrSymbol);
            exprInit[0].operand1.symbol = new FldSpec(new RelSpec(RelSpec.outer),1);
            exprInit[0].operand2.symbol = new FldSpec (new RelSpec(RelSpec.innerRel),1);
            exprInit[0].flag = 1;

            if(tags.get(firstIndex-1).equals("*")){
                exprInit[1].op    = new AttrOperator(AttrOperator.aopEQ);
                exprInit[1].type1 = new AttrType(AttrType.attrSymbol);
                exprInit[1].type2 = new AttrType(AttrType.attrString);
                exprInit[1].operand1.symbol = new FldSpec(new RelSpec(RelSpec.innerRel),2);
                exprInit[1].operand2.string = tags.get(secondIndex-1);
                exprInit[1].next  = null;
            }else{
                System.out.println("Right star loop");
                exprInit[1].op    = new AttrOperator(AttrOperator.aopEQ);
                exprInit[1].type1 = new AttrType(AttrType.attrSymbol);
                exprInit[1].type2 = new AttrType(AttrType.attrString);
                exprInit[1].operand1.symbol = new FldSpec(new RelSpec(RelSpec.outer),2);
                exprInit[1].operand2.string = tags.get(firstIndex-1);
                exprInit[1].next  = null;
                System.out.println("Right star value used" + tags.get(firstIndex-1));
            }
            exprInit[2] = null;
        }
        else{
            exprInit = new CondExpr[4];
            exprInit[0] = new CondExpr();
            exprInit[1] = new CondExpr();
            exprInit[2] = new CondExpr();
            exprInit[3] = new CondExpr();
            if(initRule.equals("AD")){
                exprInit[0].op    = new AttrOperator(AttrOperator.aopGT);
            }else{
                exprInit[0].op    = new AttrOperator(AttrOperator.aopPC);
            }

            exprInit[0].type1 = new AttrType(AttrType.attrSymbol);
            exprInit[0].type2 = new AttrType(AttrType.attrSymbol);
            exprInit[0].operand1.symbol = new FldSpec(new RelSpec(RelSpec.outer),1);
            exprInit[0].operand2.symbol = new FldSpec (new RelSpec(RelSpec.innerRel),1);
            exprInit[0].flag = 1;


            exprInit[1].op    = new AttrOperator(AttrOperator.aopEQ);
            exprInit[1].type1 = new AttrType(AttrType.attrSymbol);
            exprInit[1].type2 = new AttrType(AttrType.attrString);
            exprInit[1].operand1.symbol = new FldSpec(new RelSpec(RelSpec.outer),2);
            exprInit[1].operand2.string = tags.get(firstIndex-1);
            exprInit[1].next  = null;


            exprInit[2].op    = new AttrOperator(AttrOperator.aopEQ);
            exprInit[2].next  = null;
            exprInit[2].type1 = new AttrType(AttrType.attrSymbol);
            exprInit[2].type2 = new AttrType(AttrType.attrString);
            exprInit[2].operand1.symbol = new FldSpec(new RelSpec(RelSpec.innerRel),2);
            exprInit[2].operand2.string = tags.get(secondIndex-1);

            exprInit[3] = null;

        }



        Map<String,Integer> tagFieldMap = new HashMap<>();

        tagFieldMap.put(rule.get(0).get(0),1);
        tagFieldMap.put(rule.get(0).get(1),3);

//        createCondInit(exprInit, rule.get(0).get(0), rule.get(0).get(1),rule.get(0).get(2));



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


            CondExpr[] expr = null;
//            int firstOp = Integer.parseInt(rule.get(i).get(0));
//            int secondOp = Integer.parseInt(rule.get(i).get(1));
//            String loopRule = rule.get(i).get(2);

            int first = Integer.parseInt(rule.get(i).get(0));
            int second = Integer.parseInt(rule.get(i).get(1));
            String currRule = rule.get(i).get(2);


            if(tags.get(first-1).equals("*") && tags.get(second-1).equals("*")){
                expr = new CondExpr[2];
                expr[0] = new CondExpr();
                expr[1] = new CondExpr();

                expr[0].next  = null;
                expr[0].type1 = new AttrType(AttrType.attrSymbol);
                expr[0].type2 = new AttrType(AttrType.attrSymbol);
                expr[0].flag = 1;


                if(tagFieldMap.containsKey(Integer.toString(first))){
                    GT = true;
                    expr[0].operand1.symbol = new FldSpec(new RelSpec(RelSpec.outer),tagFieldMap.get(tags.get(first-1)));
                    expr[0].operand2.symbol = new FldSpec (new RelSpec(RelSpec.innerRel),1);
//                    expr[1].operand1.symbol = new FldSpec(new RelSpec(RelSpec.innerRel),2);
//                    expr[1].operand2.string = tags.get(second-1);
                    tagFieldMap.put(Integer.toString(second),projInc+1);
                }else if(tagFieldMap.containsKey(Integer.toString(second))){
                    GT = false;
                    expr[0].operand1.symbol = new FldSpec(new RelSpec(RelSpec.outer),tagFieldMap.get(tags.get(second-1)));
                    expr[0].operand2.symbol = new FldSpec (new RelSpec(RelSpec.innerRel),1);
//                    expr[1].operand1.symbol = new FldSpec(new RelSpec(RelSpec.innerRel),2);
//                    expr[1].operand2.string = tags.get(first-1);
                    tagFieldMap.put(Integer.toString(first),projInc+1);
                }
                expr[1] = null;

            }else if(tags.get(first-1).equals("*") || tags.get(second-1).equals("*")){


                if(tags.get(first-1).equals("*")){
                    expr = new CondExpr[3];
                    expr[0] = new CondExpr();
                    expr[1] = new CondExpr();
                    expr[2] = new CondExpr();

                    expr[1].op    = new AttrOperator(AttrOperator.aopEQ);
                    expr[1].next  = null;
                    expr[1].type1 = new AttrType(AttrType.attrSymbol);
                    expr[1].type2 = new AttrType(AttrType.attrString);
                    expr[1].operand1.symbol = new FldSpec(new RelSpec(RelSpec.innerRel),2);
                    expr[1].operand2.string = tags.get(second-1);

//                    expr[1].operand1.symbol = new FldSpec(new RelSpec(RelSpec.innerRel),2);
//                    System.out.println("Tags set" + tags.get(second-1));
//                    expr[1].operand2.string = tags.get(second-1);

                    tagFieldMap.put(Integer.toString(second),projInc+1);

                    expr[2] = null;
                }else{
                    expr = new CondExpr[2];
                    expr[0] = new CondExpr();
                    expr[1] = new CondExpr();
                    expr[1] = null;
                }


                expr[0].type1 = new AttrType(AttrType.attrSymbol);
                expr[0].type2 = new AttrType(AttrType.attrSymbol);
                expr[0].flag = 1;


                if(tagFieldMap.containsKey(Integer.toString(first))){
                    GT = true;
                    expr[0].operand1.symbol = new FldSpec(new RelSpec(RelSpec.outer),tagFieldMap.get(Integer.toString(first)));
                    expr[0].operand2.symbol = new FldSpec(new RelSpec(RelSpec.innerRel),1);
//                    expr[1].operand1.symbol = new FldSpec(new RelSpec(RelSpec.innerRel),2);
//                    expr[1].operand2.string = tags.get(second-1);
                    tagFieldMap.put(Integer.toString(second),projInc+1);
                }else if(tagFieldMap.containsKey(Integer.toString(second))){
                    GT = false;
                    expr[0].operand1.symbol = new FldSpec(new RelSpec(RelSpec.outer),tagFieldMap.get(Integer.toString(second)));
                    expr[0].operand2.symbol = new FldSpec (new RelSpec(RelSpec.innerRel),1);
//                    expr[1].operand1.symbol = new FldSpec(new RelSpec(RelSpec.innerRel),2);
//                    expr[1].operand2.string = tags.get(first-1);
                    tagFieldMap.put(Integer.toString(first),projInc+1);

                }

            }
            else{
                expr = new CondExpr[3];
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

                if(tagFieldMap.containsKey(Integer.toString(first))){
                    GT = true;
                    expr[0].operand1.symbol = new FldSpec(new RelSpec(RelSpec.outer),tagFieldMap.get(Integer.toString(first)));
                    expr[0].operand2.symbol = new FldSpec (new RelSpec(RelSpec.innerRel),1);
                    expr[1].operand1.symbol = new FldSpec(new RelSpec(RelSpec.innerRel),2);
                    expr[1].operand2.string = tags.get(second-1);
                    tagFieldMap.put(Integer.toString(second),projInc+1);
                }else if(tagFieldMap.containsKey(Integer.toString(second))){
                    GT = false;
                    expr[0].operand1.symbol = new FldSpec(new RelSpec(RelSpec.outer),tagFieldMap.get(Integer.toString(second)));
                    expr[0].operand2.symbol = new FldSpec (new RelSpec(RelSpec.innerRel),1);
                    expr[1].operand1.symbol = new FldSpec(new RelSpec(RelSpec.innerRel),2);
                    expr[1].operand2.string = tags.get(first-1);
                    tagFieldMap.put(Integer.toString(first),projInc+1);

                }
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
        Task4  xmlinsert = new Task4();
        xmlinsert.parseComplexPT(patternTreePath);

        System.out.println("Read Counter = " + PCounter.rcounter);
        System.out.println("Write Counter = " + PCounter.wcounter);
        System.out.println("Total = " + (PCounter.rcounter + PCounter.wcounter));
    }
}
