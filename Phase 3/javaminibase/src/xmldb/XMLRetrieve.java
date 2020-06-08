package xmldb;

//originally from : joins.C

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

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;

import static tests.TestDriver.FAIL;
import static tests.TestDriver.OK;

@SuppressWarnings("Duplicates")
//Define the XMLTuple schema
class XMLTuple1 {

    public IntervalType interval;
    public String tagName;

    public XMLTuple1 (int start, int end, int level, String tag) {
        IntervalType val = new IntervalType();
        val.assign(start, end, level);
        interval = val;
        tagName = tag;
    }
}


class XMLRetrieve implements GlobalConst {

    private boolean OK = true;
    private boolean FAIL = false;
    private Vector xmlTuples;
    static XMLParser xmlParser = new XMLParser();
    HashMap<String, Integer> tagIndex = new HashMap<>();
    static HashSet<String> globalResults = new HashSet<>();
    
    private static int colLength = 0;
    
	ArrayList<ArrayList<String>> sortedRules;
    
	static int currentTagIndex = 1; // Used in the hashmap
    static int currentProjCount = 6;
    static int currentInstanceIndex = 1;
    
//    ArrayList<SortMerge> sortMergeInstanceList = new ArrayList<>();
    ArrayList<NestedLoopsJoins> nestedLoopInstanceList = new ArrayList<>();

    /** Constructor
     */
    public XMLRetrieve() {

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
                System.out.println(((XMLTuple)xmlTuples.elementAt(i)).tagName + " " + i);
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

    public static ArrayList<ArrayList<String>> getSortedRules(ArrayList<String> tags, ArrayList<ArrayList<String>> rules){
        ArrayList<Integer> isRuleVisited = new ArrayList<>();
        ArrayList<Integer> isTagVisited = new ArrayList<>();
        ArrayList<ArrayList<String>> sortedRules = new ArrayList<ArrayList<String>>();
        Queue<String> q = new LinkedList<>();

        //Mark all rules as unvisited first
        for(int i=0; i<rules.size(); i++){
            isRuleVisited.add(0);
        }
        // isRuleVisited.set(0,1);
        for(int i=0; i<tags.size(); i++){
            isTagVisited.add(0);
        }

        ((LinkedList<String>) q).add(rules.get(0).get(0));
        ((LinkedList<String>) q).add(rules.get(0).get(1));
        isTagVisited.set(tags.indexOf(rules.get(0).get(0)), 1);
        // isTagVisited.set(tags.indexOf(rules.get(0).get(1)), 1);


        while(q.size() != 0){
            String tag = q.remove();
            isTagVisited.set(tags.indexOf(tag),1);
            for(int i = 0; i< rules.size(); i++){
                if(isRuleVisited.get(i) == 0){

                    if(rules.get(i).get(0).equals(tag)){
                        isRuleVisited.set(i, 1);
                        sortedRules.add(rules.get(i));
                        if(isTagVisited.get(tags.indexOf(tag)) == 0){
                            q.add(tag);

                        }
                        if(isTagVisited.get(tags.indexOf(rules.get(i).get(1))) == 0){
                            q.add(rules.get(i).get(1));
                        }

                    }
                    if(rules.get(i).get(1).equals(tag)){
                        isRuleVisited.set(i, 1);
                        sortedRules.add(rules.get(i));
                        if(isTagVisited.get(tags.indexOf(tag)) == 0){
                            q.add(tag);
                        }
                        if(isTagVisited.get(tags.indexOf(rules.get(i).get(0))) == 0){
                            q.add(rules.get(i).get(0));
                        }

                    }
                }
            }
        }



        return sortedRules;
    }
    
    public ArrayList<ArrayList<String>> wrapperForSortedRules() {
    	// assume this reads the query file, and produces a list of tag names
    	try{
            File file = new File("/Users/akshayrao/git/dbmsiPhase2/javaminibase/src/xmldbTestXML/XMLQueryInput2.txt");
            Scanner scan =new Scanner(file);

            ArrayList<String> tags = new ArrayList<>();
            ArrayList<ArrayList<String>> rules = new ArrayList<ArrayList<String>>();


            //Scan numberoftags, tags and rules from file
            int numberOfTags = scan.nextInt();
            for(int i=0; i<numberOfTags; i++){
                String tag = scan.next();
                if(tag.length() > 5)
                    tag = tag.substring(0,5);
                tags.add(tag);
                //  System.out.println(tags.get(i));
            }
            int j = 0;
            while(scan.hasNext()){
                ArrayList<String> temp = new ArrayList<>();
                int leftTag = scan.nextInt();
                int rightTag = scan.nextInt();
                String relation = scan.next();
                //System.out.println(leftTag + " " + rightTag + " " + relation);
                temp.add(tags.get(leftTag-1));
                temp.add(tags.get(rightTag-1));
                temp.add(relation);
                //   System.out.println(temp.get(0) + " " + temp.get(1) + " " + temp.get(2));
                rules.add(temp);

            }
            ArrayList<String> reversedtags = new ArrayList<>();
            for(int i= tags.size()-1; i >=0; i--){
                reversedtags.add(tags.get(i));
            }

            ArrayList<ArrayList<String>> reversedRules = new ArrayList<ArrayList<String>>();
            for(int i = rules.size()-1; i >= 0; i--) {
                reversedRules.add(rules.get(i));
            }

            sortedRules = XMLQueryParsing.getSortedRules(tags, rules);
            return sortedRules;
    	} catch(Exception e) {
    		e.printStackTrace();
    	}
    	return new ArrayList<ArrayList<String>>();
    }
    
    public void combine() {
    	ArrayList<NestedLoopsJoins> instances = nestedLoopInstanceList;
    	NestedLoopsJoins sm1, sm2;
    	
    	int joinColumnIndex = 1; // incremented each time a new "tag" is added, because a tag will bring 2 columns
    	
        boolean status = OK;
        
        AttrType [] Stypes = new AttrType[4];
        Stypes[0] = new AttrType (AttrType.attrInterval);
        Stypes[1] = new AttrType (AttrType.attrString);
        Stypes[2] = new AttrType (AttrType.attrInterval);
        Stypes[3] = new AttrType (AttrType.attrString);
        
        short [] Ssizes = new short[2];
        Ssizes[0] = 5;
        Ssizes[1] = 5;
        

//        AttrType [] Stypes2 = new AttrType[2];
//        Stypes2[0] = new AttrType (AttrType.attrInterval);
//        Stypes2[1] = new AttrType (AttrType.attrString);

        FldSpec [] projectionList = new FldSpec[6];
        projectionList[0] = new FldSpec(new RelSpec(RelSpec.outer), 1);
        projectionList[1] = new FldSpec(new RelSpec(RelSpec.outer), 2);
        projectionList[2] = new FldSpec(new RelSpec(RelSpec.outer), 3);
        projectionList[3] = new FldSpec(new RelSpec(RelSpec.outer), 4);
        projectionList[4] = new FldSpec(new RelSpec(RelSpec.innerRel), 3);
        projectionList[5] = new FldSpec(new RelSpec(RelSpec.innerRel), 4);
        
        
        
        TupleOrder ascending = new TupleOrder(TupleOrder.Ascending);
        SortMerge2 tempInstance =null;
        
        if (!tagIndex.containsKey(sortedRules.get(0).get(0))) {
        	tagIndex.put(sortedRules.get(0).get(0), currentTagIndex);
        	 currentTagIndex+= 2;
        }
        
        if (!tagIndex.containsKey(sortedRules.get(0).get(1))) {
        	tagIndex.put(sortedRules.get(0).get(1), currentTagIndex);
        	currentTagIndex+= 2;
        }
        
        joinColumnIndex = tagIndex.get(sortedRules.get(currentInstanceIndex).get(0));
        
    	CondExpr[] expr = new CondExpr[2];
    	expr[0] = new CondExpr();
        expr[1] = null;
        expr[0].next  = null;

        expr[0].op    = new AttrOperator(AttrOperator.aopEQ);
        
        expr[0].type1 = new AttrType(AttrType.attrSymbol);
        expr[0].type2 = new AttrType(AttrType.attrSymbol);
        expr[0].operand1.symbol = new FldSpec(new RelSpec(RelSpec.outer),joinColumnIndex);
        expr[0].operand2.symbol = new FldSpec(new RelSpec(RelSpec.innerRel), 1);
        expr[0].flag = 1;
        
        if (!tagIndex.containsKey(sortedRules.get(currentInstanceIndex).get(1))) {
        	tagIndex.put(sortedRules.get(currentInstanceIndex).get(1), currentTagIndex);
        	currentTagIndex+= 2;
        }
        
        sm1 = nestedLoopInstanceList.get(0);
        sm2 = nestedLoopInstanceList.get(currentInstanceIndex);        
        
        try {
            tempInstance = new SortMerge2(Stypes, 4, Ssizes, Stypes, 4, Ssizes, joinColumnIndex, 12, 1, 12, 10, sm1, sm2, false, false, ascending, expr, projectionList, 6);
        }
        catch (Exception e) {
            System.err.println("*** join error in SortMerge constructor ***");
            status = FAIL;
            System.err.println (""+e);
            e.printStackTrace();
        }
        
        
    	currentInstanceIndex++;
    	CondExpr[] condExpr;
    	SortMerge2 sortMerge2Instance;
    	
    	int columnCount, tempVal;
    	boolean flag = false;
    	AttrType[] Stypes2;
    	colLength = 6;

    	SortMerge2 tempInstance2 = null;
    	for (int index = currentInstanceIndex; index < nestedLoopInstanceList.size(); index++) {
//    		sortMerge2Instance = tempInstance;
//    		sm2 = sortMergeInstanceList.get(index);
            
            Stypes2 = new AttrType[4];
            Stypes2[0] = new AttrType (AttrType.attrInterval);
            Stypes2[1] = new AttrType (AttrType.attrString);
            Stypes2[2] = new AttrType (AttrType.attrInterval);
            Stypes2[3] = new AttrType (AttrType.attrString);
            
            AttrType[] Stypes1 = new AttrType[tagIndex.size()*2];
            for (int currentColumn = 0; currentColumn < tagIndex.size()*2; currentColumn++) {
            	if (currentColumn%2 == 0) {
            		Stypes1[currentColumn] = new AttrType(AttrType.attrInterval);
            	} else {
            		Stypes1[currentColumn] = new AttrType(AttrType.attrString);
            	}
            }
            
            short[] Ssizes1 = new short[tagIndex.size()];
            for (int i=0; i<tagIndex.size(); i++) {
            	Ssizes1[i] = 5;
            }
            
            short [] Ssizes2 = new short[2];
            Ssizes2[0] = 5;
            Ssizes2[1] = 5;
            
            
          joinColumnIndex = tagIndex.get(sortedRules.get(index).get(0));
            
            if (!tagIndex.containsKey(sortedRules.get(index).get(1))) {
            	tagIndex.put(sortedRules.get(index).get(1), currentTagIndex);
            	currentTagIndex+= 2;
            	currentProjCount+= 2;
            	flag = true;
            }
            
            tempVal = flag ? currentProjCount : currentProjCount-2;
            FldSpec [] projectionList2 = new FldSpec[tempVal];
            
            colLength = tempVal;
            for (int i=0; i<tempVal; i++) {
            	projectionList2[i] = new FldSpec(new RelSpec(RelSpec.outer), i+1);
            }
            
            if (flag) {
                projectionList2[tempVal-2] = new FldSpec(new RelSpec(RelSpec.innerRel), 3);
                projectionList2[tempVal-1] = new FldSpec(new RelSpec(RelSpec.innerRel), 4);
            }           
            
            // TupleOrder ascending = new TupleOrder(TupleOrder.Ascending);
            
            tempInstance2 = tempInstance;
            sm2 = nestedLoopInstanceList.get(index);        
            System.out.println("Column length: " + projectionList2.length);
            try {
                tempInstance = new SortMerge2(Stypes1, Stypes1.length, Ssizes1, Stypes2, Stypes2.length, Ssizes2, joinColumnIndex, 12, 1, 12, 10, tempInstance2, sm2, false, false, ascending, expr, projectionList2, tempVal);
            }
            catch (Exception e) {
                System.err.println("*** join error in SortMerge constructor ***");
                status = FAIL;
                System.err.println (""+e);
                e.printStackTrace();
            }
    		
        	flag = false;
    	}
    	
    	boolean done = false;
        Tuple t = null;
        HashSet<String> tupleSet = new HashSet<>();
        try{
            while(!done){
                t = tempInstance.get_next();
                if(t == null) {
                    done = true;
                    break;
                }
//                byte[] tupleArray = t.getTupleByteArray();
//                IntervalType i = t.getIntervalFld(1);
//                IntervalType i2 = t.getIntervalFld(3);
//                IntervalType i3 = t.getIntervalFld(5);
//                IntervalType i4 = t.getIntervalFld(7);
//                IntervalType i5 = t.getIntervalFld(9);
//
//                String tagname = t.getStrFld(2);
//                String tagname2 = t.getStrFld(4);
//                String tagname3 = t.getStrFld(6);
//                String tagname4 = t.getStrFld(8);
//                String tagname5 = t.getStrFld(10);
//
//                String result = "***Start = " + i.start + " End = " +  i.end + " Level = " + i.level + " Tagname = " + tagname;
//                result += " Start = " + i2.start + " End = " +  i2.end + " Level = " + i2.level + " Tagname = " + tagname2;
//                result += " Start = " + i3.start + " End = " +  i3.end + " Level = " + i3.level + " Tagname = " + tagname3;
//                result += " Start = " + i4.start + " End = " +  i4.end + " Level = " + i4.level + " Tagname = " + tagname4;
//                result += " Start = " + i5.start + " End = " +  i5.end + " Level = " + i5.level + " Tagname = " + tagname5;
//                
               String res = "";
                for (int k=1; k<=colLength; k++) {
                	if(k%2 != 0) {
                		res += " | Start = " + t.getIntervalFld(k).getStart() + " End = " + t.getIntervalFld(k).getEnd() + " Level = " + t.getIntervalFld(k).getLevel();
//                		System.out.print(" | Start = " + t.getIntervalFld(k).getStart() + " End = " + t.getIntervalFld(k).getEnd() + " Level = " + t.getIntervalFld(k).getLevel());
                	}else {
                		res += " TagName = " + t.getStrFld(k);
//                		System.out.print(" TagName = " + t.getStrFld(k) );
                	}
                           
                }
//                System.out.println();
                globalResults.add(res);
            }
            
        } catch(Exception e){
            e.printStackTrace();
        }
    }
    
    public void wrapper() {
    	ArrayList<ArrayList<String>> sortedRules = this.wrapperForSortedRules();
    	
    	for(int i=0; i<sortedRules.size(); i++){
    		this.QP3(sortedRules.get(i).get(0), sortedRules.get(i).get(1), sortedRules.get(i).get(2));
        }
    }
    
    public NestedLoopsJoins createCondExprQP3(String tagName1, String tagName2, String operand, FileScan iterator1, FileScan iterator2) {
    	CondExpr[] expr = new CondExpr[4];
        expr[0] = new CondExpr();
        expr[1] = new CondExpr();
        expr[2] = new CondExpr();
        expr[3] = null;
        
        expr[0].next  = null;
        
        switch (operand) {
        case "AD":
            expr[0].op    = new AttrOperator(AttrOperator.aopGT);
        	break;
        case "PC":
            expr[0].op    = new AttrOperator(AttrOperator.aopPC);
        	break;
        default:
        	break;
        }
        
        expr[0].type1 = new AttrType(AttrType.attrSymbol);
        expr[0].type2 = new AttrType(AttrType.attrSymbol);
        expr[0].operand1.symbol = new FldSpec(new RelSpec(RelSpec.outer),1);
        expr[0].operand2.symbol = new FldSpec(new RelSpec(RelSpec.innerRel), 1);
        expr[0].flag = 1;
        
        expr[1].op    = new AttrOperator(AttrOperator.aopEQ);
        expr[1].type1 = new AttrType(AttrType.attrSymbol);
        expr[1].type2 = new AttrType(AttrType.attrString);
        expr[1].operand1.symbol = new FldSpec(new RelSpec(RelSpec.outer),2);
        expr[1].operand2.string = tagName1;
        expr[1].next  = null;


        expr[2].op    = new AttrOperator(AttrOperator.aopEQ);
        expr[2].next  = null;
        expr[2].type1 = new AttrType(AttrType.attrSymbol);
        expr[2].type2 = new AttrType(AttrType.attrString);
        expr[2].operand1.symbol = new FldSpec(new RelSpec(RelSpec.innerRel),2);
        expr[2].operand2.string = tagName2;
        
        Tuple t = new Tuple();

        AttrType [] Stypes = new AttrType[2];
        Stypes[0] = new AttrType (AttrType.attrInterval);
        Stypes[1] = new AttrType (AttrType.attrString);

        //SOS
        short [] Ssizes = new short[1];
        Ssizes[0] = 5; //first elt. is 30

        FldSpec [] Sprojection = new FldSpec[2];
        Sprojection[0] = new FldSpec(new RelSpec(RelSpec.outer), 1);
        Sprojection[1] = new FldSpec(new RelSpec(RelSpec.outer), 2);

        boolean status = OK;



        FileScan am = null;
//        FileScan am2 = null;
        try {
            am  = new FileScan("test.in", Stypes, Ssizes, (short)2, (short)2, Sprojection, null);
        }
        catch (Exception e) {
            e.printStackTrace();
        }

        FldSpec [] proj_list = new FldSpec[4];
        proj_list[0] = new FldSpec(new RelSpec(RelSpec.outer), 1);
        proj_list[1] = new FldSpec(new RelSpec(RelSpec.outer), 2);
        proj_list[2] = new FldSpec(new RelSpec(RelSpec.innerRel), 1);
        proj_list[3] = new FldSpec(new RelSpec(RelSpec.innerRel), 2);

//        AttrType [] jtype = new AttrType[4];
//        jtype[0] = new AttrType (AttrType.attrInterval);
//        jtype[1] = new AttrType(AttrType.attrString);
//        jtype[2] = new AttrType (AttrType.attrInterval);
//        jtype[3] = new AttrType(AttrType.attrString);

        TupleOrder ascending = new TupleOrder(TupleOrder.Ascending);
        NestedLoopsJoins sm =null;

        try {
            sm = new NestedLoopsJoins(Stypes, 2, Ssizes, Stypes, 2, Ssizes, 10, am, "test.in", expr, null, proj_list, 4);

        }
        catch (Exception e) {
            System.err.println("*** join error in SortMerge constructor ***");
            status = FAIL;
            System.err.println (""+e);
            e.printStackTrace();
        }

        if (status != OK) {
            //bail out
            System.err.println ("*** Error constructing SortMerge");
            Runtime.getRuntime().exit(1);
        }
        
        return sm;
    }
    
    // another wrapper calls this for every rule
    public void QP3(String tagName1, String tagName2, String constraint) {

    	String[] tagNames2 = {tagName1, tagName2};
    	
    	List<FileScan> fileScanIterators = new ArrayList<FileScan>();
    	
    	
    	for (String tagName: tagNames2) {
    		fileScanIterators.add(this.tagBasedSearchReturnFileScan(tagName));
    	}
    	
//    	SortMerge sortMergeInstance = this.createCondExprQP3(tagName1, tagName2, constraint, fileScanIterators.get(0), fileScanIterators.get(1));
    	NestedLoopsJoins nestedLoopInstance = this.createCondExprQP3(tagName1, tagName2, constraint, fileScanIterators.get(0), fileScanIterators.get(1));
    	nestedLoopInstanceList.add(nestedLoopInstance);
//    	sortMergeInstanceList.add(sortMergeInstance);
    }
    
    
    public FileScan tagBasedSearchReturnFileScan(String tagnname) {

        AttrType[] Stypes = new AttrType[2];
        Stypes[0] = new AttrType(AttrType.attrInterval);
        Stypes[1] = new AttrType(AttrType.attrString);

        short[] Ssizes = new short[1];
        Ssizes[0] = 5;

        FldSpec[] sproj = new FldSpec[2];
        sproj[0] = new FldSpec(new RelSpec(RelSpec.outer), 1);
        sproj[1] = new FldSpec(new RelSpec(RelSpec.outer), 2);

        FileScan tagSearchScan = null;

        try{
            CondExpr[] expr = new CondExpr[1];
            expr[0] = new CondExpr();

            expr[0].op = new AttrOperator(AttrOperator.aopEQ);

            expr[0].type1 = new AttrType(AttrType.attrSymbol);
            expr[0].type2 = new AttrType(AttrType.attrString);

            expr[0].operand1 = new Operand();
            expr[0].operand1.symbol = new FldSpec(new RelSpec(RelSpec.outer), 2);


            expr[0].operand2 = new Operand();
            expr[0].operand2.string = tagnname;

            tagSearchScan = new FileScan("test.in", Stypes, Ssizes, (short)2, (short)2, sproj, expr);

            return tagSearchScan;
        } catch (Exception e){
            e.printStackTrace();
        }
        
        return null;
    }

    public void tagBasedSearch(String tagnname) {
        CondExpr[] expr = new CondExpr[4];
        expr[0] = new CondExpr();
        expr[1] = new CondExpr();
        expr[2] = new CondExpr();
        expr[3] = new CondExpr();
        String parentTagName= "Example";
        String childTagName = "Test";
        String nextTagName = "Test";


        expr[0].next  = null;
        expr[0].op    = new AttrOperator(AttrOperator.aopGT);
        expr[0].type1 = new AttrType(AttrType.attrSymbol);
        expr[0].type2 = new AttrType(AttrType.attrSymbol);
        expr[0].operand1.symbol = new FldSpec(new RelSpec(RelSpec.outer),1);
        expr[0].operand2.symbol = new FldSpec (new RelSpec(RelSpec.innerRel),1);
        expr[0].flag = 1;
//        expr[1] = null;

//
        // Parent Node
        expr[2].op    = new AttrOperator(AttrOperator.aopEQ);
        expr[2].next  = null;
        expr[2].type1 = new AttrType(AttrType.attrSymbol);
        expr[2].type2 = new AttrType(AttrType.attrString);
        expr[2].operand1.symbol = new FldSpec(new RelSpec(RelSpec.outer),2);
        expr[2].operand2.string = "Entry";

        // Child Node
        expr[1].op    = new AttrOperator(AttrOperator.aopEQ);
        expr[1].next  = null;
        expr[1].type1 = new AttrType(AttrType.attrSymbol);
        expr[1].type2 = new AttrType(AttrType.attrString);
        expr[1].operand1.symbol = new FldSpec(new RelSpec(RelSpec.innerRel),2);
        expr[1].operand2.string = "Org";

//
        expr[3] = null;



        Tuple t = new Tuple();

        AttrType [] Stypes = new AttrType[2];
        Stypes[0] = new AttrType (AttrType.attrInterval);
        Stypes[1] = new AttrType (AttrType.attrString);

        //SOS
        short [] Ssizes = new short[1];
        Ssizes[0] = 5; //first elt. is 30

        FldSpec [] Sprojection = new FldSpec[2];
        Sprojection[0] = new FldSpec(new RelSpec(RelSpec.outer), 1);
        Sprojection[1] = new FldSpec(new RelSpec(RelSpec.outer), 2);

        FldSpec [] Rprojection = new FldSpec[2];
        Rprojection[0] = new FldSpec(new RelSpec(RelSpec.outer), 1);
        Rprojection[1] = new FldSpec(new RelSpec(RelSpec.outer), 2);


        CondExpr [] selects = new CondExpr [1];
        selects = null;
        boolean status = OK;



        FileScan am = null;
        FileScan am2 = null;
        try {
//

            am  = new FileScan("test.in", Stypes, Ssizes,
                    (short)2, (short)2,
                    Sprojection, null);

            am2 = new FileScan("test.in", Stypes, Ssizes,
                    (short)2, (short)2,
                    Rprojection, null);

            boolean done = false;
//


        }
        catch (Exception e) {
            e.printStackTrace();
        }

        FldSpec [] proj_list = new FldSpec[4];
        proj_list[0] = new FldSpec(new RelSpec(RelSpec.outer), 1);
        proj_list[1] = new FldSpec(new RelSpec(RelSpec.outer), 2);
        proj_list[2] = new FldSpec(new RelSpec(RelSpec.innerRel), 1);
        proj_list[3] = new FldSpec(new RelSpec(RelSpec.innerRel), 2);
        Map<String, Integer> tagIndex = new HashMap<String, Integer>();

        //Add projection index to map
        tagIndex.put(parentTagName, 1);
        tagIndex.put(childTagName, 2);

        AttrType [] jtype = new AttrType[2];
        jtype[0] = new AttrType (AttrType.attrInterval);
        jtype[1] = new AttrType (AttrType.attrInterval);
//        jtype[0] = new AttrType (AttrType.attrString);
//        jtype[1] = new AttrType (AttrType.attrString);


        NestedLoopsJoins sm =null;
        try {
//            sm = new SortMerge(Stypes, 2, Ssizes,
//                    Stypes, 2, Ssizes,
//                    2, 5,
//                    2, 5,
//                    10,
//                    am, am,
//                    false, false, ascending,
//                    expr, proj_list, 4);
            sm = new NestedLoopsJoins(Stypes, 2, Ssizes, Stypes,2,Ssizes,10,am,
                    "test.in",expr,null,proj_list,4 );
        }
        catch (Exception e) {
            System.err.println("*** join error in NestedLoop constructor ***");
            status = FAIL;
            System.err.println (""+e);
            e.printStackTrace();
        }

        int iteasd1 = 0;
        boolean done1 = false;
        try{
            while(!done1){
                t = sm.get_next();
                if(t == null){
                    done1 = true;
                    break;
                }
                iteasd1++;
                byte[] tupleArray = t.getTupleByteArray();
                IntervalType i = t.getIntervalFld(1);
                String tagname = t.getStrFld(2);
                IntervalType j = t.getIntervalFld(3);
                String tagname2 = t.getStrFld(4);
//                IntervalType k = t.getIntervalFld(5);
//                String tagname3 = t.getStrFld(6);
                //  XMLRecord rec = new XMLRecord(t);
                // System.out.println( "Start = " + i.start + " End = " +  i.end + " Level = " + i.level + " Tagname = " + tagname + "|   Start = " + j.start + " End = " +  j.end + " Level = " + j.level + " Tagname = " + tagname2);
               // System.out.println( "|    Start = " + k.start + " End = " +  k.end + " Level = " + k.level + " Tagname = " + tagname3);
            }
        } catch(Exception e){
            e.printStackTrace();
        }


        int nextProjectionIndex;

        if(nextTagName == childTagName){
            nextProjectionIndex = tagIndex.get(childTagName);
        } else if(nextTagName == parentTagName){
            nextProjectionIndex = tagIndex.get(parentTagName);
        } else {
            nextProjectionIndex = 0;
        }
        System.out.println("--------------------------------------------------------------------------------------------------------------");

        /*
        *
        *
        *
        * SECOND LEVEL COMPUTATION
         *
         *
         *
         *
         */

        CondExpr[] expr1 = new CondExpr[3];
        expr1[0] = new CondExpr();
        expr1[1] = new CondExpr();
        expr1[2] = new CondExpr();

        expr1[2] = null;



        expr1[0].next  = null;

        expr1[0].op    = new AttrOperator(AttrOperator.aopGT);
        expr1[0].type1 = new AttrType(AttrType.attrSymbol);
        expr1[0].type2 = new AttrType(AttrType.attrSymbol);
        expr1[0].operand1.symbol = new FldSpec(new RelSpec(RelSpec.outer),nextProjectionIndex);
        expr1[0].operand2.symbol = new FldSpec (new RelSpec(RelSpec.innerRel),1);
        expr1[0].flag = 1;
//        expr[1] = null;

//

        // Child Node
        expr1[1].op    = new AttrOperator(AttrOperator.aopEQ);
        expr1[1].next  = null;
        expr1[1].type1 = new AttrType(AttrType.attrSymbol);
        expr1[1].type2 = new AttrType(AttrType.attrString);
        expr1[1].operand1.symbol = new FldSpec(new RelSpec(RelSpec.innerRel),2);
        expr1[1].operand2.string = "Hi";


        Tuple t1 = new Tuple();

        FldSpec [] projection = new FldSpec[2];
        projection[0] = new FldSpec(new RelSpec(RelSpec.outer), 1);
        projection[1] = new FldSpec(new RelSpec(RelSpec.outer), 2);

         status = OK;

        AttrType [] Stypes1 = new AttrType[4];
        Stypes1[0] = new AttrType (AttrType.attrInterval);
        Stypes1[1] = new AttrType (AttrType.attrString);
        Stypes1[2] = new AttrType (AttrType.attrInterval);
        Stypes1[3] = new AttrType (AttrType.attrString);

        //SOS
        short [] Ssizes1 = new short[2];
        Ssizes1[0] = 5; //first elt. is 30
        Ssizes1[1] = 5;


        FileScan am1 = null;
        try {
            am1  = new FileScan("test.in", Stypes, Ssizes,
                    (short)2, (short)2,
                    projection, null);
        }
        catch (Exception e) {
            e.printStackTrace();
        }
    }


    public static void main(String argv[]) throws ParserConfigurationException, IOException, SAXException, TupleUtilsException, FileScanException, InvalidRelation {

        DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();
        dbf.setValidating(false);
        DocumentBuilder db = dbf.newDocumentBuilder();
        Document doc = db.parse(new FileInputStream(new File("/Users/akshayrao/git/dbmsiPhase2/javaminibase/src/xmldbTestXML/sample_data.xml")));

        Node root = doc.getDocumentElement();

        xmlParser.build(root);

        // xmlParser.BFS();

        xmlParser.preOrder(xmlParser.tree.root);
        System.out.println("---------------------------");
        System.out.println();
        xmlParser.BFSSetLevel();

        // xmlParser.BFSPrint();

        XMLRetrieve instance = new XMLRetrieve();
        instance.wrapper();
        if (instance.nestedLoopInstanceList.size() > 1)
        	instance.combine();
        
        for (String result: globalResults) {
        	System.out.println(result);
        }

       System.out.println("Records returned: " + globalResults.size());
    }
}

