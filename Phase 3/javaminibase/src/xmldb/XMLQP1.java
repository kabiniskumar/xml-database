package xmldb;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Scanner;
import java.util.Vector;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;

import org.w3c.dom.Document;
import org.w3c.dom.Node;
import org.xml.sax.SAXException;

import btree.BTreeFile;
import btree.IntervalKey;
import btree.StringKey;
import bufmgr.BufMgrException;
import bufmgr.HashOperationException;
import bufmgr.PageNotFoundException;
import bufmgr.PagePinnedException;
import bufmgr.PageUnpinnedException;
import global.AttrOperator;
import global.AttrType;
import global.IndexType;
import global.IntervalType;
import global.RID;
import global.SystemDefs;
import heap.FieldNumberOutOfBoundException;
import heap.HFBufMgrException;
import heap.HFDiskMgrException;
import heap.HFException;
import heap.Heapfile;
import heap.Scan;
import heap.Tuple;
import index.IndexException;
import index.IndexScan;
import intervaltree.IntervalTreeFile;
import iterator.CondExpr;
import iterator.FileScan;
import iterator.FileScanException;
import iterator.FldSpec;
import iterator.InvalidRelation;
import iterator.Iterator;
import iterator.NestedLoopException;
import iterator.NestedLoopsJoins;
import iterator.Projection;
import iterator.RelSpec;
import iterator.TupleUtils;
import iterator.TupleUtilsException;
import iterator.UnknowAttrType;

public class XMLQP1 {

    private boolean OK = true;
    private boolean FAIL = false;
    boolean status = OK;
    private Vector<XMLTuple> xmlTuples;
    static XMLParser xmlParser = new XMLParser();
    static XMLQP1 qp1;
    static int projInc = 4;

    public void xmlDataInsert() throws HashOperationException, PageUnpinnedException, PagePinnedException, PageNotFoundException, BufMgrException, IOException {

        xmlTuples = new Vector<XMLTuple>();

        int numTuples = xmlParser.listOfXMLObjects.size();
        int numTuplesAttrs = 2;

        for (int i = 0; (i < numTuples); i++) {
            //fixed length record
            XMLInputTreeNode node = xmlParser.listOfXMLObjects.get(i);
            xmlTuples.addElement(new XMLTuple(node.interval.start, node.interval.end, node.interval.level, node.tagName));
        }


        String dbpath = "/tmp/" + System.getProperty("user.name") + ".minibase.jointestdb";
        String logpath = "/tmp/" + System.getProperty("user.name") + ".joinlog";

        String remove_cmd = "/bin/rm -rf ";
        String remove_logcmd = remove_cmd + logpath;
        String remove_dbcmd = remove_cmd + dbpath;
        String remove_joincmd = remove_cmd + dbpath;

        try {
            Runtime.getRuntime().exec(remove_logcmd);
            Runtime.getRuntime().exec(remove_dbcmd);
            Runtime.getRuntime().exec(remove_joincmd);
        } catch (IOException e) {
            System.err.println("" + e);
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

        //////////////////

        //SystemDefs.JavabaseDB.closeDB();




        //////////////////
        // System.out.println(xmlParser.listOfXMLObjects.size());
        //		SystemDefs.JavabaseBM.flushAllPages();

        if (status != OK) {
            //bail out
            System.err.println("*** Error creating relation for sailors");
            Runtime.getRuntime().exit(1);
        }
        System.out.println("");
        System.out.println("");
        System.out.println("DONE");


        //------------------------------------------------------------------------------------------------------------------------------------//

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

    /*
     * firstTuple -> tuples from tagIndexSearch for first tag in the rule
     * lastTag -> name of the second tag in the rule
     * op -> AD/PC
     * f -> unique file in which single rule results are written
     * */
    public static Heapfile indexIntervalSearchFile(Tuple firstTuple, String lastTag, String op, Heapfile f) throws IndexException, IOException, FieldNumberOutOfBoundException, UnknowAttrType, TupleUtilsException, HFException, HFBufMgrException, HFDiskMgrException {

        //System.out.println("------------------------------------------------------------Index Interval File Search Started ------------------------------------------------------------");



        AttrType[] Stypes = new AttrType[2];
        Stypes[0] = new AttrType(AttrType.attrInterval);
        Stypes[1] = new AttrType(AttrType.attrString);


        short[] Ssizes = new short[1];
        Ssizes[0] = 10; //first elt. is 30

        FldSpec[] projlist = new FldSpec[2];
        projlist[0] = new FldSpec(new RelSpec(RelSpec.outer), 1);
        projlist[1] = new FldSpec(new RelSpec(RelSpec.outer), 2);

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

        int attrOperator;
        if("PC".equals(op)) {
            attrOperator = AttrOperator.aopCP;
        } else {
            attrOperator = AttrOperator.aopAD;
        }

        //Get FirstTag interval
        IntervalType intTest = new IntervalType(firstTuple.getIntervalFld(1).start, firstTuple.getIntervalFld(1).end, firstTuple.getIntervalFld(1).level);

        //Set up CondExpr for A B AD using
        CondExpr[] select = new CondExpr[3];

        select[0] = new CondExpr();
        select[0].op = new AttrOperator(attrOperator);
        select[0].type1 = new AttrType(AttrType.attrSymbol);
        select[0].type2 = new AttrType(AttrType.attrInterval);
        select[0].operand1.symbol = new FldSpec(new RelSpec(RelSpec.outer), 1);
        select[0].operand2.interval = intTest;
        select[0].flag = 1;
        select[0].next = select[1];

        select[1] = new CondExpr();
        select[1].op = new AttrOperator(AttrOperator.aopEQ);
        select[1].type1 = new AttrType(AttrType.attrSymbol);
        select[1].type2 = new AttrType(AttrType.attrString);
        select[1].operand1.symbol = new FldSpec(new RelSpec(RelSpec.outer), 2);
        select[1].operand2.string = lastTag;
        select[2] = null;



        if("*".equals(lastTag)) {
            select[1] = null;
        }


        // start index scan
        IndexScan iscan = null;
        try {
            iscan = new IndexScan(new IndexType(IndexType.I_Index), "test.in", "IntervalTreeIndex", Stypes, Ssizes, 2, 2, projlist, select, 1, false);
        } catch (Exception e) {

            e.printStackTrace();
        }
        iscan.close();


        Tuple secondTuple = null;
        Tuple resultTuple = new Tuple();
        TupleUtils.setup_op_tuple(resultTuple, ResultTypes, Stypes, 2, Stypes, 2, Ssizes, Ssizes, resultProjlist, 4);
        String outval = null;
        String outval2 = null;


        try {
            secondTuple = iscan.get_next();
        } catch (Exception e) {

            e.printStackTrace();
        }



        while (secondTuple != null) {

            //For every result from intervalIndexSearch join with first to get a new tuple
            Projection.Join(firstTuple, Stypes, secondTuple, Stypes, resultTuple, resultProjlist, 4);

            IntervalType intervalResult = null;
            IntervalType intervalResult2 = null;

            try {
                f.insertRecord(resultTuple.returnTupleByteArray());
            } catch(Exception e) {
                e.printStackTrace();
            }


            try {
                outval = resultTuple.getStrFld(2);
                intervalResult = resultTuple.getIntervalFld(1);
                outval2 = resultTuple.getStrFld(4);
                intervalResult2 = resultTuple.getIntervalFld(3);
                //
                System.out.print("TagName = " + outval + " Start = " + intervalResult.start + " End = " + intervalResult.end + " Level = " + intervalResult.level);
                System.out.println("|| TagName = " + outval2 + " Start = " + intervalResult2.start + " End = " + intervalResult2.end + " Level = " + intervalResult2.level);

            } catch (Exception e) {

                e.printStackTrace();
            }


            try {
                secondTuple = iscan.get_next();
            } catch (Exception e) {

                e.printStackTrace();
            }
        }
        // clean up
        try {
            iscan.close();
        } catch (Exception e) {

            e.printStackTrace();
        }
//		System.out.println("------------------------------------------------------------Index Interval File Search Complete ------------------------------------------------------------\n\n\n");
        return f;
    }


    public static FileScan indexIntervalSearchScan(Tuple firstTuple, String lastTag, String op, int fileCounter) throws IndexException, IOException, FieldNumberOutOfBoundException, UnknowAttrType, TupleUtilsException, FileScanException, InvalidRelation {

        //System.out.println("------------------------------------------------------------Index Interval Search Started ------------------------------------------------------------");


        AttrType[] Stypes = new AttrType[2];
        Stypes[0] = new AttrType(AttrType.attrInterval);
        Stypes[1] = new AttrType(AttrType.attrString);


        short[] Ssizes = new short[1];
        Ssizes[0] = 10; //first elt. is 30

        FldSpec[] projlist = new FldSpec[2];
        projlist[0] = new FldSpec(new RelSpec(RelSpec.outer), 1);
        projlist[1] = new FldSpec(new RelSpec(RelSpec.outer), 2);

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

        int attrOperator;
        if("PC".equals(op)) {
            attrOperator = AttrOperator.aopCP;
        } else {
            attrOperator = AttrOperator.aopAD;
        }

        //Get FirstTag interval
        IntervalType intTest = new IntervalType(firstTuple.getIntervalFld(1).start, firstTuple.getIntervalFld(1).end, firstTuple.getIntervalFld(1).level);

        //Set up CondExpr for A B AD using
        CondExpr[] select = new CondExpr[3];

        select[0] = new CondExpr();
        select[0].op = new AttrOperator(attrOperator);
        select[0].type1 = new AttrType(AttrType.attrSymbol);
        select[0].type2 = new AttrType(AttrType.attrInterval);
        select[0].operand1.symbol = new FldSpec(new RelSpec(RelSpec.outer), 1);
        select[0].operand2.interval = intTest;
        select[0].flag = 1;
        select[0].next = select[1];

        select[1] = new CondExpr();
        select[1].op = new AttrOperator(AttrOperator.aopEQ);
        select[1].type1 = new AttrType(AttrType.attrSymbol);
        select[1].type2 = new AttrType(AttrType.attrString);
        select[1].operand1.symbol = new FldSpec(new RelSpec(RelSpec.outer), 2);
        select[1].operand2.string = lastTag;
        select[2] = null;



        if("*".equals(lastTag)) {
            select[1] = null;
        }


        // start index scan
        IndexScan iscan = null;
        try {
            iscan = new IndexScan(new IndexType(IndexType.I_Index), "test.in", "IntervalTreeIndex", Stypes, Ssizes, 2, 2, projlist, select, 1, false);
        } catch (Exception e) {

            e.printStackTrace();
        }
        iscan.close();

        //return iscan;

        //File that stores the singlequery results
        Heapfile f = null;
        String filename = "singlequeryresults" + fileCounter + ".in";
        try {
            f = new Heapfile(filename);
        } catch (Exception e) {
            System.err.println("*** error in Heapfile constructor ***");

            e.printStackTrace();
        }




        Tuple secondTuple = null;
        Tuple resultTuple = new Tuple();
        TupleUtils.setup_op_tuple(resultTuple, ResultTypes, Stypes, 2, Stypes, 2, Ssizes, Ssizes, resultProjlist, 4);
        String outval = null;
        String outval2 = null;


        try {
            secondTuple = iscan.get_next();
        } catch (Exception e) {

            e.printStackTrace();
        }



        while (secondTuple != null) {

            //For every result from intervalIndexSearch join with first to get a new tuple
            Projection.Join(firstTuple, Stypes, secondTuple, Stypes, resultTuple, resultProjlist, 4);

            IntervalType intervalResult = null;
            IntervalType intervalResult2 = null;

            try {
                f.insertRecord(resultTuple.returnTupleByteArray());
            } catch(Exception e) {
                e.printStackTrace();
            }


            try {
                outval = resultTuple.getStrFld(2);
                intervalResult = resultTuple.getIntervalFld(1);
                outval2 = resultTuple.getStrFld(4);
                intervalResult2 = resultTuple.getIntervalFld(3);
                //
                System.out.print("TagName = " + outval + " Start = " + intervalResult.start + " End = " + intervalResult.end + " Level = " + intervalResult.level);
                System.out.println("|| TagName = " + outval2 + " Start = " + intervalResult2.start + " End = " + intervalResult2.end + " Level = " + intervalResult2.level);

            } catch (Exception e) {

                e.printStackTrace();
            }


            try {
                secondTuple = iscan.get_next();
            } catch (Exception e) {

                e.printStackTrace();
            }
        }
        // clean up
        try {
            iscan.close();
        } catch (Exception e) {

            e.printStackTrace();
        }

        FileScan scan = new FileScan(filename, ResultTypes, resultSizes,(short) 4, (short) 4, resultProjlist, null);
        Tuple dummy = null;
        try {
            dummy = iscan.get_next();
        } catch (Exception e) {

            e.printStackTrace();
        }

        while(dummy != null) {
            try {
                String outvalDummy = dummy.getStrFld(2);
                IntervalType intervalResultDummy = dummy.getIntervalFld(1);
                String outval2Dummy = dummy.getStrFld(4);
                IntervalType intervalResult2Dummy = dummy.getIntervalFld(3);

                System.out.print("TagName = " + outvalDummy + " Start = " + intervalResultDummy.start + " End = " + intervalResultDummy.end + " Level = " + intervalResultDummy.level);
                System.out.println("|| TagName = " + outval2Dummy + " Start = " + intervalResult2Dummy.start + " End = " + intervalResult2Dummy.end + " Level = " + intervalResult2Dummy.level);

            } catch (Exception e) {

                e.printStackTrace();
            }
        }

        //	System.out.println("------------------------------------------------------------Index Tag Search Complete ------------------------------------------------------------\n\n\n");
        return scan;
    }

    public static IndexScan indexTagSearch(String tag){

        System.out.println("------------------------------------------------------------Index Tag Search Started ------------------------------------------------------------");
        int numTuplesAttrs = 2;

        //Set up atrributes types
        AttrType[] Stypes = new AttrType[numTuplesAttrs];
        Stypes[0] = new AttrType (AttrType.attrInterval);
        Stypes[1] = new AttrType (AttrType.attrString);

        //Set up the size for String type attribute
        short[] Ssizes;
        Ssizes = new short [1];
        Ssizes[0] = 10;


        // create a tuple of appropriate size
        Tuple t = new Tuple();
        try {
            t.setHdr((short) 2, Stypes, Ssizes);
        }
        catch (Exception e) {

            e.printStackTrace();
        }
        //Fetch the tuple size
        int size = t.size();
        t = new Tuple(size);

        try {
            t.setHdr((short) 2, Stypes, Ssizes);
        }
        catch (Exception e) {

            e.printStackTrace();
        }


        //Set up the projections for the result
        FldSpec[] projlist = new FldSpec[2];
        projlist[0] = new FldSpec(new RelSpec(RelSpec.outer), 1);
        projlist[1] = new FldSpec(new RelSpec(RelSpec.outer), 2);


        //Set up the condition expression for tag based index scan search
        CondExpr[] expr = new CondExpr[2];
        expr[0] = new CondExpr();
        expr[0].op = new AttrOperator(AttrOperator.aopEQ);
        expr[0].type1 = new AttrType(AttrType.attrSymbol);
        expr[0].type2 = new AttrType(AttrType.attrString);
        expr[0].operand1.symbol = new FldSpec(new RelSpec(RelSpec.outer), 2);
        expr[0].operand2.string = tag;
        expr[0].next = null;
        expr[1] = null;

        //Start index scan
        IndexScan iscan = null;
        try {
            iscan = new IndexScan(new IndexType(IndexType.B_Index), "test.in", "BTreeIndex", Stypes, Ssizes, 2, 2, projlist, expr, 4, false);
        }
        catch (Exception e) {

            e.printStackTrace();
        }

        return iscan;
        //
        //		//Set up variables to hold results
        //		String stringResultFirst = null;
        //		IntervalType intervalResultFirst = new IntervalType();
        //		List<XMLInputTreeNode> tagResults = new ArrayList<XMLInputTreeNode>();
        //		List<Tuple> tagTuples = new ArrayList<Tuple>();
        //
        //		//Get tuples one at a time
        //		try {
        //			t = iscan.get_next();
        //			tagTuples.add(t);
        //		}
        //		catch (Exception e) {
        //
        //			e.printStackTrace();
        //		}
        //
        //		if (t == null) {
        //			System.err.println("Index tag search -- no record retrieved.");
        //			return null;
        //		}
        //
        //		//Fetch and print one tuple at a time
        //		while( t!= null){
        //
        //			try {
        //
        //				stringResultFirst = t.getStrFld(2);
        //				intervalResultFirst = t.getIntervalFld(1);
        //
        //
        //				tagResults.add(new XMLInputTreeNode(stringResultFirst, intervalResultFirst));
        //
        //
        //			}
        //			catch (Exception e) {
        //
        //				e.printStackTrace();
        //			}
        //
        //			System.out.println("Tag name = " + stringResultFirst + " Start = " + intervalResultFirst.start + " End = " + intervalResultFirst.end + " Level = " + intervalResultFirst.level);
        //
        //			try {
        //				t = iscan.get_next();
        //			}
        //			catch (Exception e) {
        //
        //				e.printStackTrace();
        //			}
        //		}
        //
        //
        //		//Clean up
        //		try {
        //			iscan.close();
        //		}
        //		catch (Exception e) {
        //
        //			e.printStackTrace();
        //		}
        //
        //		System.out.println("------------------------------------------------------------Index Tag Search Complete ------------------------------------------------------------\n\n\n");
        //
        //		return tagResults;


    }

    /*
     * firstTag -> first tag of the rule
     * lastTag -> second tag of the rule
     * op -> AD/PC
     * fileCounter -> unique number used to create unique filenames for each rule. Please ensure you pass some unique rule
     * */
    public static String queryRuleIteratorFile(String firstTag, String lastTag, String op, int fileCounter) throws HFException, HFBufMgrException, HFDiskMgrException, IOException, FileScanException, TupleUtilsException, InvalidRelation, IndexException {
        IndexScan iscan = indexTagSearch(firstTag);

        String filename = "singlequeryresults" + fileCounter + ".in";
        Heapfile f = null;
        try {
            f = new Heapfile(filename);
        } catch (Exception e) {
            System.err.println("*** error in Heapfile constructor ***");

            e.printStackTrace();
        }

        Tuple t = null;
        try {
            t = iscan.get_next();
        } catch (Exception e) {

            e.printStackTrace();
        }


        while(t != null) {
            try {
                indexIntervalSearchFile(t, lastTag, op, f);
                t = iscan.get_next();

            } catch (Exception e) {

                e.printStackTrace();
            }

        }

        return filename;





    }

    public static FileScan queryRuleIteratorScan(String firstTag, String lastTag, String op, int fileCounter) throws HFException, HFBufMgrException, HFDiskMgrException, IOException, IndexException, FileScanException, TupleUtilsException, InvalidRelation {


        String filename = queryRuleIteratorFile(firstTag, lastTag, op, fileCounter);
//		Heapfile f = null;
//		try {
//			f = new Heapfile(filename);
//		} catch (Exception e) {
//			System.err.println("*** error in Heapfile constructor ***");
//
//			e.printStackTrace();
//		}

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
        resultProjlist[2] = new FldSpec(new RelSpec(RelSpec.outer), 3);
        resultProjlist[3] = new FldSpec(new RelSpec(RelSpec.outer), 4);

        FileScan scan = new FileScan(filename, ResultTypes, resultSizes,(short) 4, (short) 4, resultProjlist, null);


        return scan;
    }



    public static void queryHandler(String path, int patternTreeNumber) throws InvalidRelation, TupleUtilsException, FileScanException, IOException, HashOperationException, PageUnpinnedException, PagePinnedException, PageNotFoundException, BufMgrException, HFException, HFBufMgrException, HFDiskMgrException, IndexException, NestedLoopException, FieldNumberOutOfBoundException {



        Map<String,String> tagMap = new HashMap<>();

        //File Scan Operations
        //Reading
        File file = new File(path);
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
		/* for(int i=0; i<reverseSortedRules.size(); i++){
            for(int k=0; k<reverseSortedRules.get(i).size(); k++){
                System.out.print(reverseSortedRules.get(i).get(k) + " ");
            }
            System.out.println();
        } */


        Iterator result = processQueries(sortedRules, tagMap, patternTreeNumber);



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


    public static Iterator processQueries(ArrayList<ArrayList<String>> rule, Map<String,String> tagMap, int patternTreeNumber) throws HashOperationException, PageUnpinnedException, PagePinnedException, PageNotFoundException, BufMgrException, IOException, HFException, HFBufMgrException, HFDiskMgrException, IndexException, FileScanException, TupleUtilsException, InvalidRelation, NestedLoopException, FieldNumberOutOfBoundException {
        // A B OP -> firstTag lastTag aopAD/aopPC
        //				String firstTag = "Entry";
        //				String lastTag = "Mod";
        //				String op = "AD"; //CP

        qp1 = new XMLQP1();
        qp1.xmlDataInsert();

        Map<String,Integer> tagFieldMap = new HashMap<>();

        tagFieldMap.put(rule.get(0).get(0),1);
        tagFieldMap.put(rule.get(0).get(1),3);

        //Incrementing filename number to make the filename unique
        patternTreeNumber ++;
        Iterator prev = queryRuleIteratorScan(rule.get(0).get(0), rule.get(0).get(1), rule.get(0).get(2), patternTreeNumber);


//		AttrType[] ResultTypes = new AttrType[4];
//		ResultTypes[0] = new AttrType(AttrType.attrInterval);
//		ResultTypes[1] = new AttrType(AttrType.attrString);
//		ResultTypes[2] = new AttrType(AttrType.attrInterval);
//		ResultTypes[3] = new AttrType(AttrType.attrString);
//
//		short[] resultSizes = new short[2];
//		resultSizes[0] = 10;
//		resultSizes[1] = 10;
//
//		FldSpec[] resultProjlist = new FldSpec[6];
//		resultProjlist[0] = new FldSpec(new RelSpec(RelSpec.outer), 1);
//		resultProjlist[1] = new FldSpec(new RelSpec(RelSpec.outer), 2);
//		resultProjlist[2] = new FldSpec(new RelSpec(RelSpec.outer), 3);
//		resultProjlist[3] = new FldSpec(new RelSpec(RelSpec.outer), 4);
//		resultProjlist[4] = new FldSpec(new RelSpec(RelSpec.innerRel), 3);
//		resultProjlist[5] = new FldSpec(new RelSpec(RelSpec.innerRel), 4);
//
//		//Set up condExpr for first rule
//		CondExpr[] condExpr = new CondExpr[2];
//		condExpr[0] = new CondExpr();
//		condExpr[0].type1 = new AttrType(AttrType.attrSymbol);
//		condExpr[0].type2 = new AttrType(AttrType.attrSymbol);
//		condExpr[0].operand1.symbol = new FldSpec(new RelSpec(RelSpec.outer),1);
//		condExpr[0].operand2.symbol = new FldSpec(new RelSpec(RelSpec.innerRel),1);
//		condExpr[0].op = new AttrOperator(AttrOperator.aopEQ);
//		condExpr[0].next = null;
//		condExpr[0].flag = 1;
//		condExpr[1] = null;

//		NestedLoopsJoins prev = new NestedLoopsJoins(ResultTypes, 4, resultSizes, ResultTypes, 4, resultSizes, 10, scan, filename2, condExpr, null, resultProjlist, 6);

        for(int i = 1; i < rule.size(); i++) {
            // For each rule
            CondExpr[] expr = new CondExpr[2];
            expr[0] = new CondExpr();
            expr[1] = null;


            expr[0].next  = null;
            expr[0].type1 = new AttrType(AttrType.attrSymbol);
            expr[0].type2 = new AttrType(AttrType.attrSymbol);
            expr[0].op = new AttrOperator(AttrOperator.aopEQ);
            expr[0].flag = 1;

            String firstTag = rule.get(i).get(0);
            String lastTag = rule.get(i).get(1);
            String currRule = rule.get(i).get(2);

            //Right relation filename to be passed to nlj
            String rightRelationFilename = queryRuleIteratorFile(firstTag, lastTag, currRule, ++patternTreeNumber);



            if(tagFieldMap.containsKey(firstTag)){
//				GT = true;
                expr[0].operand1.symbol = new FldSpec(new RelSpec(RelSpec.outer),tagFieldMap.get(firstTag));
                expr[0].operand2.symbol = new FldSpec (new RelSpec(RelSpec.innerRel),1);
                tagFieldMap.put(lastTag,projInc+1);
            }else if(tagFieldMap.containsKey(lastTag)){
//				GT = false;
                expr[0].operand1.symbol = new FldSpec(new RelSpec(RelSpec.outer),tagFieldMap.get(lastTag));
                expr[0].operand2.symbol = new FldSpec (new RelSpec(RelSpec.innerRel),1);
                tagFieldMap.put(firstTag,projInc+1);

            }

            AttrType[] RightTypes = new AttrType[4];
            RightTypes[0] = new AttrType(AttrType.attrInterval);
            RightTypes[1] = new AttrType(AttrType.attrString);
            RightTypes[2] = new AttrType(AttrType.attrInterval);
            RightTypes[3] = new AttrType(AttrType.attrString);


            short [] rightSizes = new short[2];
            rightSizes[0] = 10;
            rightSizes[1] = 10;


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

//			if(!GT){
//				for(k=0;k<projInc;k++){
//					//TODO verify if outer works always
//					outProjection[k] = new FldSpec(new RelSpec(RelSpec.outer), k+1);
//				}
//				outProjection[k] = new FldSpec(new RelSpec(RelSpec.innerRel), 1);
//				outProjection[k+1] = new FldSpec(new RelSpec(RelSpec.innerRel), 2);
//
//
//			}
//			else{
            for(k=0;k<projInc;k++){
                //TODO verify if outer works always
                outProjection[k] = new FldSpec(new RelSpec(RelSpec.outer), k+1);
            }
            outProjection[k] = new FldSpec(new RelSpec(RelSpec.innerRel), 3);
            outProjection[k+1] = new FldSpec(new RelSpec(RelSpec.innerRel), 4);

//			}

            NestedLoopsJoins nljLoop =null;
            try {
                nljLoop = new NestedLoopsJoins(LeftTtypes, projInc, leftSizes, RightTypes,4,rightSizes,10,prev,
                        rightRelationFilename,expr,null,outProjection,projInc+2 );
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
        return prev;
    }

    public static void main(String[] args) throws FileNotFoundException, ParserConfigurationException, SAXException, IOException, HashOperationException, PageUnpinnedException, PagePinnedException, PageNotFoundException, BufMgrException, HFException, HFBufMgrException, HFDiskMgrException, FileScanException, TupleUtilsException, InvalidRelation, IndexException, NestedLoopException, FieldNumberOutOfBoundException {


        System.out.println("XMLQP1");
        DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();
        dbf.setValidating(false);
        DocumentBuilder db = dbf.newDocumentBuilder();
        Document doc = db.parse(new FileInputStream(new File("/home/user/Desktop/AfterChanges/dbmsiPhase2/javaminibase/src/xmldbTestXML/sample_data.xml")));

        Node root = doc.getDocumentElement();

        xmlParser.build(root);
        xmlParser.preOrder(xmlParser.tree.root);
        xmlParser.BFSSetLevel();

        queryHandler("/home/user/Desktop/AfterChanges/dbmsiPhase2/javaminibase/src/xmldbTestXML/pattern_tree2.txt", 200);



        //		Tuple t = null;
        //
        //		try {
        //			t = scan.get_next();
        //		} catch (Exception e) {
        //			e.printStackTrace();
        //		}
        //		while(t!=null) {
        //			String outval;
        //			IntervalType intervalResult;
        //			String outval2;
        //			IntervalType intervalResult2;
        //			try {
        //				outval = t.getStrFld(2);
        //				intervalResult = t.getIntervalFld(1);
        //				outval2 = t.getStrFld(4);
        //				intervalResult2 = t.getIntervalFld(3);
        //				System.out.print("TagName = " + outval + " Start = " + intervalResult.start + " End = " + intervalResult.end + " Level = " + intervalResult.level);
        //				System.out.println("|| TagName = " + outval2 + " Start = " + intervalResult2.start + " End = " + intervalResult2.end + " Level = " + intervalResult2.level);
        //
        //			} catch (Exception e) {
        //
        //				e.printStackTrace();
        //			}
        //			try {
        //				t = scan.get_next();
        //			} catch (Exception e) {
        //				e.printStackTrace();
        //			}
        //		}


    }
}