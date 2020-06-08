package xmldb;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Vector;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;

import org.w3c.dom.Document;
import org.w3c.dom.Node;
import org.xml.sax.SAXException;

import btree.BTreeFile;
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
import heap.Heapfile;
import heap.Scan;
import heap.Tuple;
import index.IndexScan;
import iterator.CondExpr;
import iterator.FileScanException;
import iterator.FldSpec;
import iterator.InvalidRelation;
import iterator.RelSpec;
import iterator.TupleUtilsException;

public class XMLTagIndex {

    private boolean OK = true;
    private boolean FAIL = false;
    boolean status = OK;
    private static short REC_LEN1 = 10;
    private Vector xmlTuples;
    static XMLParser xmlParser = new XMLParser();

    public void xmlDataInsert() throws HashOperationException, PageUnpinnedException, PagePinnedException, PageNotFoundException, BufMgrException, IOException {

        xmlTuples = new Vector();

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
        BTreeFile btf = null;
        try {
            btf = new BTreeFile("BTreeIndex", AttrType.attrString, 10, 1/*delete*/);
        } catch (Exception e) {
            status = FAIL;
            e.printStackTrace();
            Runtime.getRuntime().exit(1);
        }

        System.out.println("BTreeIndex created successfully.\n");

        rid = new RID();
        String key = null;
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
                key = t.getStrFld(2);
            } catch (Exception e) {
                status = FAIL;
                e.printStackTrace();
            }

            try {
                btf.insert(new StringKey(key), rid);
                //System.out.println("key = " + key);
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

        System.out.println("BTreeIndex file created successfully.\n");

        FldSpec[] projlist = new FldSpec[2];
        RelSpec rel = new RelSpec(RelSpec.outer);
        projlist[0] = new FldSpec(rel, 1);
        projlist[1] = new FldSpec(rel, 2);

        // start index scan
        IndexScan iscan = null;
        try {
            iscan = new IndexScan(new IndexType(IndexType.B_Index), "test.in", "BTreeIndex", Stypes, Ssizes, 2, 2, projlist, null, 2, true);
        } catch (Exception e) {
            status = FAIL;
            e.printStackTrace();
        }


        int count = 0;
        t = null;
        String outval = null;

        try {
            t = iscan.get_next();
        } catch (Exception e) {
            status = FAIL;
            e.printStackTrace();
        }

        boolean flag = true;

        while (t != null) {
            if (count >= numTuples) {
                System.err.println("Test1 -- OOPS! too many records");
                status = FAIL;
                flag = false;
                break;
            }

            try {
                outval = t.getStrFld(1);

            } catch (Exception e) {
                status = FAIL;
                e.printStackTrace();
            }

            count++;

            try {
                t = iscan.get_next();
            } catch (Exception e) {
                status = FAIL;
                e.printStackTrace();
            }
        }
        if (count < numTuples) {
            System.err.println("Test1 -- OOPS! too few records");
            status = FAIL;
        } else if (flag && status) {
            System.err.println("Test1 -- Index Scan OK");
        }

        // clean up
        try {
            iscan.close();
        } catch (Exception e) {
            status = FAIL;
            e.printStackTrace();
        }

    }

    public List<XMLInputTreeNode> indexTagSearch(String tag){

        System.out.println("------------------------------Index Tag Search------------------------------");
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
                status = FAIL;
                e.printStackTrace();
            }
        //Fetch the tuple size
            int size = t.size();
            t = new Tuple(size);

            try {
                t.setHdr((short) 2, Stypes, Ssizes);
            }
            catch (Exception e) {
                status = FAIL;
                e.printStackTrace();
            }


        //Set up the projections for the result
            FldSpec[] projlist = new FldSpec[2];
            RelSpec rel = new RelSpec(RelSpec.outer);
            projlist[0] = new FldSpec(rel, 1);
            projlist[1] = new FldSpec(rel, 2);

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
                iscan = new IndexScan(new IndexType(IndexType.B_Index), "test.in", "BTreeIndex", Stypes, Ssizes, 2, 2, projlist, expr, 2, false);
            }
            catch (Exception e) {
                status = FAIL;
                e.printStackTrace();
            }


        //Set up variables to hold results
            String stringResult = null;
            IntervalType intervalResult = new IntervalType();
            List<XMLInputTreeNode> tagResults = new ArrayList<XMLInputTreeNode>();

        //Get tuples one at a time
            try {
                t = iscan.get_next();
            }
            catch (Exception e) {
                status = FAIL;
                e.printStackTrace();
            }

        if (t == null) {
            System.err.println("Index tag search -- no record retrieved.");
            return null;
        }

        //Fetch and print one tuple at a time
            while( t!= null){

                try {
                    stringResult = t.getStrFld(2);
                    intervalResult = t.getIntervalFld(1);
                    
                    tagResults.add(new XMLInputTreeNode(stringResult, intervalResult));
                    

                }
                catch (Exception e) {
                    status = FAIL;
                    e.printStackTrace();
                }
                System.out.println("Tag name = " + stringResult + " Start = " + intervalResult.start + " End = " + intervalResult.end + " Level = " + intervalResult.level);

                try {
                    t = iscan.get_next();
                }
                catch (Exception e) {
                    status = FAIL;
                    e.printStackTrace();
                }
            }


        //Clean up
            try {
                iscan.close();
            }
            catch (Exception e) {
                status = FAIL;
                e.printStackTrace();
            }
            
            return tagResults;

    }

    public void indexRangeSearch(String startTag, String endTag){

        System.out.println("------------------------------Index Range Scan------------------------------");

        int numTuplesAttrs = 2;

        //Set up atrributes types
            AttrType[] Stypes = new AttrType[numTuplesAttrs];
            Stypes[0] = new AttrType (AttrType.attrInterval);
            Stypes[1] = new AttrType (AttrType.attrString);

        //Set up the size for String type attribute
            short[] Ssizes;
            Ssizes = new short [1];
            Ssizes[0] = 10;

        //Set up the projections for the result
            FldSpec[] projlist = new FldSpec[2];
            RelSpec rel = new RelSpec(RelSpec.outer);
            projlist[0] = new FldSpec(rel, 1);
            projlist[1] = new FldSpec(rel, 2);

        //Set up the condition expression for
            CondExpr[] expr = new CondExpr[3];
            expr[0] = new CondExpr();
            expr[0].op = new AttrOperator(AttrOperator.aopGE);
            expr[0].type1 = new AttrType(AttrType.attrSymbol);
            expr[0].type2 = new AttrType(AttrType.attrString);
            expr[0].operand1.symbol = new FldSpec(new RelSpec(RelSpec.outer), 2);
            expr[0].operand2.string = startTag;
            expr[0].next = null;
            
            expr[1] = new CondExpr();
            expr[1].op = new AttrOperator(AttrOperator.aopLE);
            expr[1].type1 = new AttrType(AttrType.attrSymbol);
            expr[1].type2 = new AttrType(AttrType.attrString);
            expr[1].operand1.symbol = new FldSpec(new RelSpec(RelSpec.outer), 2);
            expr[1].operand2.string = endTag;
            expr[1].next = null;
            expr[2] = null;

        //Start index scan
            IndexScan iscan = null;
            try {
                iscan = new IndexScan(new IndexType(IndexType.B_Index), "test.in", "BTreeIndex", Stypes, Ssizes, 2, 2, projlist, expr, 2, false);
            }
            catch (Exception e) {
                status = FAIL;
                e.printStackTrace();
            }


        //Fetch tuple
            Tuple t = null;

            try {
                t = iscan.get_next();
            }
            catch (Exception e) {
                status = FAIL;
                e.printStackTrace();
            }



        //Set up variables to hold results
            String stringResult = null;
            IntervalType intervalResult = new IntervalType();


        //Fetch and print one tuple at a time
            while (t != null) {

                try {
                    stringResult = t.getStrFld(2);
                    intervalResult = t.getIntervalFld(1);
                }
                catch (Exception e) {
                    e.printStackTrace();
                }
                System.out.println("Tag name = " + stringResult + " Start = " + intervalResult.start + " End = " + intervalResult.end + " Level = " + intervalResult.level);

                try {
                    t = iscan.get_next();
                }
                catch (Exception e) {
                    e.printStackTrace();
                }
            }

        // clean up
        try {
            iscan.close();
        }
        catch (Exception e) {
            e.printStackTrace();
        }
    }
    
    public static void setUpXMLDoc() throws ParserConfigurationException, FileNotFoundException, SAXException, IOException {
    	 DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();
         dbf.setValidating(false);
         DocumentBuilder db = dbf.newDocumentBuilder();
         Document doc = db.parse(new FileInputStream(new File("/Users/akshayrao/git/dbmsiPhase2/javaminibase/src/xmldbTestXML/sample_data.xml")));

         Node root = doc.getDocumentElement();

         xmlParser.build(root);
         xmlParser.preOrder(xmlParser.tree.root);
         xmlParser.BFSSetLevel();
    }




    public static void main(String argv[]) throws ParserConfigurationException, IOException, SAXException, TupleUtilsException, FileScanException, InvalidRelation, HashOperationException, PageUnpinnedException, PagePinnedException, PageNotFoundException, BufMgrException {


        String tag = "type";

        String startTag = "INTER";
        String endTag = "root";
        setUpXMLDoc();
        XMLTagIndex obj = new XMLTagIndex();
        obj.xmlDataInsert();
        obj.indexTagSearch(tag);
        obj.indexRangeSearch(startTag, endTag);



    }


}
