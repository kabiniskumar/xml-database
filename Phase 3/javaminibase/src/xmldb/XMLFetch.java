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

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;



class XMLFetch implements GlobalConst {

    private boolean OK = true;
    private boolean FAIL = false;
    private Vector xmlTuples;
    static XMLParser xmlParser = new XMLParser();

    /** Constructor
     */
    public XMLFetch() {

        //build XMLTuple table
        xmlTuples = new Vector();


        boolean status = OK;
     //   int numTuples = xmlParser.listOfXMLObjects.size();
        int numTuplesAttrs = 2;

    /*    for (int i = 0; (i < numTuples); i++) {
            //fixed length record
            XMLInputTreeNode node = xmlParser.listOfXMLObjects.get(i);
            xmlTuples.addElement(new XMLTuple(node.interval.start, node.interval.end, node.interval.level, node.tagName));
        }*/


        String dbpath = "/tmp/" + System.getProperty("user.name") + ".minibase.jointestdb";
        String logpath = "/tmp/" + System.getProperty("user.name") + ".joinlog";

     //   String remove_cmd = "/bin/rm -rf ";
     //   String remove_logcmd = remove_cmd + logpath;
//        String remove_dbcmd = remove_cmd + dbpath;
//        String remove_joincmd = remove_cmd + dbpath;

      /*  try {
            Runtime.getRuntime().exec(remove_logcmd);
            Runtime.getRuntime().exec(remove_dbcmd);
            Runtime.getRuntime().exec(remove_joincmd);
        } catch (IOException e) {
            System.err.println("" + e);
        }*/


        SystemDefs sysdef = new SystemDefs(dbpath, 10000, 10000, "Clock");

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
            System.out.println("String field = " + t.getStrFld(1));
        }
        catch (Exception e) {
            System.err.println("*** error in Tuple.setHdr() ***");
            status = FAIL;
            e.printStackTrace();
        }

     /*   for (int i=0; i<numTuples; i++) {
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
        }*/
        // System.out.println(xmlParser.listOfXMLObjects.size());

        if (status != OK) {
            //bail out
            System.err.println ("*** Error creating relation for sailors");
            Runtime.getRuntime().exit(1);
        }

    }


    public static void heapFileScan(String searchTagName) throws InvalidRelation, TupleUtilsException, FileScanException, IOException {

        // creating the sailors relation
        AttrType [] Stypes = new AttrType[2];
        Stypes[0] = new AttrType (AttrType.attrInterval);
        Stypes[1] = new AttrType (AttrType.attrString);

        //SOS
        short [] Ssizes = new short [1];
        Ssizes[0] = 5; //first elt. is 30

        FldSpec [] Sprojection = new FldSpec[2];
        Sprojection[0] = new FldSpec(new RelSpec(RelSpec.outer), 1);
        Sprojection[1] = new FldSpec(new RelSpec(RelSpec.outer), 2);

        FileScan am = null;
        try {
            CondExpr[] outFilter = new CondExpr[1];
            AttrType attrType = new AttrType(AttrType.attrString);
            CondExpr ce = new CondExpr(attrType, attrType);
            ce.type1 = new AttrType(AttrType.attrString);
            ce.type2 = new AttrType(AttrType.attrString);

            //ce.operand2.string = "root";
            outFilter[0] = ce;

            am  = new FileScan("test.in", Stypes, Ssizes,
                    (short)2, (short)2,
                    Sprojection, outFilter);
        }
        catch (Exception e) {
            e.printStackTrace();
        }

        boolean done = false;
        try{
            while(!done){
                Tuple t = am.get_next(searchTagName);
                if(t == null){
                    done = true;
                    break;
                }
                byte[] tupleArray = t.getTupleByteArray();
                IntervalType i = t.getIntervalFld(1);
                String tagname = t.getStrFld(2);
                XMLRecord rec = new XMLRecord(t);
                System.out.println( "Start = " + i.start + " End = " +  i.end + " Level = " + i.level + " Tagname = " + tagname);

            }
        } catch(Exception e){
            e.printStackTrace();
        }

    }


    public static void main(String argv[]) throws ParserConfigurationException, IOException, SAXException, TupleUtilsException, FileScanException, InvalidRelation {

        /*DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();
        dbf.setValidating(false);
        DocumentBuilder db = dbf.newDocumentBuilder();
        Document doc = db.parse(new FileInputStream(new File("xmldbTestXML/sample_data.xml")));

        Node root = doc.getDocumentElement();

        xmlParser.build(root);

        // xmlParser.BFS();

        xmlParser.preOrder(xmlParser.tree.root);
        System.out.println("---------------------------");
        System.out.println();
        xmlParser.BFSSetLevel();

        // xmlParser.BFSPrint();*/

        //XMLInsert xmlinsert = new XMLInsert();
        XMLFetch obj = new XMLFetch();
        obj.heapFileScan("*");

    }
}
