package heap;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;

import global.AttrType;
import iterator.FileScan;
import org.w3c.dom.Document;
import org.w3c.dom.Node;
import org.xml.sax.SAXException;

import global.GlobalConst;
import global.RID;
import global.SystemDefs;
import heap.XMLRecord;

import tests.TestDriver;
import xmldb.InputTree;
import xmldb.XMLInputTreeNode;
import xmldb.XMLParser;

public class XMLRecordFetch extends TestDriver implements GlobalConst {

    static XMLParser xmlParser = new XMLParser();

    private final static boolean OK = true;
    private final static boolean FAIL = false;

    private int choice;
    private final static int reclen = 32;

    public XMLRecordFetch() {
        super("hptest");
        choice = 100;
    }

    public boolean runTests () {

        System.out.println ("\n" + "Running " + testName() + " tests...." + "\n");

        SystemDefs sysdef = new SystemDefs(dbpath,10000,10000,"Clock");

        // Kill anything that might be hanging around
        String newdbpath;
        String newlogpath;
        String remove_logcmd;
        String remove_dbcmd;
        String remove_cmd = "/bin/rm -rf ";

        newdbpath = dbpath;
        newlogpath = logpath;

        remove_logcmd = remove_cmd + logpath;
        remove_dbcmd = remove_cmd + dbpath;

        // Commands here is very machine dependent.  We assume
        // user are on UNIX system here
        try {
            Runtime.getRuntime().exec(remove_logcmd);
            Runtime.getRuntime().exec(remove_dbcmd);
        }
        catch (IOException e) {
            System.err.println ("IO error: "+e);
        }

        remove_logcmd = remove_cmd + newlogpath;
        remove_dbcmd = remove_cmd + newdbpath;

        try {
            Runtime.getRuntime().exec(remove_logcmd);
            Runtime.getRuntime().exec(remove_dbcmd);
        }
        catch (IOException e) {
            System.err.println ("IO error: "+e);
        }

        //Run the tests. Return type different from C++
        boolean _pass = runAllTests();

        //Clean up again
        try {
            Runtime.getRuntime().exec(remove_logcmd);
            Runtime.getRuntime().exec(remove_dbcmd);
        }
        catch (IOException e) {
            System.err.println ("IO error: "+e);
        }

        System.out.print ("\n" + "..." + testName() + " tests ");
        System.out.print (_pass==OK ? "completely successfully" : "failed");
        System.out.print (".\n\n");

        return _pass;
    }

    protected boolean test1() {
        System.out.println ("\n Test: Insert and scan fixed-size records\n");
        boolean status = OK;
        RID rid = new RID();
        Heapfile f = null;

        System.out.println ("  - Create a heap file\n");
        try {

            //FileScan file = new FileScan("ABXYZ", []);
        }
        catch (Exception e) {
            System.out.println("Unable to open heapfile");
        }
        try {
            f = new Heapfile("ABXYZ");
            System.out.println(f.getRecCnt());
        }
        catch (Exception e) {
            status = FAIL;
            System.err.println ("*** Could not create heap file\n");
            e.printStackTrace();
        }


        // In general, a sequential scan won't be in the same order as the
        // insertions.  However, we're inserting fixed-length records here, and
        // in this case the scan must return the insertion order.

        Scan scan = null;

        if ( status == OK ) {
            int len, i = 0;
            XMLRecord rec = null;
            Tuple tuple = new Tuple();

            boolean done = false;
            while (!done) {
                try {
                    System.out.println(rid);
                    tuple = scan.getNext(rid);
                    System.out.println("Reading Tuple");

                    if (tuple == null) {
                        done = true;
                        break;
                    }
                }
                catch (Exception e) {
                    status = FAIL;
                    e.printStackTrace();
                }

                if (status == OK && !done) {
                    try {
                        rec = new XMLRecord(tuple);
                    }
                    catch (Exception e) {
                        System.err.println (""+e);
                        e.printStackTrace();
                    }

                    len = tuple.getLength();
                    if ( len != reclen ) {
                        System.err.println ("*** Record " + i + " had unexpected length "
                                + len + "\n");
                        status = FAIL;
                        break;
                    }
                    else if ( SystemDefs.JavabaseBM.getNumUnpinnedBuffers()
                            == SystemDefs.JavabaseBM.getNumBuffers() ) {
                        System.err.println ("On record " + i + ":\n");
                        System.err.println ("*** The heap-file scan has not left its " +
                                "page pinned\n");
                        status = FAIL;
                        break;
                    }

                    System.out.println(rec);
                }
                ++i;
            }

            //If it gets here, then the scan should be completed
            if (status == OK) {
                if ( SystemDefs.JavabaseBM.getNumUnpinnedBuffers()
                        != SystemDefs.JavabaseBM.getNumBuffers() ) {
                    System.err.println ("*** The heap-file scan has not unpinned " +
                            "its page after finishing\n");
                    status = FAIL;
                }
                else if ( i != (xmlParser.listOfXMLObjects.size()) )
                {
                    status = FAIL;

                    System.err.println ("*** Scanned " + i + " records instead of "
                            + choice + "\n");
                }
            }
        }

        if ( status == OK )
            System.out.println ("  Test completed successfully.\n");

        return status;
    }

    public static void main(String[] args) throws FileNotFoundException, SAXException, IOException, ParserConfigurationException {

        DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();
        dbf.setValidating(false);
        DocumentBuilder db = dbf.newDocumentBuilder();
        Document doc = db.parse(new FileInputStream(new File("xmldbTestXML/sample_data.xml")));

        Node root = doc.getDocumentElement();

        XMLRecordFetch hd = new XMLRecordFetch();
        boolean dbstatus;

        dbstatus = hd.runTests();

        if (dbstatus != true) {
            System.err.println ("Error encountered during buffer manager tests:\n");
            Runtime.getRuntime().exit(1);
        }

        Runtime.getRuntime().exit(0);
    }
}