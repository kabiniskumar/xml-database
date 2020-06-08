package xmldb;

import bufmgr.*;
import diskmgr.InvalidPageNumberException;
import global.SystemDefs;
import iterator.FileScanException;
import iterator.InvalidRelation;
import iterator.TupleUtilsException;
import org.w3c.dom.Document;
import org.w3c.dom.Node;
import org.xml.sax.SAXException;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;

import static xmldb.XMLInsert.xmlParser;

public class Test {

    public static void main(String argv[]) throws diskmgr.DiskMgrException, diskmgr.FileIOException, InvalidPageNumberException, ParserConfigurationException, IOException, SAXException, TupleUtilsException, FileScanException, InvalidRelation, PageUnpinnedException, PagePinnedException, PageNotFoundException, BufMgrException, HashOperationException {


        // xmlParser.BFSPrint();

        XMLInsert xmlinsert = new XMLInsert();
//        String dbpath = "/Users/ares/Desktop/"+System.getProperty("user.name")+".minibase.jointestdb";
//        SystemDefs sysdef = new SystemDefs( dbpath, 0, 10000, "Clock" );
        

        //xmlinsert.insertData();
        xmlinsert.heapFileScan("*");

    }
}
