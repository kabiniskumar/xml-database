package xmldb;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;

import org.w3c.dom.Document;
import org.w3c.dom.Node;
import org.xml.sax.SAXException;

import diskmgr.PCounter;
import iterator.FileScanException;
import iterator.InvalidRelation;
import iterator.TupleUtilsException;

public class RunAllQueries {

	static XMLParser xmlParser = new XMLParser();
	public static void main(String[] args) throws FileNotFoundException, SAXException, IOException, ParserConfigurationException, InvalidRelation, TupleUtilsException, FileScanException {
		
		DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();
        dbf.setValidating(false);
        DocumentBuilder db = dbf.newDocumentBuilder();
        Document doc = db.parse(new FileInputStream(new File("/Users/akshayrao/git/dbmsiPhase2/javaminibase/src/xmldbTestXML/sample_data.xml")));
        String patternTreePath="/Users/akshayrao/git/dbmsiPhase2/javaminibase/src/xmldbTestXML/XMLQueryInput.txt";
        
        Node root = doc.getDocumentElement();

        xmlParser.build(root);

        // xmlParser.BFS();

        xmlParser.preOrder(xmlParser.tree.root);
//        System.out.println("---------------------------");
//        System.out.println();
        xmlParser.BFSSetLevel();

        // xmlParser.BFSPrint();
        PCounter.initialize();
        
		QueryPlan1 query1 = new QueryPlan1();
		query1.queryHandler(patternTreePath);
	}
}
