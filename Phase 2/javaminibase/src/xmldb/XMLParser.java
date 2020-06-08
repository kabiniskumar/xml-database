package xmldb;

import java.io.File;
import java.io.FileInputStream;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;


import org.w3c.dom.*;

public class XMLParser {
	
	public List<XMLInputTreeNode> listOfXMLObjects = new ArrayList<>();
	
	//static int depthOfTree = 1;
	public XMLParser() {
		
	}
	 
	public static InputTree tree = new InputTree();
	 
    public static void main(String[] args) throws Exception {
//        DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();
//        dbf.setValidating(false);
//        DocumentBuilder db = dbf.newDocumentBuilder();
//        Document doc = db.parse(new FileInputStream(new File("/home/ronak/DBMSi Project/Phase2/dbmsiPhase2/javaminibase/src/xmldbTestXML/xml_sample_data.xml")));
//        
//    	Node root = doc.getDocumentElement();
//    	    	
//    	build(root);
//    	
//    	BFS();
//    	
//    	preOrder(tree.root);
//    	System.out.println("---------------------------");
//    	System.out.println();
//    	BFS();
    }
    
     public static int intervalCounter = 1;
     public void preOrder(XMLInputTreeNode node) {
    	if (node == null)
    		return;
    	
    	node.interval.setStart(intervalCounter++);
    	for (XMLInputTreeNode child: node.children) {
    		preOrder(child);
    	}
    	node.interval.setEnd(intervalCounter++);
    }
    
    // pass the root node here
    public void clean(Node node) {
      NodeList childNodes = node.getChildNodes();
      
      for (int n = childNodes.getLength() - 1; n >= 0; n--) {
         Node child = childNodes.item(n);
         short nodeType = child.getNodeType();

         if (nodeType == Node.ELEMENT_NODE) {
            clean(child);
         } else if (nodeType == Node.TEXT_NODE) {
            String trimmedNodeVal = child.getNodeValue().trim();
            if (trimmedNodeVal.length() == 0) {
               node.removeChild(child);
            }
            else {
            	child.setNodeValue(trimmedNodeVal.substring(0, trimmedNodeVal.length() < 5 ? trimmedNodeVal.length() : 5));
            }
         }
         else if (nodeType == Node.COMMENT_NODE) {
            node.removeChild(child);
         }
      }
    }
    
    public void build(Node rootNode) {
    	tree.root = new XMLInputTreeNode(rootNode.getNodeName());
    	clean(rootNode);
    	buildUtil(rootNode, tree.root); 	
    }
    
    public void buildUtil(Node node, XMLInputTreeNode treeNode) {
    	NodeList list = node.getChildNodes(); // get child nodes for the treeNode
    	
    	for (int i=0, len = list.getLength(); i<len; i++) {
    		Node childNode = list.item(i); // get reference of the child node 
    		
    		XMLInputTreeNode treeChildNode = null;
    		
    		if (!childNode.getNodeName().equals("#text")) {
    			treeChildNode = new XMLInputTreeNode(childNode.getNodeName().substring(0, childNode.getNodeName().length() < 5 ? childNode.getNodeName().length() : 5)); // make a child node for our data structure
    		}
    		
    		/* 1. Handle atrributes */
    		if (childNode.getAttributes() != null) {
    			// add all attribute names as child nodes to this treeChildNode
    			// and the atrribute values as child nodes to the nodes of the atrribute names in the data structure
    			XMLInputTreeNode attributeNode, attributeChildNode;
    			String attrName, attrValue;
    			
    			NamedNodeMap node_attr = childNode.getAttributes();
                for (int j = 0; j < node_attr.getLength(); j++) {
                    Node attr = node_attr.item(j);
                    
                    attrName = attr.getNodeName().substring(0, attr.getNodeName().length() < 5 ? attr.getNodeName().length() : 5);
                    attrValue = attr.getNodeValue().substring(0, attr.getNodeValue().length() < 5 ? attr.getNodeValue().length() : 5);
                    
                    attributeNode = new XMLInputTreeNode(attrName);
                    attributeChildNode = new XMLInputTreeNode(attrValue);
                   
                    treeChildNode.children.add(attributeNode);
                    // int indexOfAttributeNode = treeChildNode.children.indexOf(attributeNode);
                    treeChildNode.children.get(treeChildNode.children.size()-1).children.add(attributeChildNode);                    
                }
    		}
    		
    		/* 2. Handle nodeValue() */
    		if (childNode.getNodeValue() != null && !childNode.getNodeValue().equals("#text")) {
    			XMLInputTreeNode nodeValueNode = new XMLInputTreeNode(childNode.getNodeValue());
    			treeNode.children.add(nodeValueNode);
    		}
    		
    		/* 3. Handle children */
//    		XMLInputTreeNode treeChildNode = new XMLInputTreeNode(childNode.getNodeName());
    		if (!childNode.getNodeName().equals("#text")) {
        		treeNode.children.add(treeChildNode);
        		buildUtil(childNode, treeChildNode);
    		}
    	}
    }
    
    public void BFSPrint() {
    	XMLInputTreeNode root = tree.root;
    	
    	Queue<XMLInputTreeNode> queue = new LinkedList<>();
    	queue.add(root);
    	
    	while (true) {
    		int nodeCount = queue.size();
    		
    		if (nodeCount == 0)
    			break;
    		
    		while (nodeCount > 0) {
    			XMLInputTreeNode node = queue.peek();
    			System.out.println("Tag Name: " + node.tagName);
    			System.out.println("Interval start value: " + node.interval.getStart());
    			System.out.println("Interval start end value: " + node.interval.getEnd());
    			System.out.println("Interval start level value: " + node.interval.getLevel());
    			
    			queue.remove();
    			if (node.children.size() != 0) {
    				for (XMLInputTreeNode childNode: node.children) {
    					queue.add(childNode);
    				}
    			}
    			nodeCount--;
    		}
    		
    		System.out.println();
    	}
    }
    
    
    public void BFSSetLevel() {
    	XMLInputTreeNode root = tree.root;
    	
    	Queue<XMLInputTreeNode> queue = new LinkedList<>();
    	queue.add(root);
    	
    	int level = 1;
    	while (true) {
    		int nodeCount = queue.size();
    		
    		if (nodeCount == 0)
    			break;
    		System.out.println("Level: " + level);
    		
    		while (nodeCount > 0) {
    			XMLInputTreeNode node = queue.peek();
//    			if(print) {
//	    			System.out.println("Tag Name: " + node.tagName);
//	    			System.out.println("Interval start value: " + node.interval.getStart());
//	    			System.out.println("Interval start end value: " + node.interval.getEnd());
//	    			System.out.println("Interval start level value: " + node.interval.getLevel());
//    			}
    			
    			node.interval.setLevel(level);
    			listOfXMLObjects.add(node);
    			
    			queue.remove();
    			if (node.children.size() != 0) {
    				for (XMLInputTreeNode childNode: node.children) {
    					queue.add(childNode);
    				}
    			}
    			nodeCount--;
    		}
    		
    		System.out.println();
    		level++;
    	}
    }
    
}
