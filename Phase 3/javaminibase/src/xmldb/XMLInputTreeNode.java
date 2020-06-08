package xmldb;
import global.IntervalType;

import java.util.ArrayList;

public class XMLInputTreeNode {
	public String tagName;
    public IntervalType interval;
   

    public ArrayList<XMLInputTreeNode> children;

    public XMLInputTreeNode() {
        tagName = "";
        children = new ArrayList<>();
        interval = new IntervalType();
    }

    public XMLInputTreeNode(String name, int level){
        tagName = name;
        interval = new IntervalType();
        children = new ArrayList<>();
        
    }
    
    public XMLInputTreeNode(String name) {
    	tagName = name;
    	interval = new IntervalType();
    	interval.setStart(-1);
    	interval.setEnd(-1);
        children = new ArrayList<>();
        interval.setLevel(0);
    }
    
    public XMLInputTreeNode(String name, IntervalType _interval) {
    	tagName = name;
    	interval = _interval;
    }
    
    public String toString() {	
        return "TagName = " + tagName + " Start = " + interval.start + " End = " + interval.end+ " Level = " + interval.level;          
    }
}
