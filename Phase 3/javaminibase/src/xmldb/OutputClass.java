package xmldb;

import global.IntervalType;

import java.util.ArrayList;

public class OutputClass{
    public OutputNode node;
    public ArrayList<OutputClass> children;

    public OutputClass(OutputNode node){
        this.node = node;
        this.children = new ArrayList<>();
    }

    public OutputNode getNode(){
        return this.node;
    }

    public void setChild(OutputClass child){

        this.children.add(child);

    }
}

class OutputNode {
    public IntervalType interval;
    public String tagName;
    public int space;

    //public OutputClass parent;

    public OutputNode(IntervalType interval, String tag, int space){
        this.interval = interval;
        this.tagName = tag;
        this.space = space;
        //this.parent = null;
    }

    public String getTagName(){
        return this.tagName;
    }
    public int getStartValue(){
        return this.interval.getStart();
    }

    public int getEndValue(){
        return this.interval.getEnd();
    }

    public boolean isChild(OutputNode q){
//        System.out.println(q.getStartValue() + " " +  this.getStartValue() + " " +  q.getEndValue() +  " " + this.getEndValue());
        if(q.getStartValue() < this.getStartValue() && q.getEndValue() > this.getEndValue()){
            return true;
        }
        return false;
    }

    public int getSpace(){
        return this.space;
    }

    public void setSpace(int space){
        this.space = space;
    }

//    public void setParent(OutputClass parent){
//        this.parent = parent;
//    }
//
//    public OutputClass getParent(){
//        return this.parent;
//    }

}
