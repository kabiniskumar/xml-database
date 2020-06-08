package xmldb;
import java.util.Scanner;
import java.util.*;
import java.io.*;
public class XMLQueryParsing {

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

    public static ArrayList<ArrayList<String>> getReverseSortedRules(ArrayList<String> tags, ArrayList<ArrayList<String>> rules){

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
            //System.out.println(tags.get(i));
        }

        ((LinkedList<String>) q).add(rules.get(0).get(0));
        ((LinkedList<String>) q).add(rules.get(0).get(1));
        isTagVisited.set(tags.indexOf(rules.get(0).get(0)), 1);
        // isTagVisited.set(tags.indexOf(rules.get(0).get(1)), 1);


        while(q.size() != 0){
            String tag = q.remove();
            System.out.println(tag);
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

    public static void main(String[] args) throws IOException{

        try{
            File file = new File("/Users/ares/ASU/DBMSI/Project/Phase 2/dbmsiPhase2/javaminibase/src/xmldbTestXML/XMLQueryInput.txt");
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

            ArrayList<ArrayList<String>> sortedRules = getSortedRules(tags, rules);
            ArrayList<ArrayList<String>> reverseSortedRules = getReverseSortedRules(reversedtags, reversedRules);

            for(int i=0; i<reverseSortedRules.size(); i++){
                for(int k=0; k<reverseSortedRules.get(i).size(); k++){
                    System.out.print(reverseSortedRules.get(i).get(k) + " ");
                }
                System.out.println();
            }


        } catch (Exception e){
            e.printStackTrace();
        }


    }
}
