package btree;
/**  nodetype:  define the type of node in B++ tree
 */
public class NodeType {
   /**
    * Define constant INDEX
    */
   public static final short INDEX=11;
   /**
    * Define constant LEAF
    */
   public static final short LEAF=12;
   
   /**Define a type for header page of BTFile*/
   public static final short BTHEAD=13;

   public static final short INTERVALTHEAD=14;

//   public static final short NODE = 15;

   public static final short INTERVAL_INDEX_NODE = 16;

   public static final short INTERVAL_LEAF_NODE = 17;

} 
