package iterator;


import heap.*;
import global.*;
import bufmgr.*;
import diskmgr.*;
import index.*;
import java.lang.*;
import java.io.*;
/**
 *
 *  This file contains an implementation of the nested loops join
 *  algorithm as described in the Shapiro paper.
 *  The algorithm is extremely simple:
 *
 *      foreach tuple r in R do
 *          foreach tuple s in S do
 *              if (ri == sj) then add (r, s) to the result.
 */
// TODO: flag variables usage for nested loop joins
public class CustomNestedLoopsJoins extends Iterator
{
    private AttrType      _in1[],  _in2[];
    private   int        in1_len, in2_len;
    private   Iterator  outer;
    private   Iterator      inner;
    private   short t2_str_sizescopy[];
    private   CondExpr OutputFilter[];
    private   CondExpr RightFilter[];
    private   int        n_buf_pgs;        // # of buffer pages available.
    private   boolean        done,         // Is the join complete
            get_from_outer;                 // if TRUE, a tuple is got from outer
    private   Tuple     outer_tuple, inner_tuple;
    private   Tuple     Jtuple;           // Joined tuple
    private   FldSpec   perm_mat[];
    private   int        nOutFlds;
    private   Heapfile  hf;
    IndexType i_index;
    private int joinCol;
    private int cond;



    /**constructor
     *Initialize the two relations which are joined, including relation type,
     *@param in1  Array containing field types of R.
     *@param len_in1  # of columns in R.
     *@param t1_str_sizes shows the length of the string fields.
     *@param in2  Array containing field types of S
     *@param len_in2  # of columns in S
     *@param  t2_str_sizes shows the length of the string fields.
     *@param amt_of_mem  IN PAGES
     *@param am1  access method for left i/p to join
     *@param relationName  access hfapfile for right i/p to join
     *@param outFilter   select expressions
     *@param rightFilter reference to filter applied on right i/p
     *@param proj_list shows what input fields go where in the output tuple
     *@param n_out_flds number of outer relation fileds
     *@exception IOException some I/O fault
     *@exception NestedLoopException exception from this class
     */
    public CustomNestedLoopsJoins( AttrType    in1[],
                             int     len_in1,
                             short   t1_str_sizes[],
                             AttrType    in2[],
                             int     len_in2,
                             short   t2_str_sizes[],
                             int     amt_of_mem,
                             Iterator     am1,
                             Iterator     am2,
                             CondExpr outFilter[],
                             CondExpr rightFilter[],
                             FldSpec   proj_list[],
                             int        n_out_flds,
                                   int joinCol,
                                   int condition
    ) throws IOException,NestedLoopException
    {

        _in1 = new AttrType[in1.length];
        _in2 = new AttrType[in2.length];
        System.arraycopy(in1,0,_in1,0,in1.length);
        System.arraycopy(in2,0,_in2,0,in2.length);
        in1_len = len_in1;
        in2_len = len_in2;


        outer = am1;
        t2_str_sizescopy =  t2_str_sizes;
        inner_tuple = new Tuple();
        Jtuple = new Tuple();
        OutputFilter = outFilter;
        RightFilter  = rightFilter;

        n_buf_pgs    = amt_of_mem;
        inner = null;
        done  = false;
        get_from_outer = true;

        AttrType[] Jtypes = new AttrType[n_out_flds];
        AttrType[] Itypes = new AttrType[n_out_flds];
        short[]    t_size;

        perm_mat = proj_list;
        nOutFlds = n_out_flds;
        try {
            t_size = TupleUtils.setup_op_tuple(Jtuple, Jtypes,
                    in1, len_in1, in2, len_in2,
                    t1_str_sizes, t2_str_sizes,
                    proj_list, nOutFlds);
        }catch (TupleUtilsException e){
            throw new NestedLoopException(e,"TupleUtilsException is caught by NestedLoopsJoins.java");
        }

        short [] Tsizes = new short[in2_len/2];
        for(int i=0;i<in2_len/2;i++){
            Tsizes[i] = 10;
        }
        FldSpec[] projlist = new FldSpec[2];
        projlist[0] = new FldSpec(new RelSpec(RelSpec.outer), 1);
        projlist[1] = new FldSpec(new RelSpec(RelSpec.outer), 2);


        inner = am2;
        this.joinCol = joinCol;
        this.cond = condition;

//        try {
//            hf = new Heapfile(relationName);
//
////        }
//        catch(Exception e) {
//            throw new NestedLoopException(e, "Create new heapfile failed.");
//        }
    }



    /**
     *@return The joined tuple is returned
     *@exception IOException I/O errors
     *@exception JoinsException some join exception
     *@exception IndexException exception from super class
     *@exception InvalidTupleSizeException invalid tuple size
     *@exception InvalidTypeException tuple type not valid
     *@exception PageNotReadException exception from lower layer
     *@exception TupleUtilsException exception from using tuple utilities
     *@exception PredEvalException exception from PredEval class
     *@exception SortException sort exception
     *@exception LowMemException memory error
     *@exception UnknowAttrType attribute type unknown
     *@exception UnknownKeyTypeException key type unknown
     *@exception Exception other exceptions

     */
    public Tuple get_next()
            throws IOException,
            JoinsException ,
            IndexException,
            InvalidTupleSizeException,
            InvalidTypeException,
            PageNotReadException,
            TupleUtilsException,
            PredEvalException,
            SortException,
            LowMemException,
            UnknowAttrType,
            UnknownKeyTypeException,
            Exception
    {
        // This is a DUMBEST form of a join, not making use of any key information...

        FldSpec[] projlist = new FldSpec[2];
        projlist[0] = new FldSpec(new RelSpec(RelSpec.outer), 1);
        projlist[1] = new FldSpec(new RelSpec(RelSpec.outer), 2);

        AttrType[] Stypes = new AttrType[2];
        Stypes[0] = new AttrType(AttrType.attrInterval);
        Stypes[1] = new AttrType(AttrType.attrString);


        short [] Rsizes = new short[in2_len/2];
        for(int i=0;i<in2_len/2;i++){
            Rsizes[i] = 10;
        }


        if (done)
            return null;

        do
        {
            // If get_from_outer is true, Get a tuple from the outer, delete
            // an existing scan on the file, and reopen a new scan on the file.
            // If a get_next on the outer returns DONE?, then the nested loops
            //join is done too.

            if (get_from_outer == true)
            {
                get_from_outer = false;
                if (inner != null)     // If this not the first time,
                {
                    // close scan
                    inner.close();
                    //inner = null;
                }

                try {
//                    inner = hf.openScan();
//                    try {
//                        inner = new IndexScan( i_index, "test.in",
//                                "IntervalTreeIndex", _in2, Tsizes, 2, 2,
//                                projlist, null, 1, false);
//                    }
//
//                    catch (Exception e) {
//                        System.err.println ("*** Error creating scan for Index scan");
//                        System.err.println (""+e);
//                        Runtime.getRuntime().exit(1);
//                    }

                    if ((outer_tuple=outer.get_next()) == null)
                    {
                        done = true;
                        if (inner != null)
                        {
                            //inner.closescan();
                            inner.close();
                            inner = null;
                        }

                        return null;
                    }

                    CondExpr[] select = new CondExpr[3];

                    select[0] = new CondExpr();

                    select[0].type1 = new AttrType(AttrType.attrSymbol);
                    select[0].type2 = new AttrType(AttrType.attrInterval);
                    select[0].operand1.symbol = new FldSpec(new RelSpec(RelSpec.outer), 1);
//                    System.out.println("Cond: "+cond);
                    if(cond==1){
                        select[0].op = new AttrOperator(AttrOperator.aopAD);
                    }else{
                        select[0].op = new AttrOperator(AttrOperator.aopCP);

                    }
                    select[0].operand2.interval = outer_tuple.getIntervalFld(joinCol);

                    select[0].flag = 1;
                    select[0].next = select[1];

                    select[1] = null;
                    select[2] = null;

//                    select = null;
                    IndexScan iscan = null;
                    try {
                        iscan = new IndexScan(new IndexType(IndexType.I_Index), "test.in", "IntervalTreeIndex", Stypes, Rsizes, 2, 2, projlist, select, 1, false);
                    } catch (Exception e) {

                        e.printStackTrace();
                    }
                    inner = iscan;
                }
                catch(Exception e){
                    throw new NestedLoopException(e, "openScan failed");
                }

            }  // ENDS: if (get_from_outer == TRUE)


            // The next step is to get a tuple from the inner,
            // while the inner is not completely scanned && there
            // is no match (with pred),get a tuple from the inner.


            RID rid = new RID();
            while ((inner_tuple = inner.get_next()) != null)
            {
                inner_tuple.setHdr((short)in2_len, _in2,t2_str_sizescopy);
                if (PredEval.Eval(RightFilter, inner_tuple, null, _in2, null) == true)
                {
                    if (PredEval.Eval(OutputFilter, outer_tuple, inner_tuple, _in1, _in2) == true)
                    {
                        // Apply a projection on the outer and inner tuples.
                        Projection.Join(outer_tuple, _in1,
                                inner_tuple, _in2,
                                Jtuple, perm_mat, nOutFlds);
                        return Jtuple;
                    }
                }
            }

            // There has been no match. (otherwise, we would have
            //returned from t//he while loop. Hence, inner is
            //exhausted, => set get_from_outer = TRUE, go to top of loop

            get_from_outer = true; // Loop back to top and get next outer tuple.
        } while (true);
    }

    /**
     * implement the abstract method close() from super class Iterator
     *to finish cleaning up
     *@exception IOException I/O error from lower layers
     *@exception JoinsException join error from lower layers
     *@exception IndexException index access error
     */
    public void close() throws JoinsException, IOException,IndexException
    {
        if (!closeFlag) {

            try {
                outer.close();
            }catch (Exception e) {
                throw new JoinsException(e, "NestedLoopsJoin.java: error in closing iterator.");
            }
            closeFlag = true;
        }
    }
}





