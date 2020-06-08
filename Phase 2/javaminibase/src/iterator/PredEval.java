package iterator;

import heap.*;
import global.*;
import java.io.*;

public class PredEval
{
  /**
   *predicate evaluate, according to the condition ConExpr, judge if 
   *the two tuple can join. if so, return true, otherwise false
   *@return true or false
   *@param p[] single select condition array
   *@param t1 compared tuple1
   *@param t2 compared tuple2
   *@param in1[] the attribute type corespond to the t1
   *@param in2[] the attribute type corespond to the t2
   *@exception IOException  some I/O error
   *@exception UnknowAttrType don't know the attribute type
   *@exception InvalidTupleSizeException size of tuple not valid
   *@exception InvalidTypeException type of tuple not valid
   *@exception FieldNumberOutOfBoundException field number exceeds limit
   *@exception PredEvalException exception from this method
   */
  public static boolean Eval(CondExpr p[], Tuple t1, Tuple t2, AttrType in1[], 
			     AttrType in2[])
    throws IOException,
	   UnknowAttrType,
	   InvalidTupleSizeException,
	   InvalidTypeException,
	   FieldNumberOutOfBoundException,
	   PredEvalException
    {	
      CondExpr temp_ptr;
      int       i = 0;
      Tuple    tuple1 = null, tuple2 = null;
      int      fld1, fld2;
      Tuple    value1 =   new Tuple();
		Tuple    value2 =   new Tuple();
      short[]     str_size = new short[1];
      AttrType[]  val_type = new AttrType[1];
      
      AttrType  comparison_type = new AttrType(AttrType.attrInteger);
      int       comp_res;
      boolean   op_res = false, row_res = false, col_res = true;
		Tuple t = new Tuple();
      
      if (p == null ) //|| p[0].operand2.string.equals("*")
	{
	  return true;
	}
      try {
		  while (i < p.length && p[i] != null) {
			  temp_ptr = p[i];
			  while (temp_ptr != null) {
				  val_type[0] = new AttrType(temp_ptr.type1.attrType);
				  fld1 = 1;
				  switch (temp_ptr.type1.attrType) {
					  case AttrType.attrInteger:
						  value1.setHdr((short) 1, val_type, null);
						  value1.setIntFld(1, temp_ptr.operand1.integer);
						  tuple1 = value1;
						  comparison_type.attrType = AttrType.attrInteger;
						  break;
					  case AttrType.attrReal:
						  value1.setHdr((short) 1, val_type, null);
						  value1.setFloFld(1, temp_ptr.operand1.real);
						  tuple1 = value1;
						  comparison_type.attrType = AttrType.attrReal;
						  break;
					  case AttrType.attrString:
						  str_size[0] = (short) (temp_ptr.operand1.string.length() + 1);
						  value1.setHdr((short) 1, val_type, str_size);
						  value1.setStrFld(1, temp_ptr.operand1.string);
						  tuple1 = value1;
						   t = value1;
						  //system.out.println("TUPLE 1 = " + tuple1.getStrFld(1));
						  tuple1.setStrFld(1, value1.getStrFld(1));
						  comparison_type.attrType = AttrType.attrString;
						  break;
					  case AttrType.attrSymbol:
						  fld1 = temp_ptr.operand1.symbol.offset;
						  if (temp_ptr.operand1.symbol.relation.key == RelSpec.outer) {
							  tuple1 = t1;
							  comparison_type.attrType = in1[fld1 - 1].attrType;
						  } else {
							  tuple1 = t2;
							  comparison_type.attrType = in2[fld1 - 1].attrType;
						  }
						  break;
					  case AttrType.attrInterval:
//						  str_size[0] = (short) 5;
						  value1.setHdr((short) 1, val_type, null);
						  value1.setIntervalFld(1, temp_ptr.operand1.interval);
						  tuple1 = value1;
						  comparison_type.attrType = AttrType.attrInterval;
						  break;
					  default:
						  break;
				  }
				  //system.out.println("TUPLE 1 = " + tuple1.getStrFld(1));

				  // Setup second argument for comparison.
				  val_type[0] = new AttrType(temp_ptr.type2.attrType);
				  fld2 = 1;
				  switch (temp_ptr.type2.attrType) {
					  case AttrType.attrInteger:
						  value2.setHdr((short) 1, val_type, null);
						  value2.setIntFld(1, temp_ptr.operand2.integer);
						  tuple2 = value2;
						  break;
					  case AttrType.attrReal:
						  value2.setHdr((short) 1, val_type, null);
						  value2.setFloFld(1, temp_ptr.operand2.real);
						  tuple2 = value2;
						  break;
					  case AttrType.attrString:
						  str_size[0] = (short) (temp_ptr.operand2.string.length() + 1);
						  value2.setHdr((short) 1, val_type, str_size);
						  value2.setStrFld(1, temp_ptr.operand2.string);
						  tuple2 = value2;
						  //system.out.println("TUPLE 1 = " + tuple1.getStrFld(1));
						  //system.out.println("TUPLE 2 = " + tuple2.getStrFld(1));
						  break;
					  case AttrType.attrSymbol:
						  fld2 = temp_ptr.operand2.symbol.offset;
						  if (temp_ptr.operand2.symbol.relation.key == RelSpec.outer)
							  tuple2 = t1;
						  else
							  tuple2 = t2;
						  break;
					  case AttrType.attrInterval:
//						  str_size[0] = (short) 5;
						  value2.setHdr((short) 1, val_type, null);
						  value2.setIntervalFld(1, temp_ptr.operand2.interval);
						  tuple2 = value2;
						  comparison_type.attrType = AttrType.attrInterval;
						  break;
					  default:
						  break;
				  }


				  // Got the arguments, now perform a comparison.
				  try {
					  //system.out.println("TUPLE 1 = " + tuple1.getStrFld(1));
					  //system.out.println("TUPLE 2 = " + tuple2.getStrFld(1));
					  comp_res = TupleUtils.CompareTupleWithTuple(comparison_type, tuple1, fld1, tuple2, fld2, true);


				  } catch (TupleUtilsException e) {
					  throw new PredEvalException(e, "TupleUtilsException is caught by PredEval.java");
				  }
				  op_res = false;

				  switch (temp_ptr.op.attrOperator) {
					  //TODO not sure about this - Will need to make changes later
					  case AttrOperator.aopEQ:
					  	//TODO - overlap case pending
						  if (temp_ptr.type1.attrType == AttrType.attrSymbol || temp_ptr.type1.attrType == AttrType.attrInterval) {

						  	if(temp_ptr.type1.attrType == AttrType.attrSymbol && temp_ptr.flag==1){
						  		if(comp_res==4)
						  			op_res = true;
						  		else
						  			op_res = false;
							}
						  	else if(temp_ptr.type1.attrType==AttrType.attrSymbol){
								if (comp_res == 0) op_res = true;
							}
						  	else{
								if (comp_res == 4) {
									op_res = true;
								} else {
									op_res = false;
								}
							}

						  }
						  else {
							  if (comp_res == 0) op_res = true;
						  }
						  break;
					  case AttrOperator.aopLT:
						  if (temp_ptr.type1.attrType == AttrType.attrSymbol && temp_ptr.flag==1){
							  if(comp_res == 2 || comp_res == 10){
								  op_res = true;
							  }
							  else{
								  op_res=false;
							  }
						  }
						  break;
					  case AttrOperator.aopGT:
						  if (temp_ptr.type1.attrType == AttrType.attrSymbol && temp_ptr.flag==1){
						  	if(comp_res == 1 || comp_res == 9){
						  		op_res = true;
							}
						  	else{
						  		op_res=false;
							}
						  }

//						  if (temp_ptr.type1.attrType == AttrType.attrInterval) {
//							  if (comp_res == 1) {
//								  op_res = true;
//								  System.out.println("It worked!");
//							  }
						  else {
							  //if (comp_res > 0)
							  	op_res = false;
						  }
						  break;
					  case AttrOperator.aopNE:
						  if (temp_ptr.type1.attrType == AttrType.attrInterval) {
						  	//TODO - This may not work
							  if (comp_res != 4) {
//								  temp_ptr.flag = 0;
								  op_res = true;
							  }
//							  } else if (comp_res == 0) { // ARAVIND TODO: VERIFY THAT IT MIGHT NOT EVER HAPPEN
//								  temp_ptr.flag = 1;
//								  op_res = true;
//							  }
							  else {
								  op_res = false;
							  }
						  }
//						  } else {
//							  if (comp_res != 0) op_res = true;
//						  }
						  break;
					  case AttrOperator.aopLE:
						  if (comp_res <= 0) op_res = true;
						  break;
					  case AttrOperator.aopGE:
						  if (comp_res >= 0) op_res = true;
						  break;
					  case AttrOperator.aopNOT:
						  if (comp_res != 0) op_res = true;
						  break;
					  case AttrOperator.aopPC:
						  if (comp_res == 9){ op_res = true;}
						  else{op_res = false;}
						  break;
					  case AttrOperator.aopCP:
						  if (comp_res == 10){ op_res = true;}
						  else{op_res = false;}
						  break;
					  default:
						  break;
				  }

				  row_res = row_res || op_res;
				  if (row_res == true)
					  break;                        // OR predicates satisfied.
				  temp_ptr = temp_ptr.next;
			  }
			  i++;

			  col_res = col_res && row_res;
			  if (col_res == false) {

				  return false;
			  }
			  row_res = false;                        // Starting next row.
		  }
	  } catch(Exception e){
      	e.printStackTrace();
	  }
      
      return true;
      
    }
}

