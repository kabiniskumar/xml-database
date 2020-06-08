package intervaltree;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.OutputStream;

import btree.BTIndexPage;
import btree.BTLeafPage;
import btree.BTSortedPage;
import btree.ConstructPageException;
import btree.ConvertException;
import btree.DataClass;
import btree.IndexData;
import btree.IntegerKey;
import btree.IntervalKey;
import btree.IteratorException;
import btree.KeyClass;
import btree.KeyDataEntry;
import btree.KeyNotMatchException;
import btree.LeafData;
import btree.NodeNotMatchException;
import btree.NodeType;
import btree.StringKey;
import bufmgr.HashEntryNotFoundException;
import bufmgr.InvalidFrameNumberException;
import bufmgr.PageUnpinnedException;
import bufmgr.ReplacerException;
import diskmgr.Page;
import global.AttrType;
import global.Convert;
import global.GlobalConst;
import global.PageId;
import global.RID;
import global.SystemDefs;

public class IntervalT implements GlobalConst {
	IntervalT() {

	}

	public final static int keyCompare(KeyClass key1, KeyClass key2) throws KeyNotMatchException {
		if ((key1 instanceof IntervalKey) && (key2 instanceof IntervalKey)) {
			return (((IntervalKey) key1).getKey()).start - (((IntervalKey) key2).getKey()).start;
		} else {
			throw new KeyNotMatchException(null, "key types do not match");
		}
	}

	/**
	 * It gets the length of the key
	 * 
	 * @param key specify the key whose length will be calculated. Input parameter.
	 * @return return the length of the key
	 * @exception KeyNotMatchException key is neither StringKey nor IntegerKey
	 * @exception IOException          error from the lower layer
	 */
	protected final static int getKeyLength(KeyClass key) throws KeyNotMatchException, IOException {
		if (key instanceof StringKey) {
			OutputStream out = new ByteArrayOutputStream();
			DataOutputStream outstr = new DataOutputStream(out);
			outstr.writeUTF(((StringKey) key).getKey());
			return outstr.size();
		} else if (key instanceof IntegerKey)
			return 4;
		else if (key instanceof IntervalKey)
			return 12; // TODO verify
		else
			throw new KeyNotMatchException(null, "key types do not match");
	}

	/**
	 * It gets the length of the data
	 * 
	 * @param pageType NodeType.LEAF or NodeType.INDEX. Input parameter.
	 * @return return 8 if it is of NodeType.LEAF; return 4 if it is of
	 *         NodeType.INDEX.
	 * @exception NodeNotMatchException pageType is neither NodeType.LEAF nor
	 *                                  NodeType.INDEX.
	 */
	protected final static int getDataLength(short pageType) throws NodeNotMatchException {
		if (pageType == NodeType.LEAF)
			return 8;
		else if (pageType == NodeType.INDEX)
			return 4;
		else
			throw new NodeNotMatchException(null, "key types do not match");
	}

	/**
	 * It gets the length of the (key,data) pair in leaf or index page.
	 * 
	 * @param key      an object of KeyClass. Input parameter.
	 * @param pageType NodeType.LEAF or NodeType.INDEX. Input parameter.
	 * @return return the lenrth of the (key,data) pair.
	 * @exception KeyNotMatchException  key is neither StringKey nor IntegerKey
	 * @exception NodeNotMatchException pageType is neither NodeType.LEAF nor
	 *                                  NodeType.INDEX.
	 * @exception IOException           error from the lower layer
	 */
	protected final static int getKeyDataLength(KeyClass key, short pageType)
			throws KeyNotMatchException, NodeNotMatchException, IOException {
		return getKeyLength(key) + getDataLength(pageType);
	}

	/**
	 * For debug. Print the B+ tree structure out
	 * 
	 * @param header the head page of the B+ tree file
	 * @exception IOException                 error from the lower layer
	 * @exception ConstructPageException      error from BT page constructor
	 * @exception IteratorException           error from iterator
	 * @exception HashEntryNotFoundException  error from lower layer
	 * @exception InvalidFrameNumberException error from lower layer
	 * @exception PageUnpinnedException       error from lower layer
	 * @exception ReplacerException           error from lower layer
	 */
	public static void printIntervalTree(IntervalTreeHeaderPage header)
			throws IOException, ConstructPageException, IteratorException, HashEntryNotFoundException,
			InvalidFrameNumberException, PageUnpinnedException, ReplacerException {
		if (header.get_rootId().pid == INVALID_PAGE) {
			System.out.println("The Tree is Empty!!!");
			return;
		}

		System.out.println("");
		System.out.println("");
		System.out.println("");
		System.out.println("---------------The B+ Tree Structure---------------");

		System.out.println(1 + "     " + header.get_rootId());

		_printIntervalTree(header.get_rootId(), "     ", 1, header.get_keyType());

		System.out.println("--------------- End ---------------");
		System.out.println("");
		System.out.println("");
	}

	/**
	 * used for debug: to print a page out. The page is either BTIndexPage, or
	 * BTLeafPage.
	 *
	 * @param pageno  the number of page. Input parameter.
	 * @param keyType It specifies the type of key. It can be AttrType.attrString or
	 *                AttrType.attrInteger. Input parameter.
	 * @exception IOException                 error from the lower layer
	 * @exception IteratorException           error for iterator
	 * @exception ConstructPageException      error for BT page constructor
	 * @exception HashEntryNotFoundException  error from the lower layer
	 * @exception ReplacerException           error from the lower layer
	 * @exception PageUnpinnedException       error from the lower layer
	 * @exception InvalidFrameNumberException error from the lower layer
	 */

	// TODO: Modify implementation based on use case
	public static void printPage(PageId pageno, int keyType)
			throws IOException, IteratorException, ConstructPageException, HashEntryNotFoundException,
			ReplacerException, PageUnpinnedException, InvalidFrameNumberException {
		IntervalTSortedPage sortedPage = new IntervalTSortedPage(pageno, keyType);
		int i;
		i = 0;
		if (sortedPage.getType() == NodeType.INDEX) {
			IntervalTIndexPage indexPage = new IntervalTIndexPage((Page) sortedPage, keyType);
			System.out.println("");
			System.out.println("**************To Print an Index Page ********");
			System.out.println("Current Page ID: " + indexPage.getCurPage().pid);
			System.out.println("Left Link      : " + indexPage.getLeftLink().pid);

			RID rid = new RID();

			for (KeyDataEntry entry = indexPage.getFirst(rid); entry != null; entry = indexPage.getNext(rid)) {
				if (keyType == AttrType.attrInteger)
					System.out.println(
							i + " (key, pageId):   (" + (IntegerKey) entry.key + ",  " + (IndexData) entry.data + " )");
				if (keyType == AttrType.attrString)
					System.out.println(
							i + " (key, pageId):   (" + (StringKey) entry.key + ",  " + (IndexData) entry.data + " )");
				// TODO: CONFIRM WITH RONAK
				if (keyType == AttrType.attrInterval)
					System.out.println(
							i + " (key, pageId): (" + (IntervalKey) entry.key + ", " + (IndexData) entry.data + " )");

				i++;
			}

			System.out.println("************** END ********");
			System.out.println("");
		} else if (sortedPage.getType() == NodeType.LEAF) {
			IntervalTLeafPage leafPage = new IntervalTLeafPage((Page) sortedPage, keyType);
			System.out.println("");
			System.out.println("**************To Print an Leaf Page ********");
			System.out.println("Current Page ID: " + leafPage.getCurPage().pid);
			System.out.println("Left Link      : " + leafPage.getPrevPage().pid);
			System.out.println("Right Link     : " + leafPage.getNextPage().pid);

			RID rid = new RID();

			for (KeyDataEntry entry = leafPage.getFirst(rid); entry != null; entry = leafPage.getNext(rid)) {
				if (keyType == AttrType.attrInteger)
					System.out.println(i + " (key, [pageNo, slotNo]):   (" + (IntegerKey) entry.key + ",  "
							+ (LeafData) entry.data + " )");
				if (keyType == AttrType.attrString)
					System.out.println(i + " (key, [pageNo, slotNo]):   (" + (StringKey) entry.key + ",  "
							+ (LeafData) entry.data);
				// TODO: CONFIRM WITH RONAK
				if (keyType == AttrType.attrInterval)
					System.out.println(i + " (key, [pageNo, slotNo]):   (" + (StringKey) entry.key + ",  "
							+ (LeafData) entry.data);

				i++;
			}

			System.out.println("************** END ********");
			System.out.println("");
		} else {
			System.out.println("Sorry!!! This page is neither Index nor Leaf page.");
		}

		SystemDefs.JavabaseBM.unpinPage(pageno, true/* dirty */);
	}

	/**
	 * It gets an keyDataEntry from bytes array and position
	 * 
	 * @param from     It's a bytes array where KeyDataEntry will come from. Input
	 *                 parameter.
	 * @param offset   the offset in the bytes. Input parameter.
	 * @param keyType  It specifies the type of key. It can be AttrType.attrString
	 *                 or AttrType.attrInteger. Input parameter.
	 * @param nodeType It specifes NodeType.LEAF or NodeType.INDEX. Input parameter.
	 * @param length   The length of (key, data) in byte array "from". Input
	 *                 parameter.
	 * @return return a KeyDataEntry object
	 * @exception KeyNotMatchException  key is neither StringKey nor IntegerKey
	 * @exception NodeNotMatchException nodeType is neither NodeType.LEAF nor
	 *                                  NodeType.INDEX.
	 * @exception ConvertException      error from the lower layer
	 */
	protected final static KeyDataEntry getEntryFromBytes(byte[] from, int offset, int length, int keyType,
			short nodeType) throws KeyNotMatchException, NodeNotMatchException, ConvertException {
		KeyClass key;
		DataClass data;
		int n;
		try {

			if (nodeType == NodeType.INDEX) {
				n = 4;
				data = new IndexData(Convert.getIntValue(offset + length - 4, from));
			} else if (nodeType == NodeType.LEAF) {
				n = 8;
				RID rid = new RID();
				rid.slotNo = Convert.getIntValue(offset + length - 8, from);
				rid.pageNo = new PageId();
				rid.pageNo.pid = Convert.getIntValue(offset + length - 4, from);
				data = new LeafData(rid);
			} else
				throw new NodeNotMatchException(null, "node types do not match");

			if (keyType == AttrType.attrInteger) {
				key = new IntegerKey(new Integer(Convert.getIntValue(offset, from)));
			} else if (keyType == AttrType.attrString) {
				// System.out.println(" offset "+ offset + " " + length + " "+n);
				key = new StringKey(Convert.getStrValue(offset, from, length - n));
			} else if (keyType == AttrType.attrInterval) {
				// TODO: verify whether it reads all 12 bytes
				key = new IntervalKey(Convert.getIntervalValue(offset, from));
			} else
				throw new KeyNotMatchException(null, "key types do not match");

			return new KeyDataEntry(key, data);

		} catch (IOException e) {
			throw new ConvertException(e, "convert faile");
		}
	}

	// TODO: Modify implementation based on use case
	public static void printTreeUtilization(IntervalTreeHeaderPage header) {

	}

	// TODO: Modify implementation based on use case
	public static void printNonLeafTreeUtilization(IntervalTreeHeaderPage header) {

	}

	/**
	 * It convert a keyDataEntry to byte[].
	 * 
	 * @param entry specify the data entry. Input parameter.
	 * @return return a byte array with size equal to the size of (key,data).
	 * @exception KeyNotMatchException  entry.key is neither StringKey nor
	 *                                  IntegerKey
	 * @exception NodeNotMatchException entry.data is neither LeafData nor IndexData
	 * @exception ConvertException      error from the lower layer
	 */
	protected final static byte[] getBytesFromEntry(KeyDataEntry entry)
			throws KeyNotMatchException, NodeNotMatchException, ConvertException {
		byte[] data;
		int n, m;
		try {
			n = getKeyLength(entry.key); // should be 12 for interval key
			m = n;
			if (entry.data instanceof IndexData)
				n += 4;
			else if (entry.data instanceof LeafData)
				n += 8;

			data = new byte[n];

			if (entry.key instanceof IntegerKey) {
				Convert.setIntValue(((IntegerKey) entry.key).getKey().intValue(), 0, data);
			} else if (entry.key instanceof StringKey) {
				Convert.setStrValue(((StringKey) entry.key).getKey(), 0, data);
			} else if (entry.key instanceof IntervalKey) {
				Convert.setIntervalValue(((IntervalKey) entry.key).getKey(), 0, data);
			} else
				throw new KeyNotMatchException(null, "key types do not match");

			if (entry.data instanceof IndexData) {
				Convert.setIntValue(((IndexData) entry.data).getData().pid, m, data);
			} else if (entry.data instanceof LeafData) {
				Convert.setIntValue(((LeafData) entry.data).getData().slotNo, m, data);
				Convert.setIntValue(((LeafData) entry.data).getData().pageNo.pid, m + 4, data);
			} else
				throw new NodeNotMatchException(null, "node types do not match");
			return data;
		} catch (IOException e) {
			throw new ConvertException(e, "convert failed");
		}
	}
	
	private static void _printIntervalTree(PageId currentPageId, String prefix, int i, int keyType)
			throws IOException, ConstructPageException, IteratorException, HashEntryNotFoundException,
			InvalidFrameNumberException, PageUnpinnedException, ReplacerException {

		IntervalTSortedPage sortedPage = new IntervalTSortedPage(currentPageId, keyType);
		prefix = prefix + "       ";
		i++;
		if (sortedPage.getType() == NodeType.INDEX) {
			IntervalTIndexPage indexPage = new IntervalTIndexPage((Page) sortedPage, keyType);

			System.out.println(i + prefix + indexPage.getPrevPage());
			_printTree(indexPage.getPrevPage(), prefix, i, keyType);

			RID rid = new RID();
			for (KeyDataEntry entry = indexPage.getFirst(rid); entry != null; entry = indexPage.getNext(rid)) {
				System.out.println(i + prefix + (IndexData) entry.data);
				_printTree(((IndexData) entry.data).getData(), prefix, i, keyType);
			}
		}
		SystemDefs.JavabaseBM.unpinPage(currentPageId, true/* dirty */);
	}

	/**
	 * For debug. Print all leaf pages of the B+ tree out
	 * 
	 * @param header the head page of the B+ tree file
	 * @exception IOException                 error from the lower layer
	 * @exception ConstructPageException      error for BT page constructor
	 * @exception IteratorException           error from iterator
	 * @exception HashEntryNotFoundException  error from lower layer
	 * @exception InvalidFrameNumberException error from lower layer
	 * @exception PageUnpinnedException       error from lower layer
	 * @exception ReplacerException           error from lower layer
	 */
	public static void printAllLeafPages(IntervalTreeHeaderPage header)
			throws IOException, ConstructPageException, IteratorException, HashEntryNotFoundException,
			InvalidFrameNumberException, PageUnpinnedException, ReplacerException {
		if (header.get_rootId().pid == INVALID_PAGE) {
			System.out.println("The Tree is Empty!!!");
			return;
		}

		System.out.println("");
		System.out.println("");
		System.out.println("");
		System.out.println("---------------The B+ Tree Leaf Pages---------------");

		_printAllLeafPages(header.get_rootId(), header.get_keyType());

		System.out.println("");
		System.out.println("");
		System.out.println("------------- All Leaf Pages Have Been Printed --------");
		System.out.println("");
		System.out.println("");
	}

	private static void _printAllLeafPages(PageId currentPageId, int keyType)
			throws IOException, ConstructPageException, IteratorException, InvalidFrameNumberException,
			HashEntryNotFoundException, PageUnpinnedException, ReplacerException {

		IntervalTSortedPage sortedPage = new IntervalTSortedPage(currentPageId, keyType);

		if (sortedPage.getType() == NodeType.INDEX) {
			IntervalTIndexPage indexPage = new IntervalTIndexPage((Page) sortedPage, keyType);

			_printAllLeafPages(indexPage.getPrevPage(), keyType);

			RID rid = new RID();
			for (KeyDataEntry entry = indexPage.getFirst(rid); entry != null; entry = indexPage.getNext(rid)) {
				_printAllLeafPages(((IndexData) entry.data).getData(), keyType);
			}
		}

		if (sortedPage.getType() == NodeType.LEAF) {
			printPage(currentPageId, keyType);
		}

		SystemDefs.JavabaseBM.unpinPage(currentPageId, true/* dirty */);
	}

	private static void _printTree(PageId currentPageId, String prefix, int i, int keyType)
			throws IOException, ConstructPageException, IteratorException, HashEntryNotFoundException,
			InvalidFrameNumberException, PageUnpinnedException, ReplacerException {

		IntervalTSortedPage sortedPage = new IntervalTSortedPage(currentPageId, keyType);
		prefix = prefix + "       ";
		i++;
		if (sortedPage.getType() == NodeType.INDEX) {
			IntervalTIndexPage indexPage = new IntervalTIndexPage((Page) sortedPage, keyType);

			System.out.println(i + prefix + indexPage.getPrevPage());
			_printTree(indexPage.getPrevPage(), prefix, i, keyType);

			RID rid = new RID();
			for (KeyDataEntry entry = indexPage.getFirst(rid); entry != null; entry = indexPage.getNext(rid)) {
				System.out.println(i + prefix + (IndexData) entry.data);
				_printTree(((IndexData) entry.data).getData(), prefix, i, keyType);
			}
		}
		SystemDefs.JavabaseBM.unpinPage(currentPageId, true/* dirty */);
	}
}
