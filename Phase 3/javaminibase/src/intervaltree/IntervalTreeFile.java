package intervaltree;

import java.io.DataOutputStream;
import java.io.FileOutputStream;
import java.io.IOException;

import intervaltree.IntervalT;
import intervaltree.IntervalTreeHeaderPage;
import btree.AddFileEntryException;
import btree.ConstructPageException;
import btree.ConvertException;
import btree.DeleteFashionException;
import btree.DeleteFileEntryException;
import btree.DeleteRecException;
import btree.FreePageException;
import btree.GetFileEntryException;
import btree.IndexData;
import btree.IndexFile;
import btree.IndexFullDeleteException;
import btree.IndexInsertRecException;
import btree.IndexSearchException;
import btree.InsertException;
import btree.InsertRecException;
import btree.IntegerKey;
import btree.IntervalKey;
import btree.IteratorException;
import btree.KeyClass;
import btree.KeyDataEntry;
import btree.KeyNotMatchException;
import btree.KeyTooLongException;
import btree.LeafData;
import btree.LeafDeleteException;
import btree.LeafInsertRecException;
import btree.LeafRedistributeException;
import btree.NodeNotMatchException;
import btree.NodeType;
import btree.PinPageException;
import btree.RecordNotFoundException;
import btree.RedistributeException;
import btree.StringKey;
import btree.UnpinPageException;
import bufmgr.HashEntryNotFoundException;
import bufmgr.InvalidFrameNumberException;
import bufmgr.PageUnpinnedException;
import bufmgr.ReplacerException;
import diskmgr.Page;
import global.AttrType;
import global.GlobalConst;
import global.PageId;
import global.RID;
import global.SystemDefs;
import heap.HFPage;

public class IntervalTreeFile extends IndexFile implements GlobalConst {

	private final static int MAGIC0 = 1989;

	private final static String lineSep = System.getProperty("line.separator");

	private static FileOutputStream fos;
	private static DataOutputStream trace;

	private IntervalTreeHeaderPage headerPage;
	private PageId headerPageId;
	private String dbname;

	public IntervalTreeFile(String filename) throws ConstructPageException, GetFileEntryException {
		headerPageId = get_file_entry(filename);

		headerPage = new IntervalTreeHeaderPage(headerPageId);
		dbname = new String(filename);
	}
	
	public IntervalTreeFile(String filename, int keytype, int keysize, int delete_fashion)
			throws GetFileEntryException, ConstructPageException, IOException, AddFileEntryException {

		headerPageId = get_file_entry(filename);
		if (headerPageId == null) // file not exist
		{
			headerPage = new IntervalTreeHeaderPage(); // initialize a header page
			headerPageId = headerPage.getPageId(); // get the page ID
			add_file_entry(filename, headerPageId);
			headerPage.set_magic0(MAGIC0);
			headerPage.set_rootId(new PageId(INVALID_PAGE));
			headerPage.set_keyType((short) keytype);
			headerPage.set_maxKeySize(keysize);
			headerPage.set_deleteFashion(delete_fashion);
			headerPage.setType(NodeType.INTERVALTHEAD);
		} else {
			headerPage = new IntervalTreeHeaderPage(headerPageId);
		}

		dbname = new String(filename);
	}

	public IntervalTreeFile(String filename, int delete_fashion) {

	}

	private void freePage(PageId pageno) throws FreePageException {
		try {
			SystemDefs.JavabaseBM.freePage(pageno);
		} catch (Exception e) {
			e.printStackTrace();
			throw new FreePageException(e, "");
		}

	}
	
	private PageId get_file_entry(String filename) throws GetFileEntryException {
		try {
			return SystemDefs.JavabaseDB.get_file_entry(filename);
		} catch (Exception e) {
			e.printStackTrace();
			throw new GetFileEntryException(e, "");
		}
	}

	private void add_file_entry(String fileName, PageId pageno) throws AddFileEntryException {
		try {
			SystemDefs.JavabaseDB.add_file_entry(fileName, pageno);
		} catch (Exception e) {
			e.printStackTrace();
			throw new AddFileEntryException(e, "");
		}
	}
	
	/*
	 * findRunStart. Status BTreeFile::findRunStart (const void lo_key, RID
	 * *pstartrid)
	 *
	 * find left-most occurrence of `lo_key', going all the way left if lo_key is
	 * null.
	 *
	 * Starting record returned in *pstartrid, on page *pppage, which is pinned.
	 *
	 * Since we allow duplicates, this must "go left" as described in the text (for
	 * the search algorithm).
	 * 
	 * @param lo_key find left-most occurrence of `lo_key', going all the way left
	 * if lo_key is null.
	 * 
	 * @param startrid it will reurn the first rid =< lo_key
	 * 
	 * @return return a BTLeafPage instance which is pinned. null if no key was
	 * found.
	 */

	IntervalTLeafPage findRunStart(KeyClass lo_key, RID startrid) throws IOException, IteratorException,
			KeyNotMatchException, ConstructPageException, PinPageException, UnpinPageException {
		IntervalTLeafPage pageLeaf;
		IntervalTIndexPage pageIndex;
		Page page;
		IntervalTSortedPage sortPage;
		PageId pageno;
		PageId curpageno = null; // iterator
		PageId prevpageno;
		PageId nextpageno;
		RID curRid;
		KeyDataEntry curEntry;

		pageno = headerPage.get_rootId();

		if (pageno.pid == INVALID_PAGE) { // no pages in the BTREE
			pageLeaf = null; // should be handled by
			// startrid =INVALID_PAGEID ; // the caller
			return pageLeaf;
		}

		page = pinPage(pageno);
		sortPage = new IntervalTSortedPage(page, headerPage.get_keyType());

		if (trace != null) {
			trace.writeBytes("VISIT node " + pageno + lineSep);
			trace.flush();
		}

		// ASSERTION
		// - pageno and sortPage is the root of the btree
		// - pageno and sortPage valid and pinned

		while (sortPage.getType() == NodeType.INDEX) {
			pageIndex = new IntervalTIndexPage(page, headerPage.get_keyType());
			prevpageno = pageIndex.getPrevPage();
			curEntry = pageIndex.getFirst(startrid);
			while (curEntry != null && lo_key != null && IntervalT.keyCompare(curEntry.key, lo_key) < 0) {

				prevpageno = ((IndexData) curEntry.data).getData();
				curEntry = pageIndex.getNext(startrid);
			}

			unpinPage(pageno);

			pageno = prevpageno;
			page = pinPage(pageno);
			sortPage = new IntervalTSortedPage(page, headerPage.get_keyType());

			if (trace != null) {
				trace.writeBytes("VISIT node " + pageno + lineSep);
				trace.flush();
			}

		}

		pageLeaf = new IntervalTLeafPage(page, headerPage.get_keyType());

		curEntry = pageLeaf.getFirst(startrid);
		while (curEntry == null) {
			// skip empty leaf pages off to left
			nextpageno = pageLeaf.getNextPage();
			unpinPage(pageno);
			if (nextpageno.pid == INVALID_PAGE) {
				// oops, no more records, so set this scan to indicate this.
				return null;
			}

			pageno = nextpageno;
			pageLeaf = new IntervalTLeafPage(pinPage(pageno), headerPage.get_keyType());
			curEntry = pageLeaf.getFirst(startrid);
		}

		// ASSERTIONS:
		// - curkey, curRid: contain the first record on the
		// current leaf page (curkey its key, cur
		// - pageLeaf, pageno valid and pinned

		if (lo_key == null) {
			return pageLeaf;
			// note that pageno/pageLeaf is still pinned;
			// scan will unpin it when done
		}

		while (IntervalT.keyCompare(curEntry.key, lo_key) < 0) {
			curEntry = pageLeaf.getNext(startrid);
			while (curEntry == null) { // have to go right
				nextpageno = pageLeaf.getNextPage();
				unpinPage(pageno);

				if (nextpageno.pid == INVALID_PAGE) {
					return null;
				}

				pageno = nextpageno;
				pageLeaf = new IntervalTLeafPage(pinPage(pageno), headerPage.get_keyType());

				curEntry = pageLeaf.getFirst(startrid);
			}
		}

		return pageLeaf;
	}

	private Page pinPage(PageId pageno) throws PinPageException {
		try {
			Page page = new Page();
			SystemDefs.JavabaseBM.pinPage(pageno, page, false/* Rdisk */);
			return page;
		} catch (Exception e) {
			e.printStackTrace();
			throw new PinPageException(e, "");
		}
	}

	private void unpinPage(PageId pageno) throws UnpinPageException {
		try {
			SystemDefs.JavabaseBM.unpinPage(pageno, false /* = not DIRTY */);
		} catch (Exception e) {
			e.printStackTrace();
			throw new UnpinPageException(e, "");
		}
	}

	private void unpinPage(PageId pageno, boolean dirty) throws UnpinPageException {
		try {
			SystemDefs.JavabaseBM.unpinPage(pageno, dirty);
		} catch (Exception e) {
			e.printStackTrace();
			throw new UnpinPageException(e, "");
		}
	}

	private void delete_file_entry(String filename) throws DeleteFileEntryException {
		try {
			SystemDefs.JavabaseDB.delete_file_entry(filename);
		} catch (Exception e) {
			e.printStackTrace();
			throw new DeleteFileEntryException(e, "");
		}
	}

	/**
	 * Destroy entire B+ tree file.
	 * 
	 * @exception IOException              error from the lower layer
	 * @exception IteratorException        iterator error
	 * @exception UnpinPageException       error when unpin a page
	 * @exception FreePageException        error when free a page
	 * @exception DeleteFileEntryException failed when delete a file from DM
	 * @exception ConstructPageException   error in BT page constructor
	 * @exception PinPageException         failed when pin a page
	 */
	public void destroyFile() throws IOException, IteratorException, UnpinPageException, FreePageException,
			DeleteFileEntryException, ConstructPageException, PinPageException {
		if (headerPage != null) {
			PageId pgId = headerPage.get_rootId();
			if (pgId.pid != INVALID_PAGE)
				_destroyFile(pgId);
			unpinPage(headerPageId);
			freePage(headerPageId);
			delete_file_entry(dbname);
			headerPage = null;
		}
	}

	private void _destroyFile(PageId pageno) throws IOException, IteratorException, PinPageException,
			ConstructPageException, UnpinPageException, FreePageException {

		IntervalTSortedPage sortedPage;
		Page page = pinPage(pageno);
		sortedPage = new IntervalTSortedPage(page, headerPage.get_keyType());

		if (sortedPage.getType() == NodeType.INDEX) {
			IntervalTIndexPage indexPage = new IntervalTIndexPage(page, headerPage.get_keyType());
			RID rid = new RID();
			PageId childId;
			KeyDataEntry entry;
			for (entry = indexPage.getFirst(rid); entry != null; entry = indexPage.getNext(rid)) {
				childId = ((IndexData) (entry.data)).getData();
				_destroyFile(childId);
			}
		} else { // IntervalTLeafPage
			unpinPage(pageno);
			freePage(pageno);
		}
	}

	/**
	 * Stop tracing. And close trace file.
	 * 
	 * @exception IOException error from the lower layer
	 */
	public static void destroyTrace() throws IOException {
		if (trace != null)
			trace.close();
		if (fos != null)
			fos.close();
		fos = null;
		trace = null;
	}

//	/**
//	 * insert record with the given key and rid
//	 * 
//	 * @param key the key of the record. Input parameter.
//	 * @param rid the rid of the record. Input parameter.
//	 * @exception KeyTooLongException     key size exceeds the max keysize.
//	 * @exception KeyNotMatchException    key is not integer key nor string key
//	 * @exception IOException             error from the lower layer
//	 * @exception LeafInsertRecException  insert error in leaf page
//	 * @exception IndexInsertRecException insert error in index page
//	 * @exception ConstructPageException  error in BT page constructor
//	 * @exception UnpinPageException      error when unpin a page
//	 * @exception PinPageException        error when pin a page
//	 * @exception NodeNotMatchException   node not match index page nor leaf page
//	 * @exception ConvertException        error when convert between revord and byte
//	 *                                    array
//	 * @exception DeleteRecException      error when delete in index page
//	 * @exception IndexSearchException    error when search
//	 * @exception IteratorException       iterator error
//	 * @exception LeafDeleteException     error when delete in leaf page
//	 * @exception InsertException         error when insert in index page
//	 */
//	public void insert(KeyClass key, RID rid) throws KeyTooLongException, KeyNotMatchException, LeafInsertRecException,
//			IndexInsertRecException, ConstructPageException, UnpinPageException, PinPageException,
//			NodeNotMatchException, ConvertException, DeleteRecException, IndexSearchException, IteratorException,
//			LeafDeleteException, InsertException, IOException {
//		
//		KeyDataEntry newRootEntry;
//
////		if (IntervalT.getKeyLength(key) > headerPage.get_maxKeySize())
////			throw new KeyTooLongException(null, "");
////
////		if (key instanceof StringKey) {
////			if (headerPage.get_keyType() != AttrType.attrString) {
////				throw new KeyNotMatchException(null, "");
////			}
////		} else if (key instanceof IntegerKey) {
////			if (headerPage.get_keyType() != AttrType.attrInteger) {
////				throw new KeyNotMatchException(null, "");
////			}
////		}
//		if (key instanceof IntervalKey) {
//			if (headerPage.get_keyType() != AttrType.attrInterval) {
//				throw new KeyNotMatchException(null, "");
//			}
//		} else
//			throw new KeyNotMatchException(null, "");
//
//		// TWO CASES:
//		
//		// 1. headerPage.root == INVALID_PAGE:
//		// - the tree is empty and we have to create a new first page;
//		// this page will be a leaf page
//		
//		// 2. headerPage.root != INVALID_PAGE:
//		// - we call _insert() to insert the pair (key, rid)
//
//		if (trace != null) {
//			trace.writeBytes("INSERT " + rid.pageNo + " " + rid.slotNo + " " + key + lineSep);
//			trace.writeBytes("DO" + lineSep);
//			trace.flush();
//		}
//
//		// case 1
//		if (headerPage.get_rootId().pid == INVALID_PAGE) {
//			
//			PageId newRootPageId;
//			BTLeafPage newRootPage;
//			RID dummyrid;
//
//			newRootPage = new BTLeafPage(headerPage.get_keyType()); // create the root page as a leaf page
//			newRootPageId = newRootPage.getCurPage(); // page number of the current page
//
//			if (trace != null) {
//				trace.writeBytes("NEWROOT " + newRootPageId + lineSep);
//				trace.flush();
//			}
//
//			// root is the leaf page & the only page. so previous page is invalid, next page is invalid
//			newRootPage.setNextPage(new PageId(INVALID_PAGE));
//			newRootPage.setPrevPage(new PageId(INVALID_PAGE));
//
//			// ASSERTIONS:
//			// - newRootPage, newRootPageId valid and pinned
//
//			newRootPage.insertRecord(key, rid);
//
//			if (trace != null) {
//				trace.writeBytes("PUTIN node " + newRootPageId + lineSep);
//				trace.flush();
//			}
//
//			unpinPage(newRootPageId, true); /* = DIRTY */
//			updateHeader(newRootPageId);
//
//			if (trace != null) {
//				trace.writeBytes("DONE" + lineSep);
//				trace.flush();
//			}
//
//			return;
//		}
//
//		// ASSERTIONS:
//		// - headerPageId, headerPage valid and pinned
//		// - headerPage.root holds the pageId of the root of the B-tree
//		// - none of the pages of the tree is pinned yet
//
//		if (trace != null) {
//			trace.writeBytes("SEARCH" + lineSep);
//			trace.flush();
//		}
//// TODO: To be continued on 8th April 2019
//		newRootEntry = _insert(key, rid, headerPage.get_rootId());
//
//		// TWO CASES:
//		// - newRootEntry != null: a leaf split propagated up to the root
//		// and the root split: the new pageNo is in
//		// newChildEntry.data.pageNo
//		// - newRootEntry == null: no new root was created;
//		// information on headerpage is still valid
//
//		// ASSERTIONS:
//		// - no page pinned
//
//		if (newRootEntry != null) {
//			BTIndexPage newRootPage;
//			PageId newRootPageId;
//			Object newEntryKey;
//
//			// the information about the pair <key, PageId> is
//			// packed in newRootEntry: extract it
//
//			newRootPage = new BTIndexPage(headerPage.get_keyType());
//			newRootPageId = newRootPage.getCurPage();
//
//			// ASSERTIONS:
//			// - newRootPage, newRootPageId valid and pinned
//			// - newEntryKey, newEntryPage contain the data for the new entry
//			// which was given up from the level down in the recursion
//
//			if (trace != null) {
//				trace.writeBytes("NEWROOT " + newRootPageId + lineSep);
//				trace.flush();
//			}
//
//			newRootPage.insertKey(newRootEntry.key, ((IndexData) newRootEntry.data).getData());
//
//			// the old root split and is now the left child of the new root
//			newRootPage.setPrevPage(headerPage.get_rootId());
//
//			unpinPage(newRootPageId, true /* = DIRTY */);
//
//			updateHeader(newRootPageId);
//
//		}
//
//		if (trace != null) {
//			trace.writeBytes("DONE" + lineSep);
//			trace.flush();
//		}
//
//		return;
//	}

	private void updateHeader(PageId newRoot) throws IOException, PinPageException, UnpinPageException {

		IntervalTreeHeaderPage header;
		PageId old_data;

		header = new IntervalTreeHeaderPage(pinPage(headerPageId));

		old_data = headerPage.get_rootId();
		header.set_rootId(newRoot);

		// clock in dirty bit to bm so our dtor needn't have to worry about it
		unpinPage(headerPageId, true /* = DIRTY */ );

		// ASSERTIONS:
		// - headerPage, headerPageId valid, pinned and marked as dirty
	}

	/**
	 * Insert entry into the index file.
	 * 
	 * @param data the key for the entry
	 * @param rid  the rid of the tuple with the key
	 * @exception IOException             from lower layers
	 * @exception KeyTooLongException     the key is too long
	 * @exception KeyNotMatchException    the keys do not match
	 * @exception LeafInsertRecException  insert record to leaf page failed
	 * @exception IndexInsertRecException insert record to index page failed
	 * @exception ConstructPageException  fail to construct a header page
	 * @exception UnpinPageException      unpin page failed
	 * @exception PinPageException        pin page failed
	 * @exception NodeNotMatchException   nodes do not match
	 * @exception ConvertException        conversion failed (from global package)
	 * @exception DeleteRecException      delete record failed
	 * @exception IndexSearchException    index search failed
	 * @exception IteratorException       error from iterator
	 * @exception LeafDeleteException     delete leaf page failed
	 * @exception InsertException         insert record failed
	 */
	public void insert(KeyClass key, final RID rid) throws KeyTooLongException, KeyNotMatchException,
			LeafInsertRecException, IndexInsertRecException, ConstructPageException, UnpinPageException,
			PinPageException, NodeNotMatchException, ConvertException, DeleteRecException, IndexSearchException,
			IteratorException, LeafDeleteException, InsertException, IOException {

		KeyDataEntry newRootEntry;

		/* 1. Basic Check */
		if (IntervalT.getKeyLength(key) > headerPage.get_maxKeySize())
			throw new KeyTooLongException(null, "");

		/* 2. Basic Check */
		if (key instanceof IntervalKey == false || headerPage.get_keyType() != AttrType.attrInterval) {
			throw new KeyNotMatchException(null, "");
		}

		// TWO CASES:

		// 1. headerPage.root == INVALID_PAGE:
		// - the tree is empty and we have to create a new first page;
		// this page will be a leaf page

		// 2. headerPage.root != INVALID_PAGE:
		// - we call _insert() to insert the pair (key, rid)

		if (trace != null) {
			trace.writeBytes("INSERT " + rid.pageNo + " " + rid.slotNo + " " + key + lineSep);
			trace.writeBytes("DO" + lineSep);
			trace.flush();
		}

		/* 3. If header page's root ID says INVALID_PAGE */
		// case 1
		if (headerPage.get_rootId().pid == INVALID_PAGE) {

			PageId newRootPageId;
			IntervalTLeafPage newRootPage;
			RID dummyrid;

			/* 4. Create a leaf page as the root page. */
			newRootPage = new IntervalTLeafPage(headerPage.get_keyType()); // create the root page as a leaf page
			newRootPageId = newRootPage.getCurPage(); // page number of the current page

			if (trace != null) {
				trace.writeBytes("NEWROOT " + newRootPageId + lineSep);
				trace.flush();
			}

			/* 5. Set the previous as invalid and next as invalid as it's the only page. */
			// root is the leaf page & the only page. so previous page is invalid, next page
			// is invalid
			newRootPage.setNextPage(new PageId(INVALID_PAGE));
			newRootPage.setPrevPage(new PageId(INVALID_PAGE));

			// ASSERTIONS:
			// - newRootPage, newRootPageId valid and pinned

			newRootPage.insertRecord(key, rid);

			if (trace != null) {
				trace.writeBytes("PUTIN node " + newRootPageId + lineSep);
				trace.flush();
			}

			unpinPage(newRootPageId, true); /* = DIRTY */
			updateHeader(newRootPageId);

			if (trace != null) {
				trace.writeBytes("DONE" + lineSep);
				trace.flush();
			}

			return;
		}

		// ASSERTIONS:
		// - headerPageId, headerPage valid and pinned
		// - headerPage.root holds the pageId of the root of the B-tree
		// - none of the pages of the tree is pinned yet

		if (trace != null) {
			trace.writeBytes("SEARCH" + lineSep);
			trace.flush();
		}

		newRootEntry = _insert(key, rid, headerPage.get_rootId());
	}

	private KeyDataEntry _insert(KeyClass key, RID rid, PageId currentPageId)
			throws PinPageException, IOException, ConstructPageException, LeafDeleteException, ConstructPageException,
			DeleteRecException, IndexSearchException, UnpinPageException, LeafInsertRecException, ConvertException,
			IteratorException, IndexInsertRecException, KeyNotMatchException, NodeNotMatchException, InsertException {

		IntervalTSortedPage currentPage;
		Page page;
		KeyDataEntry upEntry;

		// pin the current page ID, headerPage.get_rootId() initially -- pin the header
		// page
		page = pinPage(currentPageId);
		currentPage = new IntervalTSortedPage(page, headerPage.get_keyType());

		if (trace != null) {
			trace.writeBytes("VISIT node " + currentPageId + lineSep);
			trace.flush();
		}

		// TWO CASES:
		// - pageType == INDEX:
		// recurse and then split if necessary
		// - pageType == LEAF:
		// try to insert pair (key, rid), maybe split

		if (currentPage.getType() == NodeType.INDEX) {
			IntervalTIndexPage currentIndexPage = new IntervalTIndexPage(page, headerPage.get_keyType());
			PageId currentIndexPageId = currentPageId;
			PageId nextPageId;

			nextPageId = currentIndexPage.getPageNoByKey(key);

			// unpin page, recurse and then pin it again
			unpinPage(currentIndexPageId);

			upEntry = _insert(key, rid, nextPageId);

			// two cases:
			// - upEntry == null: one level lower no split has occurred:
			// we are done.

			// - upEntry != null: one of the children has split and
			// upEntry is the new data entry which has
			// to be inserted on this index page

			if (upEntry == null)
				return null;

			currentIndexPage = new IntervalTIndexPage(pinPage(currentPageId), headerPage.get_keyType());

			// ASSERTIONS:
			// - upEntry != null
			// - currentIndexPage, currentIndexPageId valid and pinned

			// the information about the pair <key, PageId> is
			// packed in upEntry

			// check whether there can still be entries inserted on that page
			if (currentIndexPage.available_space() >= IntervalT.getKeyDataLength(upEntry.key, NodeType.INDEX)) {

				// no split has occurred
				currentIndexPage.insertKey(upEntry.key, ((IndexData) upEntry.data).getData());

				unpinPage(currentIndexPageId, true /* DIRTY */);

				return null;
			}

			// ASSERTIONS:
			// - on the current index page is not enough space available .
			// it splits

			// therefore we have to allocate a new index page and we will
			// distribute the entries
			// - currentIndexPage, currentIndexPageId valid and pinned

			IntervalTIndexPage newIndexPage;
			PageId newIndexPageId;

			// we have to allocate a new INDEX page and
			// to redistribute the index entries
			newIndexPage = new IntervalTIndexPage(headerPage.get_keyType()); // allocate a new IntervalTIndexPage of
																				// type IntervalKey
			newIndexPageId = newIndexPage.getCurPage();

			if (trace != null) {
				if (headerPage.get_rootId().pid != currentIndexPageId.pid)
					trace.writeBytes("SPLIT node " + currentIndexPageId + " IN nodes " + currentIndexPageId + " "
							+ newIndexPageId + lineSep);
				else
					trace.writeBytes("ROOTSPLIT IN nodes " + currentIndexPageId + " " + newIndexPageId + lineSep);
				trace.flush();
			}

			// ASSERTIONS:
			// - newIndexPage, newIndexPageId valid and pinned
			// - currentIndexPage, currentIndexPageId valid and pinned
			// - upEntry containing (Key, Page) for the new entry which was
			// given up from the level down in the recursion

			KeyDataEntry tmpEntry;
			PageId tmpPageId;
			RID insertRid;
			RID delRid = new RID();

			// The RID passed to getFirst is the input and output parameter
			for (tmpEntry = currentIndexPage.getFirst(delRid); tmpEntry != null; tmpEntry = currentIndexPage
					.getFirst(delRid)) {
				newIndexPage.insertKey(tmpEntry.key, ((IndexData) tmpEntry.data).getData());
				currentIndexPage.deleteSortedRecord(delRid);
			}

			// ASSERTIONS:
			// - currentIndexPage empty
			// - newIndexPage holds all former records from currentIndexPage

			// we will try to make an equal split
			RID firstRid = new RID();
			KeyDataEntry undoEntry = null;
			for (tmpEntry = newIndexPage.getFirst(firstRid); (currentIndexPage.available_space() > newIndexPage
					.available_space()); tmpEntry = newIndexPage.getFirst(firstRid)) {
				// now insert the <key,pageId> pair on the new
				// index page
				undoEntry = tmpEntry;
				currentIndexPage.insertKey(tmpEntry.key, ((IndexData) tmpEntry.data).getData());
				newIndexPage.deleteSortedRecord(firstRid);
			}

			// undo the final record
			if (currentIndexPage.available_space() < newIndexPage.available_space()) {

				newIndexPage.insertKey(undoEntry.key, ((IndexData) undoEntry.data).getData());

				currentIndexPage.deleteSortedRecord(
						new RID(currentIndexPage.getCurPage(), (int) currentIndexPage.getSlotCnt() - 1));
			}

			// check whether <newKey, newIndexPageId>
			// will be inserted
			// on the newly allocated or on the old index page

			tmpEntry = newIndexPage.getFirst(firstRid);

			if (IntervalT.keyCompare(upEntry.key, tmpEntry.key) >= 0) {
				// the new data entry belongs on the new index page
				newIndexPage.insertKey(upEntry.key, ((IndexData) upEntry.data).getData());
			} else {
				currentIndexPage.insertKey(upEntry.key, ((IndexData) upEntry.data).getData());

				int i = (int) currentIndexPage.getSlotCnt() - 1;
				tmpEntry = IntervalT.getEntryFromBytes(currentIndexPage.getpage(), currentIndexPage.getSlotOffset(i),
						currentIndexPage.getSlotLength(i), headerPage.get_keyType(), NodeType.INDEX);

				newIndexPage.insertKey(tmpEntry.key, ((IndexData) tmpEntry.data).getData());

				currentIndexPage.deleteSortedRecord(new RID(currentIndexPage.getCurPage(), i));

			}

			unpinPage(currentIndexPageId, true /* dirty */);

			// fill upEntry
			upEntry = newIndexPage.getFirst(delRid);

			// now set prevPageId of the newIndexPage to the pageId
			// of the deleted entry:
			newIndexPage.setPrevPage(((IndexData) upEntry.data).getData());

			// delete first record on new index page since it is given up
			newIndexPage.deleteSortedRecord(delRid);

			unpinPage(newIndexPageId, true /* dirty */);

			if (trace != null) {
				trace_children(currentIndexPageId);
				trace_children(newIndexPageId);
			}

			((IndexData) upEntry.data).setData(newIndexPageId);

			return upEntry;

			// ASSERTIONS:
			// - no pages pinned
			// - upEntry holds the pointer to the KeyDataEntry which is
			// to be inserted on the index page one level up

		} else if (currentPage.getType() == NodeType.LEAF) {
			IntervalTLeafPage currentLeafPage = new IntervalTLeafPage(page, headerPage.get_keyType());

			PageId currentLeafPageId = currentPageId;

			// ASSERTIONS:
			// - currentLeafPage, currentLeafPageId valid and pinned

			// check whether there can still be entries inserted on that page
			if (currentLeafPage.available_space() >= IntervalT.getKeyDataLength(key, NodeType.LEAF)) {
				// no split has occurred

				currentLeafPage.insertRecord(key, rid);

				unpinPage(currentLeafPageId, true /* DIRTY */);

				if (trace != null) {
					trace.writeBytes("PUTIN node " + currentLeafPageId + lineSep);
					trace.flush();
				}

				return null;
			}

			// ASSERTIONS:
			// - on the current leaf page is not enough space available.
			// It splits.
			// - therefore we have to allocate a new leaf page and we will
			// - distribute the entries

			IntervalTLeafPage newLeafPage;
			PageId newLeafPageId;

			// we have to allocate a new LEAF page and
			// to redistribute the data entries
			newLeafPage = new IntervalTLeafPage(headerPage.get_keyType());
			newLeafPageId = newLeafPage.getCurPage();

			newLeafPage.setNextPage(currentLeafPage.getNextPage());
			newLeafPage.setPrevPage(currentLeafPageId); // for dbl-linked list
			currentLeafPage.setNextPage(newLeafPageId); // don't know why this is needed

			// change the prevPage pointer on the next page:

			PageId rightPageId;
			rightPageId = newLeafPage.getNextPage();
			if (rightPageId.pid != INVALID_PAGE) {
				IntervalTLeafPage rightPage;
				rightPage = new IntervalTLeafPage(rightPageId, headerPage.get_keyType());

				rightPage.setPrevPage(newLeafPageId);
				unpinPage(rightPageId, true /* = DIRTY */);

				// ASSERTIONS:
				// - newLeafPage, newLeafPageId valid and pinned
				// - currentLeafPage, currentLeafPageId valid and pinned
			}

			if (trace != null) {
				if (headerPage.get_rootId().pid != currentLeafPageId.pid)
					trace.writeBytes("SPLIT node " + currentLeafPageId + " IN nodes " + currentLeafPageId + " "
							+ newLeafPageId + lineSep);
				else
					trace.writeBytes("ROOTSPLIT IN nodes " + currentLeafPageId + " " + newLeafPageId + lineSep);
				trace.flush();
			}

			KeyDataEntry tmpEntry;
			RID firstRid = new RID();

			for (tmpEntry = currentLeafPage.getFirst(firstRid); tmpEntry != null; tmpEntry = currentLeafPage
					.getFirst(firstRid)) {

				newLeafPage.insertRecord(tmpEntry.key, ((LeafData) (tmpEntry.data)).getData());
				currentLeafPage.deleteSortedRecord(firstRid);

			}

			// ASSERTIONS:
			// - currentLeafPage empty
			// - newLeafPage holds all former records from currentLeafPage

			KeyDataEntry undoEntry = null;
			for (tmpEntry = newLeafPage.getFirst(firstRid); newLeafPage.available_space() < currentLeafPage
					.available_space(); tmpEntry = newLeafPage.getFirst(firstRid)) {
				undoEntry = tmpEntry;
				currentLeafPage.insertRecord(tmpEntry.key, ((LeafData) tmpEntry.data).getData());
				newLeafPage.deleteSortedRecord(firstRid);
			}

			if (IntervalT.keyCompare(key, undoEntry.key) < 0) {
				// undo the final record
				if (currentLeafPage.available_space() < newLeafPage.available_space()) {
					newLeafPage.insertRecord(undoEntry.key, ((LeafData) undoEntry.data).getData());

					currentLeafPage.deleteSortedRecord(
							new RID(currentLeafPage.getCurPage(), (int) currentLeafPage.getSlotCnt() - 1));
				}
			}

			// check whether <key, rid>
			// will be inserted
			// on the newly allocated or on the old leaf page

			if (IntervalT.keyCompare(key, undoEntry.key) >= 0) {
				// the new data entry belongs on the new Leaf page
				newLeafPage.insertRecord(key, rid);

				if (trace != null) {
					trace.writeBytes("PUTIN node " + newLeafPageId + lineSep);
					trace.flush();
				}

			} else {
				currentLeafPage.insertRecord(key, rid);
			}

			unpinPage(currentLeafPageId, true /* dirty */);

			if (trace != null) {
				trace_children(currentLeafPageId);
				trace_children(newLeafPageId);
			}

			// fill upEntry
			tmpEntry = newLeafPage.getFirst(firstRid);
			upEntry = new KeyDataEntry(tmpEntry.key, newLeafPageId);

			unpinPage(newLeafPageId, true /* dirty */);

			// ASSERTIONS:
			// - no pages pinned
			// - upEntry holds the valid KeyDataEntry which is to be inserted
			// on the index page one level up
			return upEntry;
		} else {
			throw new InsertException(null, "");
		}
	}
	
	/**
	 * It causes a structured trace to be written to a file. This output is used to
	 * drive a visualization tool that shows the inner workings of the b-tree during
	 * its operations.
	 * 
	 * @param filename input parameter. The trace file name
	 * @exception IOException error from the lower layer
	 */
	public static void traceFilename(String filename) throws IOException {

		fos = new FileOutputStream(filename);
		trace = new DataOutputStream(fos);
	}
	
	public IntervalTreeHeaderPage getHeaderPage() {
		return headerPage;
	}

	void trace_children(PageId id)
			throws IOException, IteratorException, ConstructPageException, PinPageException, UnpinPageException {

		if (trace != null) {

			IntervalTSortedPage sortedPage;
			RID metaRid = new RID();
			PageId childPageId;
			KeyClass key;
			KeyDataEntry entry;
			sortedPage = new IntervalTSortedPage(pinPage(id), headerPage.get_keyType());

			// Now print all the child nodes of the page.
			if (sortedPage.getType() == NodeType.INDEX) {
				IntervalTIndexPage indexPage = new IntervalTIndexPage(sortedPage, headerPage.get_keyType());
				trace.writeBytes("INDEX CHILDREN " + id + " nodes" + lineSep);
				trace.writeBytes(" " + indexPage.getPrevPage());
				for (entry = indexPage.getFirst(metaRid); entry != null; entry = indexPage.getNext(metaRid)) {
					trace.writeBytes("   " + ((IndexData) entry.data).getData());
				}
			} else if (sortedPage.getType() == NodeType.LEAF) {
				IntervalTLeafPage leafPage = new IntervalTLeafPage(sortedPage, headerPage.get_keyType());
				trace.writeBytes("LEAF CHILDREN " + id + " nodes" + lineSep);
				for (entry = leafPage.getFirst(metaRid); entry != null; entry = leafPage.getNext(metaRid)) {
					trace.writeBytes("   " + entry.key + " " + entry.data);
				}
			}
			unpinPage(id);
			trace.writeBytes(lineSep);
			trace.flush();
		}

	}

	/**
	 * Close the B+ tree file. Unpin header page.
	 * 
	 * @exception PageUnpinnedException       error from the lower layer
	 * @exception InvalidFrameNumberException error from the lower layer
	 * @exception HashEntryNotFoundException  error from the lower layer
	 * @exception ReplacerException           error from the lower layer
	 */
	public void close()
			throws PageUnpinnedException, InvalidFrameNumberException, HashEntryNotFoundException, ReplacerException {
		if (headerPage != null) {
			SystemDefs.JavabaseBM.unpinPage(headerPageId, true);
			headerPage = null;
		}
	}

	@Override
	public boolean Delete(KeyClass data, RID rid)
			throws DeleteFashionException, LeafRedistributeException, RedistributeException, InsertRecException,
			KeyNotMatchException, UnpinPageException, IndexInsertRecException, FreePageException,
			RecordNotFoundException, PinPageException, IndexFullDeleteException, LeafDeleteException, IteratorException,
			ConstructPageException, DeleteRecException, IndexSearchException, IOException {
		// TODO Auto-generated method stub
		return false;
	}
	
	private KeyClass _Delete(KeyClass key, RID rid, PageId currentPageId, PageId parentPageId)
			throws IndexInsertRecException, RedistributeException, IndexSearchException, RecordNotFoundException,
			DeleteRecException, InsertRecException, LeafRedistributeException, IndexFullDeleteException,
			FreePageException, LeafDeleteException, KeyNotMatchException, ConstructPageException, UnpinPageException,
			IteratorException, PinPageException, IOException {

		IntervalTSortedPage sortPage;
		Page page;
		page = pinPage(currentPageId);
		sortPage = new IntervalTSortedPage(page, headerPage.get_keyType());

		if (trace != null) {
			trace.writeBytes("VISIT node " + currentPageId + lineSep);
			trace.flush();
		}

		if (sortPage.getType() == NodeType.LEAF) {
			RID curRid = new RID(); // iterator
			KeyDataEntry tmpEntry;
			KeyClass curkey;
			RID dummyRid;
			PageId nextpage;
			IntervalTLeafPage leafPage;
			leafPage = new IntervalTLeafPage(page, headerPage.get_keyType());

			KeyClass deletedKey = key;
			tmpEntry = leafPage.getFirst(curRid);

			RID delRid;
			// for all records with key equal to 'key', delete it if its rid = 'rid'
			while ((tmpEntry != null) && (IntervalT.keyCompare(key, tmpEntry.key) >= 0)) {
				// WriteUpdateLog is done in the btleafpage level - to log the
				// deletion of the rid.

				if (leafPage.delEntry(new KeyDataEntry(key, rid))) {
					// successfully found <key, rid> on this page and deleted it.

					if (trace != null) {
						trace.writeBytes("TAKEFROM node " + leafPage.getCurPage() + lineSep);
						trace.flush();
					}

					PageId leafPage_no = leafPage.getCurPage();
					if ((4 + leafPage.available_space()) <= ((MAX_SPACE - HFPage.DPFIXED) / 2)) {
						// the leaf page is at least half full after the deletion
						unpinPage(leafPage.getCurPage(), true /* = DIRTY */);
						return null;
					} else if (leafPage_no.pid == headerPage.get_rootId().pid) {
						// the tree has only one node - the root
						if (leafPage.numberOfRecords() != 0) {
							unpinPage(leafPage_no, true /* = DIRTY */);
							return null;
						} else {
							// the whole tree is empty

							if (trace != null) {
								trace.writeBytes("DEALLOCATEROOT " + leafPage_no + lineSep);
								trace.flush();
							}

							freePage(leafPage_no);

							updateHeader(new PageId(INVALID_PAGE));
							return null;
						}
					} else {
						// get a sibling
						IntervalTIndexPage parentPage;
						parentPage = new IntervalTIndexPage(pinPage(parentPageId), headerPage.get_keyType());

						PageId siblingPageId = new PageId();
						IntervalTLeafPage siblingPage;
						int direction;
						direction = parentPage.getSibling(key, siblingPageId);

						if (direction == 0) {
							// there is no sibling. nothing can be done.

							unpinPage(leafPage.getCurPage(), true /* =DIRTY */);

							unpinPage(parentPageId);

							return null;
						}

						siblingPage = new IntervalTLeafPage(pinPage(siblingPageId), headerPage.get_keyType());

						if (siblingPage.redistribute(leafPage, parentPage, direction, deletedKey)) {
							// the redistribution has been done successfully

							if (trace != null) {

								trace_children(leafPage.getCurPage());
								trace_children(siblingPage.getCurPage());

							}

							unpinPage(leafPage.getCurPage(), true);
							unpinPage(siblingPageId, true);
							unpinPage(parentPageId, true);
							return null;
						} else if ((siblingPage.available_space()
								+ 8 /* 2*sizeof(slot) */ ) >= ((MAX_SPACE - HFPage.DPFIXED)
										- leafPage.available_space())) {

							// we can merge these two children
							// get old child entry in the parent first
							KeyDataEntry oldChildEntry;
							if (direction == -1)
								oldChildEntry = leafPage.getFirst(curRid);
							// get a copy
							else {
								oldChildEntry = siblingPage.getFirst(curRid);
							}

							// merge the two children
							IntervalTLeafPage leftChild, rightChild;
							if (direction == -1) {
								leftChild = siblingPage;
								rightChild = leafPage;
							} else {
								leftChild = leafPage;
								rightChild = siblingPage;
							}

							// move all entries from rightChild to leftChild
							RID firstRid = new RID(), insertRid;
							for (tmpEntry = rightChild.getFirst(firstRid); tmpEntry != null; tmpEntry = rightChild
									.getFirst(firstRid)) {
								leftChild.insertRecord(tmpEntry);
								rightChild.deleteSortedRecord(firstRid);
							}

							// adjust chain
							leftChild.setNextPage(rightChild.getNextPage());
							if (rightChild.getNextPage().pid != INVALID_PAGE) {
								IntervalTLeafPage nextLeafPage = new IntervalTLeafPage(rightChild.getNextPage(),
										headerPage.get_keyType());
								nextLeafPage.setPrevPage(leftChild.getCurPage());
								unpinPage(nextLeafPage.getCurPage(), true);
							}

							if (trace != null) {
								trace.writeBytes("MERGE nodes " + leftChild.getCurPage() + " " + rightChild.getCurPage()
										+ lineSep);
								trace.flush();
							}

							unpinPage(leftChild.getCurPage(), true);

							unpinPage(parentPageId, true);

							freePage(rightChild.getCurPage());

							return oldChildEntry.key;
						} else {
							// It's a very rare case when we can do neither
							// redistribution nor merge.

							unpinPage(leafPage.getCurPage(), true);

							unpinPage(siblingPageId, true);

							unpinPage(parentPageId, true);

							return null;
						}
					} // get a sibling block
				} // delete success block

				nextpage = leafPage.getNextPage();
				unpinPage(leafPage.getCurPage());

				if (nextpage.pid == INVALID_PAGE)
					throw new RecordNotFoundException(null, "");

				leafPage = new IntervalTLeafPage(pinPage(nextpage), headerPage.get_keyType());
				tmpEntry = leafPage.getFirst(curRid);

			} // while loop

			/*
			 * We reached a page with first key > `key', so return an error. We should have
			 * got true back from delUserRid above. Apparently the specified <key,rid> data
			 * entry does not exist.
			 */

			unpinPage(leafPage.getCurPage());
			throw new RecordNotFoundException(null, "");
		}

		if (sortPage.getType() == NodeType.INDEX) {
			PageId childPageId;
			IntervalTIndexPage indexPage = new IntervalTIndexPage(page, headerPage.get_keyType());
			childPageId = indexPage.getPageNoByKey(key);

			// now unpin the page, recurse and then pin it again
			unpinPage(currentPageId);

			KeyClass oldChildKey = _Delete(key, rid, childPageId, currentPageId);

			// two cases:
			// - oldChildKey == null: one level lower no merge has occurred:
			// - oldChildKey != null: one of the children has been deleted and
			// oldChildEntry is the entry to be deleted.

			indexPage = new IntervalTIndexPage(pinPage(currentPageId), headerPage.get_keyType());

			if (oldChildKey == null) {
				unpinPage(indexPage.getCurPage(), true);
				return null;
			}

			// delete the oldChildKey

			// save possible old child entry before deletion
			PageId dummyPageId;
			KeyClass deletedKey = key;
			RID curRid = indexPage.deleteKey(oldChildKey);

			if (indexPage.getCurPage().pid == headerPage.get_rootId().pid) {
				// the index page is the root
				if (indexPage.numberOfRecords() == 0) {
					IntervalTSortedPage childPage;
					childPage = new IntervalTSortedPage(indexPage.getPrevPage(), headerPage.get_keyType());

					if (trace != null) {
						trace.writeBytes("CHANGEROOT from node " + indexPage.getCurPage() + " to node "
								+ indexPage.getPrevPage() + lineSep);
						trace.flush();
					}

					updateHeader(indexPage.getPrevPage());
					unpinPage(childPage.getCurPage());

					freePage(indexPage.getCurPage());
					return null;
				}
				unpinPage(indexPage.getCurPage(), true);
				return null;
			}

			// now we know the current index page is not a root
			if ((4 /* sizeof slot */ + indexPage.available_space()) <= ((MAX_SPACE - HFPage.DPFIXED) / 2)) {
				// the index page is at least half full after the deletion
				unpinPage(currentPageId, true);

				return null;
			} else {
				// get a sibling
				IntervalTIndexPage parentPage;
				parentPage = new IntervalTIndexPage(pinPage(parentPageId), headerPage.get_keyType());

				PageId siblingPageId = new PageId();
				IntervalTIndexPage siblingPage;
				int direction;
				direction = parentPage.getSibling(key, siblingPageId);
				if (direction == 0) {
					// there is no sibling. nothing can be done.

					unpinPage(indexPage.getCurPage(), true);

					unpinPage(parentPageId);

					return null;
				}

				siblingPage = new IntervalTIndexPage(pinPage(siblingPageId), headerPage.get_keyType());

				int pushKeySize = 0;
				if (direction == 1) {
					pushKeySize = IntervalT.getKeyLength(parentPage.findKey(siblingPage.getFirst(new RID()).key));
				} else if (direction == -1) {
					pushKeySize = IntervalT.getKeyLength(parentPage.findKey(indexPage.getFirst(new RID()).key));
				}

				if (siblingPage.redistribute(indexPage, parentPage, direction, deletedKey)) {
					// the redistribution has been done successfully

					if (trace != null) {

						trace_children(indexPage.getCurPage());
						trace_children(siblingPage.getCurPage());

					}

					unpinPage(indexPage.getCurPage(), true);

					unpinPage(siblingPageId, true);

					unpinPage(parentPageId, true);

					return null;
				} else if (siblingPage.available_space() + 4 /* slot size */ >= ((MAX_SPACE - HFPage.DPFIXED)
						- (indexPage.available_space() + 4 /* slot size */) + pushKeySize + 4 /* slot size */
						+ 4 /* pageId size */)) {

					// we can merge these two children

					// get old child entry in the parent first
					KeyClass oldChildEntry;
					if (direction == -1) {
						oldChildEntry = indexPage.getFirst(curRid).key;
					} else {
						oldChildEntry = siblingPage.getFirst(curRid).key;
					}

					// merge the two children
					IntervalTIndexPage leftChild, rightChild;
					if (direction == -1) {
						leftChild = siblingPage;
						rightChild = indexPage;
					} else {
						leftChild = indexPage;
						rightChild = siblingPage;
					}

					if (trace != null) {
						trace.writeBytes(
								"MERGE nodes " + leftChild.getCurPage() + " " + rightChild.getCurPage() + lineSep);
						trace.flush();
					}

					// pull down the entry in its parent node
					// and put it at the end of the left child
					RID firstRid = new RID(), insertRid;
					PageId curPageId;

					leftChild.insertKey(parentPage.findKey(oldChildEntry), rightChild.getLeftLink());

					// move all entries from rightChild to leftChild
					for (KeyDataEntry tmpEntry = rightChild.getFirst(firstRid); tmpEntry != null; tmpEntry = rightChild
							.getFirst(firstRid)) {
						leftChild.insertKey(tmpEntry.key, ((IndexData) tmpEntry.data).getData());
						rightChild.deleteSortedRecord(firstRid);
					}

					unpinPage(leftChild.getCurPage(), true);

					unpinPage(parentPageId, true);

					freePage(rightChild.getCurPage());

					return oldChildEntry; // ???

				} else {
					// It's a very rare case when we can do neither
					// redistribution nor merge.

					unpinPage(indexPage.getCurPage(), true);

					unpinPage(siblingPageId, true);

					unpinPage(parentPageId);

					return null;
				}
			}
		} // index node
		return null; // neither leaf and index page

	}
	
	/*
	 * Status BTreeFile::FullDelete (const void *key, const RID rid)
	 *
	 * Remove specified data entry (<key, rid>) from an index.
	 *
	 * Most work done recursively by _Delete
	 *
	 * Special case: delete root if the tree is empty
	 *
	 * Page containing first occurrence of key `key' is found for us After the page
	 * containing first occurence of key 'key' is found, we iterate for (just a few)
	 * pages, if necesary, to find the one containing <key,rid>, which we then
	 * delete via BTLeafPage::delUserRid.
	 * 
	 * @return false if no such record; true if succees
	 */

	private boolean FullDelete(KeyClass key, RID rid)
			throws IndexInsertRecException, RedistributeException, IndexSearchException, RecordNotFoundException,
			DeleteRecException, InsertRecException, LeafRedistributeException, IndexFullDeleteException,
			FreePageException, LeafDeleteException, KeyNotMatchException, ConstructPageException, IOException,
			IteratorException, PinPageException, UnpinPageException, IteratorException {

		try {

			if (trace != null) {
				trace.writeBytes("DELETE " + rid.pageNo + " " + rid.slotNo + " " + key + lineSep);
				trace.writeBytes("DO" + lineSep);
				trace.writeBytes("SEARCH" + lineSep);
				trace.flush();
			}

			_Delete(key, rid, headerPage.get_rootId(), null);

			if (trace != null) {
				trace.writeBytes("DONE" + lineSep);
				trace.flush();
			}

			return true;
		} catch (RecordNotFoundException e) {
			return false;
		}

	}
	
	/**
	 * create a scan with given keys Cases: (1) lo_key = null, hi_key = null scan
	 * the whole index (2) lo_key = null, hi_key!= null range scan from min to the
	 * hi_key (3) lo_key!= null, hi_key = null range scan from the lo_key to max (4)
	 * lo_key!= null, hi_key!= null, lo_key = hi_key exact match ( might not unique)
	 * (5) lo_key!= null, hi_key!= null, lo_key < hi_key range scan from lo_key to
	 * hi_key
	 * 
	 * @param lo_key the key where we begin scanning. Input parameter.
	 * @param hi_key the key where we stop scanning. Input parameter.
	 * @exception IOException            error from the lower layer
	 * @exception KeyNotMatchException   key is not integer key nor string key
	 * @exception IteratorException      iterator error
	 * @exception ConstructPageException error in BT page constructor
	 * @exception PinPageException       error when pin a page
	 * @exception UnpinPageException     error when unpin a page
	 */
	public IntervalFileScan new_scan(KeyClass lo_key, KeyClass hi_key) throws IOException, KeyNotMatchException,
			IteratorException, ConstructPageException, PinPageException, UnpinPageException

	{
		IntervalFileScan scan = new IntervalFileScan();
		if (headerPage.get_rootId().pid == INVALID_PAGE) {
			scan.leafPage = null;
			return scan;
		}

		scan.treeFilename = dbname;
		scan.endkey = hi_key;
		scan.didfirst = false;
		scan.deletedcurrent = false;
		scan.curRid = new RID();
		scan.keyType = headerPage.get_keyType();
		scan.maxKeysize = headerPage.get_maxKeySize();
		scan.ifile = this;

		// this sets up scan at the starting position, ready for iteration
		scan.leafPage = findRunStart(lo_key, scan.curRid);
		return scan;
	}
}
