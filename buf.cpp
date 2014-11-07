///////////////////////////////////////////////////////////////////////////////
//                   ALL STUDENTS COMPLETE THESE SECTIONS
// Title:            Buffer Pool
// Files:            buf.cpp, db.h, buf.h, db.cpp, error.h, error.cpp, 
//                   page.cpp, page.h, bufhash.cpp
// Semester:         CS564 Fall 2014
//
// Author:           Cong Sun
// Email:            csun27@wisc.edu
// CS Login:         cong
//////////////////// PAIR PROGRAMMERS COMPLETE THIS SECTION ////////////////////
// Pair Partner:     Boqun Yan
// Email:            byan23@wisc.edu
// CS Login:         boqun
//////////////////////////// 80 columns wide //////////////////////////////////

/**
 * It implements the buf.h; it implements the basic functions of a buffer pool
 * for a DBMS
 * <p>Bugs: No bug
 * @author Cong Sun
 *         Boqun Yan
 */

#include <memory.h>
#include <unistd.h>
#include <errno.h>
#include <stdlib.h>
#include <fcntl.h>
#include <iostream>
#include <stdio.h>
#include "page.h"
#include "buf.h"

#define ASSERT(c)  { if (!(c)) {				\
      cerr << "At line " << __LINE__ << ":" << endl << "  ";	\
      cerr << "This condition should hold: " #c << endl;	\
      exit(1);							\
    }								\
  }

//----------------------------------------
// Constructor of the class BufMgr
//----------------------------------------

BufMgr::BufMgr(const int bufs)
{
  numBufs = bufs;
  //array of buffer description table; only contains description of a table
  bufTable = new BufDesc[bufs];
  //allocate memory for the bufTable
  memset(bufTable, 0, bufs * sizeof(BufDesc));
  //buf descritption table; set the frame number;
  for (int i = 0; i < bufs; i++) 
    {
      bufTable[i].frameNo = i;
      //initially there is nothing in the buffer pool;
      //so valid bits are all false for all frames
      bufTable[i].valid = false;
    }
  //actual buffer pool; buffer pool is an array of PAGE pointers
  bufPool = new Page[bufs];
  memset(bufPool, 0, bufs * sizeof(Page));
  int htsize = ((((int) (bufs * 1.2))*2)/2)+1;
  hashTable = new BufHashTbl (htsize);  // allocate the buffer hash table
  //initalize clockhand as num of pages in the buffer pool-1;
  //so that the first time we call the advance clockhand, it points to
  //the 0th page in the buffer pool
  clockHand = bufs - 1;
  /*code for advance clockhand
    clockhand = (clockhand + 1) %numBufs
  */
}

/*
 * Destructor-write the dirty page in the buffer pool back to disk and free all the
 * memory that buffer pool used
 */
BufMgr::~BufMgr() {
  // TODO: Implement this method by looking at the description in the writeup.
  for(int i = 0; i < numBufs; i++){
    //if the frame is valid
    if(bufTable[i].valid){
      //if this page is dirty
      if(bufTable[i].dirty){
	//write back to the disk
	bufTable[i].file->writePage(bufTable[i].pageNo, &bufPool[i]);
      }
    }
  }
  //free buffer description table
  delete [] bufTable;
  //free actually buffer pool
  delete [] bufPool;
  //free hashtable
  // delete hashTable;
}



/*
 * Allocates a free frame usign the clock algorithm; if necessary writing a dirty page
 * back to disk
 * @param int &frame the allocated frameNo
 * @return OK on success
 * BUFFEREXCEEDED if all buffer frames are pinned
 * UNIXERR if the call to the I/O returned an error when a dirty page to disk
*/
const Status BufMgr::allocBuf(int & frame) {
  // TODO: Implement this method by looking at the description in the writeup.
  // we only advance numBufs times clockhand; if we dont find anything, then return 
  // BUFFEREXCEEDED
  int numPin = 0;
  int count = 0;
  while(1){
    //clock algo
    advanceClock();
    count++;
    if(count % numBufs == 0){
      numPin = 0;
    }
    //if current frame is not valid
    if(!bufTable[clockHand].valid){
      //mark this frame valid
      bufTable[clockHand].valid = true;
      //give the frameNo out to the caller for further use
      frame = clockHand;
      return OK;
    }
    //the frame is valid
    else{
      //check if the frame is recently referenced
      if(bufTable[clockHand].refbit){
	//recently referenced, clear the ref bit and advance the clock
	bufTable[clockHand].refbit = false;
      }
      //if not recently referenced
      else{
	//not pinned
	if(bufTable[clockHand].pinCnt == 0){
	  //check if its dirty
	  //is dirty
	  if(bufTable[clockHand].dirty){	    
	    if(bufTable[clockHand].file->writePage(bufTable[clockHand].pageNo, &bufPool[clockHand]) != OK){
	      return UNIXERR;
	    }
	  }
	  //not dirty or successfully write back the dirty frame to disk
	  //clear the chosen frame
	  Status temp =  hashTable->remove(bufTable[clockHand].file, bufTable[clockHand].pageNo); 
	  if (temp != OK)
	  return temp;
	  bufTable[clockHand].Clear();
	  //give the frameNo out to the caller for further use
	  frame = clockHand;
	  return OK;
	} //end of not pinned -> true
	//this frame is pinned
	else{
	  //increment the number of pinned frame
	  if(++numPin == numBufs){
	    //all frames in the buffer pool is pinned
	    //break out the endless loop
	    break;
	  }
	}
      }// end of rencenty referenced checking
    } // end of valid checking
  } // endless loop
  //fall off the for loop, meaning every frame in the buffer pool is pinned
  return BUFFEREXCEEDED;
}
/*
 * Read a page in a file; Handling two cases: 1. the page is in the buffer pool
 * 2. the page is not in the buffer pool -> bring it in

 * @param *file the file pointer pointing to from which file we read
 * PageNo the page number in the file that we want to read
 * *&page return a pointer to the frame containing the page via this pointer

 * @return OK if no error
 * UNIXERR if unix error occured
 * BUFFEREXCEEDED if all buffer frames are pinned
 * HASHTBLERROR -> dont think lookup() will generate any hashtable error
 * -> insert may generate this error
*/
const Status BufMgr::readPage(File* file, const int PageNo, Page*& page) {
  // TODO: Implement this method by looking at the description in the writeup.
  int frame = -1;
  //if we found the page in the buffer pool
  if(hashTable->lookup(file, PageNo, frame) == OK){
    //now we found the frame number in the buffer pool containing the page
    //set ref bit
    bufTable[frame].refbit = true;
    //pin count incremented
    bufTable[frame].pinCnt++;
    page = &bufPool[frame];
    return OK;
  }
  //if we have not found the page in the buffer pool
  else{
    Status abstatus = allocBuf(frame);
    if(abstatus == OK){
      //read the pageNo in file from disk to memory address specified
      //by page pointer in the buffer pool frame allocated by allocBuf
      if((file->readPage(PageNo, &bufPool[frame])) == OK){
	//now we successfully read the page from disk to the buffer pool
	//insert entry into the hashtable
	if((hashTable->insert(file, PageNo, frame)) != OK){
	  return HASHTBLERROR;
	}//end of unable to insert the page table entry
	//invoke set()
	bufTable[frame].Set(file, PageNo);
	//return the page pointer
	page = &bufPool[frame];
 	//then it would return OK in the end of the function
      }else{
	return UNIXERR;
      }// end of unable to read the page from the disk     
    } // end of did successfully allocate a buffer in the buffer pool for this page
    else{
      return abstatus;
    }//end of not found the page in the buffer pool
  }
  return OK;
}

/*
 * Unpin a page in the buffer pool
 * @param *file, the file that contains the page needs to be unpinned
 *        PageNo, the page number within the file that needs to be unpinned
 *        dirty, to tell the buffer pool if the page we are going to unpin is 
 *        dirty or not
 * @return OK on success
 *         HASHNOTFOUND if the page is not in the buffer pool hash table
 *         PAGENOTPINNED if the pin count is already 0
 */
const Status BufMgr::unPinPage(File* file, const int PageNo, 
			       const bool dirty) {
  // TODO: Implement this method by looking at the description in the writeup.
  //used to store the frame no returned by hashtable lookup
  int frame = -1;
  Status lk;
  lk = hashTable->lookup(file, PageNo, frame);
  if(lk == OK){
    if(dirty){
      //set the dirty bit if dirty == true
      bufTable[frame].dirty = true;
    }
    //decrement the pinCnt
    if(bufTable[frame].pinCnt != 0){
      bufTable[frame].pinCnt--;
    }else{
      return PAGENOTPINNED;
    }
  }else{
    return lk;
  }
  return OK;
}



/*
 * Allocate a new page in the file and bring it into the buffer pool
 * @param *file, the file that we want to allcate a new page in
 *        PageNo, the new page number that will be returned
 *        page, the pointer to the buffer frame containing the new allocated page 
 * @return OK on success
 *         UNIXERR if a Unix error occurred
 *         BUFFEREXCEEDED if all buffer frames are pinned
 *         HASHTBLERROR if a hash table error occurred
 */

const Status BufMgr::allocPage(File* file, int& pageNo, Page*& page)  {
  // TODO: Implement this method by looking at the description in the writeup.
  int pn = -1; // new allocated page number by file system  
  int fm = -1; // we try to get a new frame number by calling allocBuf
  if(file->allocatePage(pn) != OK){
    //question if we return unixerr when allocatePage failed
    return UNIXERR;
  }else{    
  
    //successfully allocate a new page in a file
    //set() frame in buffer pool
    Status tmp = allocBuf(fm);
    if(tmp == OK){
      //we successfully allocate a frame in the buffer pool
      //set this entry
      bufTable[fm].Set(file, pn);
      //we load this into the actual buffer pool entry
      if(file->readPage(pn, &bufPool[fm]) != OK){
	return UNIXERR;
      }
      else{
	//successfully read into the actual buffer pool
	//insert into hashTable
	Status tmp1 = hashTable->insert(file, pn, fm);
	if(tmp1 == OK){
	  //insertion into hashtable successful
	  //return the pageNo
	  pageNo = pn;
	  //return the page pointer
	  page = (bufPool+fm);
	}else{
	  //hash table err
	  return tmp1;
	}
      }
    }
    else{
      //unix error or bufferexceeded 
      return tmp;
    }
  }
  return OK;
}

/*
 * dipose a page in the buffer pool and in the file
 * @param *file, the file that contains the page needs to be diposed
 *        PageNo, the page number within the file that needs to be diposed
 * @return OK on success
 *         HASHNOTFOUND if the page is not in the buffer pool hash table
 *         UNIXERR on dispose failure in the file
 */
const Status BufMgr::disposePage(File* file, const int pageNo) {
  // TODO: Implement this method by looking at the description in the writeup.
  int frame;
  Status lookuphashtbl = hashTable->lookup(file, pageNo, frame);
  if(lookuphashtbl == OK){
    //clear the frame in the bufTable
    bufTable[frame].Clear();
    //no need error checking cuz we have found the page
    hashTable->remove(file,pageNo);
    //OK, unixerrr or badpageNo.
    return file->disposePage(pageNo);
  }else{
    //hash not found
    return lookuphashtbl;
  }
  return OK;
}


/*
 * flush the pages in the buffer pool belonging to the file; write back if dirty
 * clear the frame for the flushed page
 * @param *file, the file that contains the page needs to be flushed
 * @return OK on success
 *         PAGEPINNED if the page is pinned in the buffer
 *         UNIXERR on write back failure to the file
 */

const Status BufMgr::flushFile(const File* file) {
  // TODO: Implement this method by looking at the description in the writeup.
  for(int i = 0; i < numBufs; i++){
    if(bufTable[i].file == file){
      if(bufTable[i].pinCnt > 0){
	return PAGEPINNED;
      }
      if(bufTable[i].dirty){
	//write back
	Status writest = bufTable[i].file->writePage(bufTable[i].pageNo, &bufPool[i]);
	if(writest == OK){
	hashTable->remove(file, bufTable[i].pageNo);
	bufTable[i].Clear();
	}else{
	  return writest;
	}
      }
    }
  }
  return OK;
}

/*
 * For debug use; print the status of each buffer pool frame
 */


void BufMgr::printSelf(void) 
{
  BufDesc* tmpbuf;
  
  cout << endl << "Print buffer...\n";
  for (int i=0; i<numBufs; i++) {
    tmpbuf = &(bufTable[i]);
    cout << i << "\t" << (char*)(&bufPool[i]) 
	 << "actual page num " << tmpbuf->pageNo
	 << "\tpinCnt: " << tmpbuf->pinCnt;
    
    if (tmpbuf->valid == true)
      cout << "\tvalid\n";
    if (tmpbuf->refbit == true)
      cout << "\tref\n";
    else
      cout << "\tnot ref\n";
    cout << endl;
  };
}


