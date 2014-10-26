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
  Destructor-write the dirty page in the buffer pool back to disk and free all the
  memory that buffer pool used
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
  delete bufTable;
  //free actually buffer pool
  delete bufPool;
  //free hashtable
  delete hashTable;
}

/*
  Allocates a free frame usign the clock algorithm; if necessary writing a dirty page
  back to disk
  @param int &frame the allocated frameNo
  @return OK on success
          BUFFEREXCEEDED if all buffer frames are pinned
	  UNIXERR if the call to the I/O returned an error when a dirty page to disk
 */
const Status BufMgr::allocBuf(int & frame) {
  // TODO: Implement this method by looking at the description in the writeup.
  // we only advance numBufs times clockhand; if we dont find anything, then return 
  // BUFFEREXCEEDED
  int numPin = 0;
  while(1){
    //clock algo
    advanceClock();
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
	      bufTable[clockHand].Clear();
	      //mark it valid
	      bufTable[clockHand].valid = true;
	      //give the frameNo out to the caller for further use
	      frame = clockHand;
	      return OK;
	} //end of not pinned -> true
	//this frame is pinned
	else{
	  //increment the number of pinned frame
	  if(numPin++ == numBufs){
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


const Status BufMgr::readPage(File* file, const int PageNo, Page*& page) {
  // TODO: Implement this method by looking at the description in the writeup.
  return OK;
}


const Status BufMgr::unPinPage(File* file, const int PageNo, 
			       const bool dirty) {
  // TODO: Implement this method by looking at the description in the writeup.
  return OK;
}


const Status BufMgr::allocPage(File* file, int& pageNo, Page*& page)  {
  // TODO: Implement this method by looking at the description in the writeup.
  return OK;
}


const Status BufMgr::disposePage(File* file, const int pageNo) {
  // TODO: Implement this method by looking at the description in the writeup.
  return OK;
}


const Status BufMgr::flushFile(const File* file) {
  // TODO: Implement this method by looking at the description in the writeup.
  return OK;
}


void BufMgr::printSelf(void) 
{
  BufDesc* tmpbuf;
  
  cout << endl << "Print buffer...\n";
  for (int i=0; i<numBufs; i++) {
    tmpbuf = &(bufTable[i]);
    cout << i << "\t" << (char*)(&bufPool[i]) 
	 << "\tpinCnt: " << tmpbuf->pinCnt;
    
    if (tmpbuf->valid == true)
      cout << "\tvalid\n";
    cout << endl;
  };
}


