/**********************************
 * FILE NAME: MP1Node.cpp
 *
 * DESCRIPTION: Membership protocol run by this Node.
 * 				Definition of MP1Node class functions.
 **********************************/

#include "MP1Node.h"

/*
 * Note: You can change/add any functions in MP1Node.{h,cpp}
 */

/**
 * Overloaded Constructor of the MP1Node class
 * You can add new members to the class if you think it
 * is necessary for your logic to work
 */
MP1Node::MP1Node(Member *member, Params *params, EmulNet *emul, Log *log, Address *address) {
	for( int i = 0; i < 6; i++ ) {
		NULLADDR[i] = 0;
	}
	this->memberNode = member;
	this->emulNet = emul;
	this->log = log;
	this->par = params;
	this->memberNode->addr = *address;
}

/**
 * Destructor of the MP1Node class
 */
MP1Node::~MP1Node() {}

/**
 * FUNCTION NAME: recvLoop
 *
 * DESCRIPTION: This function receives message from the network and pushes into the queue
 * 				This function is called by a node to receive messages currently waiting for it
 */
int MP1Node::recvLoop() {
    if ( memberNode->bFailed ) {
    	return false;
    }
    else {
    	return emulNet->ENrecv(&(memberNode->addr), enqueueWrapper, NULL, 1, &(memberNode->mp1q));
    }
}

/**
 * FUNCTION NAME: enqueueWrapper
 *
 * DESCRIPTION: Enqueue the message from Emulnet into the queue
 */
int MP1Node::enqueueWrapper(void *env, char *buff, int size) {
	Queue q;
	return q.enqueue((queue<q_elt> *)env, (void *)buff, size);
}

/**
 * FUNCTION NAME: nodeStart
 *
 * DESCRIPTION: This function bootstraps the node
 * 				All initializations routines for a member.
 * 				Called by the application layer.
 */
void MP1Node::nodeStart(char *servaddrstr, short servport) {
    Address joinaddr;
    joinaddr = getJoinAddress();

    // Self booting routines
    if( initThisNode(&joinaddr) == -1 ) {
#ifdef DEBUGLOG
        log->LOG(&memberNode->addr, "init_thisnode failed. Exit.");
#endif
        exit(1);
    }

    if( !introduceSelfToGroup(&joinaddr) ) {
        finishUpThisNode();
#ifdef DEBUGLOG
        log->LOG(&memberNode->addr, "Unable to join self to group. Exiting.");
#endif
        exit(1);
    }

    return;
}

/**
 * FUNCTION NAME: initThisNode
 *
 * DESCRIPTION: Find out who I am and start up
 */
int MP1Node::initThisNode(Address *joinaddr) {
	/*
	 * This function is partially implemented and may require changes
	 */
	int id = *(int*)(&memberNode->addr.addr);
	int port = *(short*)(&memberNode->addr.addr[4]);

	memberNode->bFailed = false;
	memberNode->inited = true;
	memberNode->inGroup = false;
    // node is up!
	memberNode->nnb = 0;
	memberNode->heartbeat = 0;
	memberNode->pingCounter = TFAIL;
	memberNode->timeOutCounter = -1;
    initMemberListTable(memberNode);

    // Add reference to memberNode's self to membership list
    MemberListEntry myEntry = MemberListEntry(id, port, memberNode->heartbeat, par->getcurrtime());
    memberNode->memberList.push_back(myEntry);
    memberNode->myPos = memberNode->memberList.begin();

    return 0;
}

/**
 * FUNCTION NAME: introduceSelfToGroup
 *
 * DESCRIPTION: Join the distributed system
 */
int MP1Node::introduceSelfToGroup(Address *joinaddr) {
	MessageHdr *msg;
#ifdef DEBUGLOG
    static char s[1024];
#endif

    if ( 0 == memcmp((char *)&(memberNode->addr.addr), (char *)&(joinaddr->addr), sizeof(memberNode->addr.addr))) {
        // I am the group booter (first process to join the group). Boot up the group
#ifdef DEBUGLOG
        log->LOG(&memberNode->addr, "Starting up group...");
#endif
        memberNode->inGroup = true;
    }
    else {
        size_t msgsize = sizeof(MessageHdr) + sizeof(joinaddr->addr) + sizeof(long) + 1;
        msg = (MessageHdr *) malloc(msgsize * sizeof(char));

        // create JOINREQ message: format of data is {struct Address myaddr}
        msg->msgType = JOINREQ;
        memcpy((char *)(msg+1), &memberNode->addr.addr, sizeof(memberNode->addr.addr));
        memcpy((char *)(msg+1) + 1 + sizeof(memberNode->addr.addr), &memberNode->heartbeat, sizeof(long));

#ifdef DEBUGLOG
        sprintf(s, "Trying to join...");
        log->LOG(&memberNode->addr, s);
#endif

        // send JOINREQ message to introducer member
        emulNet->ENsend(&memberNode->addr, joinaddr, (char *)msg, msgsize);

        free(msg);
    }

    return 1;

}

/**
 * FUNCTION NAME: finishUpThisNode
 *
 * DESCRIPTION: Wind up this node and clean up state
 */
int MP1Node::finishUpThisNode(){
   memberNode->inGroup = false;
   initMemberListTable(memberNode);
}

/**
 * FUNCTION NAME: nodeLoop
 *
 * DESCRIPTION: Executed periodically at each member
 * 				Check your messages in queue and perform membership protocol duties
 */
void MP1Node::nodeLoop() {
    if (memberNode->bFailed) {
    	return;
    }

    // Check my messages
    checkMessages();

    // Wait until you're in the group...
    if( !memberNode->inGroup ) {
    	return;
    }

    // ...then jump in and share your responsibilites!
    nodeLoopOps();

    return;
}

/**
 * FUNCTION NAME: checkMessages
 *
 * DESCRIPTION: Check messages in the queue and call the respective message handler
 */
void MP1Node::checkMessages() {
    void *ptr;
    int size;

    // Pop waiting messages from memberNode's mp1q
    while ( !memberNode->mp1q.empty() ) {
    	ptr = memberNode->mp1q.front().elt;
    	size = memberNode->mp1q.front().size;
    	memberNode->mp1q.pop();
    	recvCallBack((void *)memberNode, (char *)ptr, size);
    }
    return;
}

/**
 * FUNCTION NAME: recvCallBack
 *
 * DESCRIPTION: Message handler for different message types
 */
bool MP1Node::recvCallBack(void *env, char *data, int size ) {
    long heartbeat;
    Address addr;
    MessageHdr *msg = (MessageHdr *) data;
    char * restOfMessage = (char *)(msg + 1);

    switch (msg->msgType)
    {
    case JOINREQ:
        memcpy(&(addr.addr), restOfMessage, sizeof(memberNode->addr.addr));
        
        // cout<<"Received join req at node: ";
        // printAddress(&memberNode->addr);
        // cout<< " from node: ";
        // printAddress(&addr);
        // cout << endl;

        memcpy(&heartbeat, restOfMessage + 1 + sizeof(memberNode->addr.addr), sizeof(heartbeat));
        addMember(addr, heartbeat);
        gossipMembershipToNode(JOINREP, &addr);
        gossipToAllNodes();
        break;
    case JOINREP:
        // cout<<"Received join reply at node: ";
        // printAddress(&memberNode->addr);
        // cout<< endl;


        processJoinReply(restOfMessage, size - sizeof(MessageHdr));
        break;
    case MEMSHPLIST:
        updateMemberList(restOfMessage, size - sizeof(MessageHdr));
        break;
    default:
        break;
    }
}

void MP1Node::updateMemberList(char * message, int size) {
    size_t memberListLength;
    // Read size of member list, move message pointer
    memcpy(&memberListLength, message, sizeof(size_t));
    message+= sizeof(size_t);

    MemberListEntry nextEntry;
    for (int i = 0; i < memberListLength; i++) {
        memcpy(&nextEntry, message, sizeof(MemberListEntry));
        addOrUpdateMember(nextEntry);
        message += sizeof(MemberListEntry);
    }
}

void MP1Node::addOrUpdateMember(MemberListEntry entry) {
    for (std::vector<MemberListEntry>::iterator it = memberNode->memberList.begin(); it != memberNode->memberList.end(); ++it) {
        if (it->getid() == entry.getid() && it->getport() == entry.getport()) {
            if (it->getheartbeat() < entry.getheartbeat()) {
                it->setheartbeat(entry.getheartbeat());
                it->settimestamp(par->getcurrtime());
            }
            return;
        }
    }

    // Address a;
    // addressFromIdAndPort(&a, entry.getid(), entry.getport());
    // cout<<"Adding member: ";
    // printAddress(&a);
    addMember(entry.getid(), entry.getport(), entry.getheartbeat());
}

void MP1Node::processJoinReply(char * message, int size) {
    size_t memberListLength;
    // Confirm we are now in the group
    memberNode->inGroup = true;

    // Read size of member list, move message pointer
    memcpy(&memberListLength, message, sizeof(size_t));
    message+= sizeof(size_t);
    updateMemberList((message + sizeof(size_t)), memberListLength);
}

void MP1Node::gossipMembershipToNode(MsgTypes msgType, Address *addr) {
    MessageHdr *msg;
    size_t memberListEntrySize = sizeof(MemberListEntry);
    size_t memberListLength = memberNode->memberList.size();
    
    // header + member list length + membership list
    size_t msgsize = sizeof(MessageHdr) + sizeof(long) + (memberListLength * memberListEntrySize);
    msg = (MessageHdr *) malloc(msgsize * sizeof(char));
    msg->msgType = msgType;
    memcpy((char *)(msg+1) , &memberListLength, sizeof(size_t));

    // iterate through memberlist 
    // append entry info to list
    int counter = 0;
    for (std::vector<MemberListEntry>::iterator it = memberNode->memberList.begin(); it != memberNode->memberList.end(); ++it) {
        // Already offset by MessageHdr + memberListLength
        // increment offset by MemberListEntry size for each entry
        memcpy((char *)(msg+1) + sizeof(size_t) + sizeof(MemberListEntry) * counter, &(*it), sizeof(MemberListEntry));
        counter++;
    }

    emulNet->ENsend(&memberNode->addr, addr, (char *) msg, msgsize);

    free(msg);
}

void MP1Node::addMember(int id, int port, long heartbeat) {
    memberNode->nnb += 1;
    MemberListEntry newMember = MemberListEntry(
        id,
        port,
        heartbeat,
        par->getcurrtime()
    );
    memberNode->memberList.push_back(newMember);
    Address addr;
    addressFromIdAndPort(&addr, id, port);
    log->logNodeAdd(&memberNode->addr, &addr);
}

void MP1Node::addMember(Address addr, long heartbeat) {
    int id = *(int*)(&addr.addr[0]);
	short port = *(short*)(&addr.addr[4]);
    addMember(id, port, heartbeat);
}

/**
 * FUNCTION NAME: nodeLoopOps
 *
 * DESCRIPTION: Check if any node hasn't responded within a timeout period and then delete
 * 				the nodes
 * 				Propagate your membership list
 */
void MP1Node::nodeLoopOps() {
    Address addr;
    memberNode->heartbeat++;
    // Remove nodes that have timed out past TREMOVE
    for (std::vector<MemberListEntry>::iterator it = memberNode->memberList.begin(); it != memberNode->memberList.end();) {
        addressFromIdAndPort(&addr, it->getid(), it->getport());
        // Update own hearbeat
        if (addr == memberNode->addr) {
            it->setheartbeat(memberNode->heartbeat);
            it->settimestamp(par->getcurrtime());
        }
        
        // check timestamp of last membership update
        // If past the timeout period then we remove
        if (par->getcurrtime() - it->gettimestamp() > TREMOVE) {
            // cout<<"last timestamp: " << it->gettimestamp() << " - Current: "<< par->getcurrtime() << " removing node: ";
            // printAddress(&addr);
            // cout<<"From list of node: ";
            // printAddress(&memberNode->addr);
            memberNode->nnb--;
            log->logNodeRemove(&memberNode->addr, &addr);
            memberNode->memberList.erase(it);
        } else {
            ++it;
        }

    }

    memberNode->pingCounter--;

    if (memberNode->pingCounter == 0) {
        memberNode->pingCounter = TFAIL;
        gossipToAllNodes();
    }
}

void MP1Node::gossipToAllNodes() {
    Address addr;
    for (std::vector<MemberListEntry>::iterator it = memberNode->memberList.begin(); it != memberNode->memberList.end(); ++it) {
        addressFromIdAndPort(&addr, it->getid(), it->getport());
        gossipMembershipToNode(MEMSHPLIST, &addr);
    }
}

void MP1Node::addressFromIdAndPort(Address *addr, int id, short port) {
    memcpy(&addr->addr[0], &id,  sizeof(int));
    memcpy(&addr->addr[4], &port, sizeof(short));
}

/**
 * FUNCTION NAME: isNullAddress
 *
 * DESCRIPTION: Function checks if the address is NULL
 */
int MP1Node::isNullAddress(Address *addr) {
	return (memcmp(addr->addr, NULLADDR, 6) == 0 ? 1 : 0);
}

/**
 * FUNCTION NAME: getJoinAddress
 *
 * DESCRIPTION: Returns the Address of the coordinator
 */
Address MP1Node::getJoinAddress() {
    Address joinaddr;

    memset(&joinaddr, 0, sizeof(Address));
    *(int *)(&joinaddr.addr) = 1;
    *(short *)(&joinaddr.addr[4]) = 0;

    return joinaddr;
}

/**
 * FUNCTION NAME: initMemberListTable
 *
 * DESCRIPTION: Initialize the membership list
 */
void MP1Node::initMemberListTable(Member *memberNode) {
	memberNode->memberList.clear();
}

/**
 * FUNCTION NAME: printAddress
 *
 * DESCRIPTION: Print the Address
 */
void MP1Node::printAddress(Address *addr)
{
    printf("%d.%d.%d.%d:%d \n",  addr->addr[0],addr->addr[1],addr->addr[2],
                                                       addr->addr[3], *(short*)&addr->addr[4]) ;    
}
