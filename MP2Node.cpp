/**********************************
 * FILE NAME: MP2Node.cpp
 *
 * DESCRIPTION: MP2Node class definition
 **********************************/
#include "MP2Node.h"

/**
 * constructor
 */
MP2Node::MP2Node(Member *memberNode, Params *par, EmulNet *emulNet, Log *log, Address *address)
{
	this->memberNode = memberNode;
	this->par = par;
	this->emulNet = emulNet;
	this->log = log;
	ht = new HashTable();
	this->memberNode->addr = *address;
}

/**
 * Destructor
 */
MP2Node::~MP2Node()
{
	delete ht;
	delete memberNode;
}

/**
 * FUNCTION NAME: updateRing
 *
 * DESCRIPTION: This function does the following:
 * 				1) Gets the current membership list from the Membership Protocol (MP1Node)
 * 				   The membership list is returned as a vector of Nodes. See Node class in Node.h
 * 				2) Constructs the ring based on the membership list
 * 				3) Calls the Stabilization Protocol
 */
void MP2Node::updateRing()
{
	/*
	 * Implement this. Parts of it are already implemented
	 */
	vector<Node> curMemList;

	/*
	 *  Step 1. Get the current membership list from Membership Protocol / MP1
	 */
	curMemList = getMembershipList();

	/*
	 * Step 2: Construct the ring
	 */
	// Sort the list based on the hashCode
	sort(curMemList.begin(), curMemList.end());

	/*
	 * Step 3: Run the stabilization protocol IF REQUIRED
	 */
	// Run stabilization protocol if the hash table size is greater than zero and if there has been a changed in the ring
	bool ringChanged = false;
	// different length guarantees the change
	if (ring.size() != curMemList.size())
	{
		ringChanged = true;
	}
	// If same length, go through sorted list and look for changes
	else
	{
		for (int i = 0; i < ring.size(); i++)
		{
			if (ring[i].getHashCode() != curMemList[i].getHashCode())
			{
				ringChanged = true;
			}
		}
	}

	if (ringChanged)
	{
		ring = curMemList;
		stabilizationProtocol();
	}
}

/**
 * FUNCTION NAME: getMemberhipList
 *
 * DESCRIPTION: This function goes through the membership list from the Membership protocol/MP1 and
 * 				i) generates the hash code for each member
 * 				ii) populates the ring member in MP2Node class
 * 				It returns a vector of Nodes. Each element in the vector contain the following fields:
 * 				a) Address of the node
 * 				b) Hash code obtained by consistent hashing of the Address
 */
vector<Node> MP2Node::getMembershipList()
{
	unsigned int i;
	vector<Node> curMemList;
	for (i = 0; i < this->memberNode->memberList.size(); i++)
	{
		Address addressOfThisMember;
		int id = this->memberNode->memberList.at(i).getid();
		short port = this->memberNode->memberList.at(i).getport();
		memcpy(&addressOfThisMember.addr[0], &id, sizeof(int));
		memcpy(&addressOfThisMember.addr[4], &port, sizeof(short));
		curMemList.emplace_back(Node(addressOfThisMember));
	}
	return curMemList;
}

/**
 * FUNCTION NAME: hashFunction
 *
 * DESCRIPTION: This functions hashes the key and returns the position on the ring
 * 				HASH FUNCTION USED FOR CONSISTENT HASHING
 *
 * RETURNS:
 * size_t position on the ring
 */
size_t MP2Node::hashFunction(string key)
{
	std::hash<string> hashFunc;
	size_t ret = hashFunc(key);
	return ret % RING_SIZE;
}

/**
 * FUNCTION NAME: clientCreate
 *
 * DESCRIPTION: client side CREATE API
 * 				The function does the following:
 * 				1) Constructs the message
 * 				2) Finds the replicas of this key
 * 				3) Sends a message to the replica
 */
void MP2Node::clientCreate(string key, string value)
{
	/*
	 * Implement this
	 */
	int transaction_id = g_transID++;
	vector<Node> replicas = findNodes(key);
	Address myAddr = memberNode->addr.getAddress();
	TransactionEntry t;
	t.failCount = 0;
	t.initTimestamp = par->getcurrtime();
	t.successCount = 0;
	t.transType = CREATE;
	t.key = key;
	t.value = value;
	transactionLog.emplace(transaction_id, t);

	for (int i = 0; i < replicas.size(); i++)
	{
		ReplicaType replicaType;
		if (i == 0)
		{
			replicaType = PRIMARY;
		}
		else if (i == 1)
		{
			replicaType = SECONDARY;
		}
		else
		{
			replicaType = TERTIARY;
		}

		Message msg = Message(transaction_id, myAddr, CREATE, key, value, replicaType);
		emulNet->ENsend(&myAddr, replicas[i].getAddress(), msg.toString());
	}
}

ReplicaType MP2Node::getReplicaType(int index)
{
	if (index == 0)
	{
		return PRIMARY;
	}
	else if (index == 1)
	{
		return SECONDARY;
	}
	else
	{
		return TERTIARY;
	}
}

/**
 * FUNCTION NAME: clientRead
 *
 * DESCRIPTION: client side READ API
 * 				The function does the following:
 * 				1) Constructs the message
 * 				2) Finds the replicas of this key
 * 				3) Sends a message to the replica
 */
void MP2Node::clientRead(string key)
{
	/*
	 * Implement this
	 */

	int transaction_id = g_transID++;
	TransactionEntry t;
	t.failCount = 0;
	t.successCount = 0;
	t.initTimestamp = par->getcurrtime();
	t.transType = READ;
	t.key = key;
	transactionLog.emplace(transaction_id, t);

	Address myAddr = memberNode->addr.getAddress();
	Message msg = Message(transaction_id, myAddr, READ, key);
	vector<Node> replicas = findNodes(key);

	for (int i = 0; i < replicas.size(); i++)
	{
		emulNet->ENsend(&myAddr, replicas[i].getAddress(), msg.toString());
	}
}

/**
 * FUNCTION NAME: clientUpdate
 *
 * DESCRIPTION: client side UPDATE API
 * 				The function does the following:
 * 				1) Constructs the message
 * 				2) Finds the replicas of this key
 * 				3) Sends a message to the replica
 */
void MP2Node::clientUpdate(string key, string value)
{
	/*
	 * Implement this
	 */

	int transaction_id = g_transID++;
	TransactionEntry t;
	t.failCount = 0;
	t.successCount = 0;
	t.initTimestamp = par->getcurrtime();
	t.transType = UPDATE;
	t.key = key;
	t.value = value;
	transactionLog.emplace(transaction_id, t);

	Address myAddr = memberNode->addr.getAddress();
	vector<Node> replicas = findNodes(key);

	for (int i = 0; i < replicas.size(); i++)
	{
		ReplicaType replicaType = getReplicaType(i);
		// Message(int _transID, Address _fromAddr, MessageType _type, string _key, string _value, ReplicaType _replica);
		Message msg = Message(transaction_id, myAddr, UPDATE, key, value, replicaType);
		emulNet->ENsend(&myAddr, replicas[i].getAddress(), msg.toString());
	}
}

/**
 * FUNCTION NAME: clientDelete
 *
 * DESCRIPTION: client side DELETE API
 * 				The function does the following:
 * 				1) Constructs the message
 * 				2) Finds the replicas of this key
 * 				3) Sends a message to the replica
 */
void MP2Node::clientDelete(string key)
{
	/*
	 * Implement this
	 */
	int transaction_id = g_transID++;
	TransactionEntry t;
	t.failCount = 0;
	t.successCount = 0;
	t.initTimestamp = par->getcurrtime();
	t.transType = DELETE;
	t.key = key;
	transactionLog.emplace(transaction_id, t);

	Address myAddr = memberNode->addr.getAddress();
	vector<Node> replicas = findNodes(key);

	// Message(int _transID, Address _fromAddr, MessageType _type, string _key);
	Message msg = Message(transaction_id, myAddr, DELETE, key);

	for (int i = 0; i < replicas.size(); i++)
	{
		emulNet->ENsend(&myAddr, replicas[i].getAddress(), msg.toString());
	}
}

/**
 * FUNCTION NAME: createKeyValue
 *
 * DESCRIPTION: Server side CREATE API
 * 			   	The function does the following:
 * 			   	1) Inserts key value into the local hash table
 * 			   	2) Return true or false based on success or failure
 */
bool MP2Node::createKeyValue(string key, string value, ReplicaType replica)
{
	/*
	 * Implement this
	 */
	// Insert key, value, replicaType into the hash table
	Entry entry(value, par->getcurrtime(), replica);
	// cout << "Created key " << key << endl;
	return ht->create(key, entry.convertToString());
}

/**
 * FUNCTION NAME: readKey
 *
 * DESCRIPTION: Server side READ API
 * 			    This function does the following:
 * 			    1) Read key from local hash table
 * 			    2) Return value
 */
string MP2Node::readKey(string key)
{
	/*
	 * Implement this
	 */
	// Read key from local hash table and return value
	return ht->read(key);
}

/**
 * FUNCTION NAME: updateKeyValue
 *
 * DESCRIPTION: Server side UPDATE API
 * 				This function does the following:
 * 				1) Update the key to the new value in the local hash table
 * 				2) Return true or false based on success or failure
 */
bool MP2Node::updateKeyValue(string key, string value, ReplicaType replica)
{
	/*
	 * Implement this
	 */
	// Update key in local hash table and return true or false
	Entry entry(value, par->getcurrtime(), replica);
	return ht->update(key, entry.convertToString());
}

/**
 * FUNCTION NAME: deleteKey
 *
 * DESCRIPTION: Server side DELETE API
 * 				This function does the following:
 * 				1) Delete the key from the local hash table
 * 				2) Return true or false based on success or failure
 */
bool MP2Node::deletekey(string key)
{
	/*
	 * Implement this
	 */
	// Delete the key from the local hash table
	return ht->deleteKey(key);
}

/**
 * FUNCTION NAME: checkMessages
 *
 * DESCRIPTION: This function is the message handler of this node.
 * 				This function does the following:
 * 				1) Pops messages from the queue
 * 				2) Handles the messages according to message types
 */
void MP2Node::checkMessages()
{
	/*
	 * Implement this. Parts of it are already implemented
	 */
	char *data;
	int size;

	/*
	 * Declare your local variables here
	 */

	// dequeue all messages and handle them
	while (!memberNode->mp2q.empty())
	{
		/*
		 * Pop a message from the queue
		 */
		data = (char *)memberNode->mp2q.front().elt;
		size = memberNode->mp2q.front().size;
		memberNode->mp2q.pop();

		string message(data, data + size);

		/*
		 * Handle the message types here
		 */

		Message msg(message);

		switch (msg.type)
		{
		case CREATE:
			handleCreate(msg);
			break;
		case READ:
			handleRead(msg);
			break;
		case UPDATE:
			handleUpdate(msg);
			break;
		case DELETE:
			handleDelete(msg);
			break;
		case REPLY:
			handleReply(msg);
			break;
		case READREPLY:
			handleReadReply(msg);
			break;
		default:
			cout << "Unrecognized Message Type" << endl;
			break;
		}
	}

	/*
	 * This function should also ensure all READ and UPDATE operation
	 * get QUORUM replies
	 */

	failOldTransactions();
}

void MP2Node::failOldTransactions()
{
	// Find all transactions that have not failed or succeeded within the given timeout period
	// automaticallly fail the transaction
	for (map<int, TransactionEntry>::iterator it = transactionLog.begin(); it != transactionLog.end(); ++it)
	{
		TransactionEntry txn(it->second);

		if ((par->getcurrtime() - txn.initTimestamp) > REQUEST_TIMEOUT && !txn.transactionCommitted && !txn.transactionFailed)
		{
			Address myAddr = memberNode->addr;

			switch (txn.transType)
			{
			case CREATE:
				log->logCreateFail(&myAddr, true, it->first, txn.key, txn.value);
				break;
			case READ:
				log->logReadFail(&myAddr, true, it->first, txn.key);
				break;
			case UPDATE:
				log->logUpdateFail(&myAddr, true, it->first, txn.key, txn.value);
				break;
			case DELETE:
				log->logDeleteFail(&myAddr, true, it->first, txn.key);
				break;

			default:
				break;
			}

			txn.transactionFailed = true;
			transactionLog.at(it->first) = txn;
		}
	}
}

void MP2Node::handleCreate(Message msg)
{
	bool success = createKeyValue(msg.key, msg.value, msg.replica);
	Address myAddr = memberNode->addr;

	if (success)
	{
		log->logCreateSuccess(&myAddr, false, msg.transID, msg.key, msg.value);
	}
	else
	{
		log->logCreateFail(&myAddr, false, msg.transID, msg.key, msg.value);
	}

	Message createReply(msg.transID, myAddr, REPLY, success);
	emulNet->ENsend(&myAddr, &msg.fromAddr, createReply.toString());
}

void MP2Node::handleRead(Message msg)
{
	string value = readKey(msg.key);
	Address myAddr = memberNode->addr;

	if (value.empty())
	{
		log->logReadFail(&myAddr, false, msg.transID, msg.key);
	}
	else
	{
		log->logReadSuccess(&myAddr, false, msg.transID, msg.key, value);
	}

	// Message(int _transID, Address _fromAddr, string _value);
	Message readReply(msg.transID, myAddr, value);
	emulNet->ENsend(&myAddr, &msg.fromAddr, readReply.toString());
}

void MP2Node::handleUpdate(Message msg)
{
	bool success = updateKeyValue(msg.key, msg.value, msg.replica);
	Address myAddr = memberNode->addr;

	if (success)
	{
		log->logUpdateSuccess(&myAddr, false, msg.transID, msg.key, msg.value);
	}
	else
	{
		log->logUpdateFail(&myAddr, false, msg.transID, msg.key, msg.value);
	}

	// construct reply message
	// Message(int _transID, Address _fromAddr, MessageType _type, bool _success);
	Message updateReply(msg.transID, myAddr, REPLY, success);
	emulNet->ENsend(&myAddr, &msg.fromAddr, updateReply.toString());
}

void MP2Node::handleDelete(Message msg)
{
	bool success = deletekey(msg.key);
	Address myAddr = memberNode->addr;

	if (success)
	{
		log->logDeleteSuccess(&myAddr, false, msg.transID, msg.key);
	}
	else
	{
		log->logDeleteFail(&myAddr, false, msg.transID, msg.key);
	}

	// construct reply message
	// Message(int _transID, Address _fromAddr, MessageType _type, bool _success);
	Message deleteReply(msg.transID, myAddr, REPLY, success);
	emulNet->ENsend(&myAddr, &msg.fromAddr, deleteReply.toString());
}

void MP2Node::handleReply(Message msg)
{
	if (msg.transID >= 0)
	{
		vector<Node> replicas = findNodes(msg.key);
		TransactionEntry t = transactionLog.find(msg.transID)->second;

		if (msg.success)
		{
			t.successCount++;
		}
		else
		{
			t.failCount++;
		}

		// QUORUM of success messages
		if (t.successCount > (replicas.size() / 2) && !t.transactionCommitted)
		{
			t.transactionCommitted = true;
			logSuccess(msg.transID, t);
		}
		else if (t.failCount > (replicas.size() / 2) && !t.transactionFailed)
		{
			t.transactionFailed = true;
			logFailure(msg.transID, t);
		}

		transactionLog.at(msg.transID) = t;
	}
}

void MP2Node::logSuccess(int transID, TransactionEntry entry)
{
	Address myAddr = memberNode->addr.getAddress();

	switch (entry.transType)
	{
	case CREATE:
		log->logCreateSuccess(&myAddr, true, transID, entry.key, entry.value);
		break;
	case UPDATE:
		log->logUpdateSuccess(&myAddr, true, transID, entry.key, entry.value);
		break;
	case DELETE:
		log->logDeleteSuccess(&myAddr, true, transID, entry.key);
		break;

	default:
		break;
	}
}

void MP2Node::logFailure(int transID, TransactionEntry entry)
{
	Address myAddr = memberNode->addr.getAddress();

	switch (entry.transType)
	{
	case CREATE:
		log->logCreateFail(&myAddr, true, transID, entry.key, entry.value);
		break;
	case UPDATE:
		log->logUpdateFail(&myAddr, true, transID, entry.key, entry.value);
		break;
	case DELETE:
		log->logDeleteFail(&myAddr, true, transID, entry.key);
		break;

	default:
		break;
	}
}

void MP2Node::handleReadReply(Message msg)
{
	vector<Node> replicas = findNodes(msg.key);
	TransactionEntry t = transactionLog.find(msg.transID)->second;

	if (msg.success)
	{
		t.successCount++;
	}
	else
	{
		t.failCount++;
	}

	Address myAddr = memberNode->addr.getAddress();
	// QUORUM of success messages
	if (t.successCount > (replicas.size() / 2) && !t.transactionCommitted)
	{
		Entry entry(msg.value);
		log->logReadSuccess(&myAddr, true, msg.transID, msg.key, entry.value);
		t.transactionCommitted = true;
	}
	else if (t.failCount > (replicas.size() / 2) && !t.transactionFailed)
	{
		log->logReadFail(&myAddr, true, msg.transID, msg.key);
		t.transactionFailed = true;
	}

	transactionLog.at(msg.transID) = t;
}

/**
 * FUNCTION NAME: findNodes
 *
 * DESCRIPTION: Find the replicas of the given keyfunction
 * 				This function is responsible for finding the replicas of a key
 */
vector<Node> MP2Node::findNodes(string key)
{
	size_t pos = hashFunction(key);
	vector<Node> addr_vec;
	if (ring.size() >= 3)
	{
		// if pos <= min || pos > max, the leader is the min
		if (pos <= ring.at(0).getHashCode() || pos > ring.at(ring.size() - 1).getHashCode())
		{
			addr_vec.emplace_back(ring.at(0));
			addr_vec.emplace_back(ring.at(1));
			addr_vec.emplace_back(ring.at(2));
		}
		else
		{
			// go through the ring until pos <= node
			for (int i = 1; i < ring.size(); i++)
			{
				Node addr = ring.at(i);
				if (pos <= addr.getHashCode())
				{
					addr_vec.emplace_back(addr);
					addr_vec.emplace_back(ring.at((i + 1) % ring.size()));
					addr_vec.emplace_back(ring.at((i + 2) % ring.size()));
					break;
				}
			}
		}
	}
	return addr_vec;
}

/**
 * FUNCTION NAME: recvLoop
 *
 * DESCRIPTION: Receive messages from EmulNet and push into the queue (mp2q)
 */
bool MP2Node::recvLoop()
{
	if (memberNode->bFailed)
	{
		return false;
	}
	else
	{
		return emulNet->ENrecv(&(memberNode->addr), this->enqueueWrapper, NULL, 1, &(memberNode->mp2q));
	}
}

/**
 * FUNCTION NAME: enqueueWrapper
 *
 * DESCRIPTION: Enqueue the message from Emulnet into the queue of MP2Node
 */
int MP2Node::enqueueWrapper(void *env, char *buff, int size)
{
	Queue q;
	return q.enqueue((queue<q_elt> *)env, (void *)buff, size);
}

/**
 * FUNCTION NAME: stabilizationProtocol
 *
 * DESCRIPTION: This runs the stabilization protocol in case of Node joins and leaves
 * 				It ensures that there always 3 copies of all keys in the DHT at all times
 * 				The function does the following:
 *				1) Ensures that there are three "CORRECT" replicas of all the keys in spite of failures and joins
 *				Note:- "CORRECT" replicas implies that every key is replicated in its two neighboring nodes in the ring
 */
void MP2Node::stabilizationProtocol()
{
	/*
	 * Implement this
	 */

	int myIndex;
	vector<Node> nowHasMyReplicas;
	vector<Node> nowHaveReplicasOf;

	// find index of current node;
	// from there we can identify the nodes which memberNode is storing replicas for
	// as well as the nodes replicating memberNode's keys
	for (int i = 0; i < ring.size(); i++)
	{
		if (ring[i].getAddress()->getAddress() == memberNode->addr.getAddress())
		{
			myIndex = i;
			// immediate next neighbor should be SECONDARY
			int secondaryReplicaIndex = (i + 1) % ring.size();
			nowHasMyReplicas.push_back(ring[secondaryReplicaIndex]);
			// 2nd next neighbor should be TERTIARY
			int tertiaryReplicaIndex = (i + 2) % ring.size();
			nowHasMyReplicas.push_back(ring[tertiaryReplicaIndex]);

			int primaryOneIndex = (ring.size() + i - 1) % ring.size();
			int primaryTwoIndex = (ring.size() + i - 2) % ring.size();
			nowHaveReplicasOf.push_back(ring[primaryOneIndex]);
			nowHaveReplicasOf.push_back(ring[primaryTwoIndex]);

			break;
		}
	}

	/**
	 * Check for if any PRIMARY nodes are failed;
	 * 	- if a PRIMARY failed and memberNode was previously SECONDARY, it should become PRIMARY
	 * 	- if a PRIMARY failed and memberNode was previously TERTIARY, it should become SECONDARY
	 */
	for (int i = 0; i < haveReplicasOf.size(); i++)
	{
		ReplicaType rt = getReplicaType(i + 1);
		// If no longer in the replicas list, one of our PRIMARY replicas has failed
		if (!hasNodeInList(nowHaveReplicasOf, haveReplicasOf[i]))
		{
			for (map<string, string>::iterator it = ht->hashTable.begin(); it != ht->hashTable.end(); ++it)
			{
				Entry e(it->second);
				if (e.replica == rt)
				{
					ReplicaType newRt;
					if (rt == SECONDARY)
					{
						newRt = PRIMARY;
					}
					else
					{
						newRt = SECONDARY;
					}

					updateKeyValue(it->first, e.value, newRt);
				}
			}
		}
	}

	/**
	 * Replicate PRIMARY HashTable entries to replicas if necessary
	 */
	for (int i = 0; i < nowHasMyReplicas.size(); i++)
	{
		ReplicaType rt = getReplicaType(i + 1);
		MessageType mt;
		// either there is not an entry at that index currently for the existing replicas,
		// or it is a different node now
		if (hasMyReplicas.size() < (i + 1) || hasMyReplicas[i].getAddress()->getAddress() != nowHasMyReplicas[i].getAddress()->getAddress())
		{
			// Node was in the list, but is now a different ReplicaType so we have to update the entry's ReplicaType
			if (hasNodeInList(hasMyReplicas, nowHasMyReplicas[i]))
			{
				// cout << "Moved positions " << endl;
				mt = UPDATE;
			}
			// Node is new the the hasMyReplicas list so we have to create the keys on this new replica
			else
			{
				// cout << "Adding stuff " << endl;
				mt = CREATE;
			}

			// For each HashTable entry that is PRIMARY to the given memberNode
			// send out the create/update message to the given replica
			for (map<string, string>::iterator it = ht->hashTable.begin(); it != ht->hashTable.end(); ++it)
			{
				Entry e(it->second);

				if (e.replica == PRIMARY)
				{
					Message msg(-1, memberNode->addr, mt, it->first, e.value, rt);
					emulNet->ENsend(&memberNode->addr, nowHasMyReplicas[i].getAddress(), msg.toString());
				}
			}
		}
	}

	// update our replicas vectors to reflect the current state of the ring
	hasMyReplicas = nowHasMyReplicas;
	haveReplicasOf = nowHaveReplicasOf;
}

bool MP2Node::hasNodeInList(vector<Node> entries, Node node)
{
	for (int i = 0; i < entries.size(); i++)
	{
		if (entries[i].getAddress() == node.getAddress())
			return true;
	}

	return false;
}
