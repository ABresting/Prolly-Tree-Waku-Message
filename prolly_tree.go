package main

import (
	"crypto/sha256"
	"encoding/hex"
	"strconv"
	"fmt"
)

// GreaterThan checks if one node is 'greater than' another.
func (n *Node) GreaterThan(other *Node) bool {
    return n.isTail || n.timestamp > other.timestamp
}

// LessThan checks if one node is 'less than' another.
func (n *Node) LessThan(other *Node) bool {
    return (!n.isTail) && n.timestamp < other.timestamp
}

// EqualTo checks if one node is 'equal to' another.
func (n *Node) EqualTo(other *Node) bool {
    return n.timestamp == other.timestamp
}

// NotEqualTo checks if one node is 'not equal to' another.
func (n *Node) NotEqualTo(other *Node) bool {
    return n.timestamp != other.timestamp
}

// GreaterThanOrEqualTo checks if one node is 'greater than or equal to' another.
func (n *Node) GreaterThanOrEqualTo(other *Node) bool {
    return n.isTail || n.timestamp >= other.timestamp
}

// LessThanOrEqualTo checks if one node is 'less than or equal to' another.
func (n *Node) LessThanOrEqualTo(other *Node) bool {
    return (!n.isTail) && n.timestamp <= other.timestamp
}

// CalculateHash computes the SHA256 hash of a given string.
func CalculateHash(hashableStr string) string {
	hasher := sha256.New()
	hasher.Write([]byte(hashableStr))
	return hex.EncodeToString(hasher.Sum(nil))
}

// IsBoundaryHash checks if the last hex digit of the hash is below a certain threshold.
func IsBoundaryHash(hashValue string, threshold int) bool {
	// Convert the last hex digit of the hash to an integer.
	lastHexDigit := hashValue[len(hashValue)-1:]
	hashInt, _ := strconv.ParseInt(lastHexDigit, 16, 64)
	return int(hashInt) < threshold
}

// BucketHash calculates the Merkel hash of a bucket of nodes by concatenating their Merkel hashes.
func BucketHash(nodes []*Node) string {
	// Concatenate the merkel hashes of the nodes.
	var hashStr string
	for _, node := range nodes {
		hashStr += node.merkelHash
	}
	// Calculate the hash of the concatenated string.
	return CalculateHash(hashStr)
}

// Node represents each node in the tree structure.
type Node struct {
	timestamp   int
	data        string
	nodeHash    string
	level       int
	up          *Node
	down        *Node
	left        *Node
	right       *Node
	merkelHash  string
	boundary    *bool // using a pointer to differentiate between unset and false
	isTail      bool
}

// NewNode creates and initializes a new Node.
func NewNode(data string, timestamp int, isTail bool) *Node {
	n := &Node{
		timestamp:  timestamp,
		data:       data,
		isTail:     isTail,
		level:      0,
		up:         nil,
		down:       nil,
		left:       nil,
		right:      nil,
		merkelHash: CalculateHash(data + strconv.Itoa(timestamp)),
		nodeHash:   CalculateHash(data + strconv.Itoa(timestamp)),
		boundary:   nil,
	}
	return n
}

// CreateHigherLevelNode creates a new node one level higher.
func (n *Node) CreateHigherLevelNode() *Node {
	newNode := NewNode("", n.timestamp, n.isTail)
	newNode.level = n.level + 1
	newNode.down = n
	n.up = newNode
	newNode.nodeHash = CalculateHash(n.nodeHash)
	return newNode
}

// FillMerkelHash fills the merkel hash of the node by going down walking left till you hit the boundary.
func (n *Node) FillMerkelHash() {
	var bucketNodes []*Node
	node := n.down
	bucketNodes = append(bucketNodes, node)

	for node.left != nil {
		if node.left.IsBoundaryNode() {
			break
		}
		node = node.left
		bucketNodes = append(bucketNodes, node)
	}

	for _, node := range bucketNodes {
		if node.merkelHash == "" {
			node.FillMerkelHash()
		}
	}

	// Reverse the slice of nodes.
	for i, j := 0, len(bucketNodes)-1; i < j; i, j = i+1, j-1 {
		bucketNodes[i], bucketNodes[j] = bucketNodes[j], bucketNodes[i]
	}

	n.merkelHash = BucketHash(bucketNodes)
}

// IsBoundaryNode checks if the node is a boundary node.
func (n *Node) IsBoundaryNode() bool {
	if n.boundary != nil {
		return *n.boundary
	}
	boundary := n.isTail || IsBoundaryHash(n.nodeHash, 7) // Assuming the threshold is 7
	n.boundary = &boundary
	return boundary
}

// FindNextBoundaryNode finds the next boundary node by going right till you hit the boundary node.
func (n *Node) FindNextBoundaryNode() *Node {
	node := n
	for node.right != nil {
		if node.right.IsBoundaryNode() {
			return node.right
		}
		node = node.right
	}
	return node
}

// Level represents a level in the ProllyTree.
type Level struct {
	level int
	tail  *Node
}

// NewLevel creates a new Level instance.
func NewLevel(lvl int) *Level {
	return &Level{level: lvl}
}

// ToList converts a level's nodes to a list.
func (l *Level) ToList() []*Node {
	var nodes []*Node
	node := l.tail
	for node != nil {
		nodes = append([]*Node{node}, nodes...) // Prepend to reverse
		node = node.left
	}
	return nodes
}

// String provides a string representation of the level.
func (l *Level) String() string {
	var timestamps []int
	for _, node := range l.ToList() {
		timestamps = append(timestamps, node.timestamp)
	}
	return " <=> " + fmt.Sprint(timestamps)
}

// BaseLevel creates a base level from a list of messages.
func BaseLevel(messages []*Message) *Level {
	level := NewLevel(0)
	var nodes []*Node
	for _, m := range messages {
		nodes = append(nodes, NewNode(m.data, m.timestamp, false))
	}
	fakeTail := NewNode("Tail", -1, true)
	nodes = append(nodes, fakeTail)
	level.tail = LinkNodes(nodes)[len(nodes)-1]
	return level
}

// NextLevel creates a level from the previous level.
func NextLevel(prevLevel *Level) *Level {
	nodes := prevLevel.ToList()
	var eligibleNodes []*Node
	for _, node := range nodes {
		if node.IsBoundaryNode() {
			eligibleNodes = append(eligibleNodes, node.CreateHigherLevelNode())
		}
	}
	linkedNodes := LinkNodes(eligibleNodes)
	for _, node := range linkedNodes {
		node.FillMerkelHash()
	}
	newLevel := NewLevel(prevLevel.level + 1)
	newLevel.tail = linkedNodes[len(linkedNodes)-1]
	return newLevel
}

// LinkNodes links nodes together in a level.
func LinkNodes(nodes []*Node) []*Node {
	for i := 0; i < len(nodes)-1; i++ {
		nodes[i].right = nodes[i+1]
		nodes[i+1].left = nodes[i]
	}
	return nodes
}

// ProllyTree represents the entire tree structure.
type ProllyTree struct {
	levels []*Level
}

// NewProllyTree creates a new ProllyTree instance with the given messages.
func NewProllyTree(messages []*Message) *ProllyTree {
	tree := &ProllyTree{}
	baseLevel := BaseLevel(messages)
	tree.levels = append(tree.levels, baseLevel)
	for len(baseLevel.ToList()) > 1 {
		baseLevel = NextLevel(baseLevel)
		tree.levels = append(tree.levels, baseLevel)
	}
	return tree
}

// Insert adds a message to the ProllyTree.
func (pt *ProllyTree) Insert(message *Message) {
	newNode := NewNode(message.data, message.timestamp, false)
	rightOfNewNode := pt.findNodeGreaterThan(newNode.timestamp)
	pt.insertNodeAtLevel(newNode, 0, rightOfNewNode)
}

// Delete removes a node from the ProllyTree based on the timestamp.
func (pt *ProllyTree) Delete(timestamp int) *Node {
	originalNode := pt.Search(timestamp)
	node := originalNode
	if originalNode == nil {
		return nil
	}
	rightBoundaryNode := originalNode.FindNextBoundaryNode()

	for node != nil {
		leftNode := node.left
		rightNode := node.right
		if rightNode != nil {
			rightNode.left = leftNode
		}
		if leftNode != nil {
			leftNode.right = rightNode
		}
		node = node.up
	}

	// Clean up empty levels
	var levelsToRemove []int
	for i := len(pt.levels) - 1; i > 1; i-- {
		levelBelow := pt.levels[i-1]
		if levelBelow.tail.left == nil { // Only has tail node
			levelsToRemove = append(levelsToRemove, i)
		}
	}

	for _, levelIndex := range levelsToRemove {
		pt.levels = append(pt.levels[:levelIndex], pt.levels[levelIndex+1:]...)
	}

	pt.updatePropagateMerkelHash(rightBoundaryNode.up)

	return originalNode
}

// Search finds a node based on the timestamp.
func (pt *ProllyTree) Search(timestamp int) *Node {
	rightNode := pt.findNodeGreaterThan(timestamp)
	if rightNode.left != nil && rightNode.left.timestamp == timestamp {
		return rightNode.left
	}
	return nil
}

// String provides a string representation of the ProllyTree.
func (pt *ProllyTree) String() string {
	var levels []string
	for _, level := range pt.levels {
		levels = append(levels, level.String())
	}
	return fmt.Sprint(levels)
}

// findNodeGreaterThan finds the node with the smallest timestamp that is greater than the given timestamp.
func (pt *ProllyTree) findNodeGreaterThan(timestamp int) *Node {
	nodeToFind := NewNode("", timestamp, false)
	node := pt.levels[len(pt.levels)-1].tail // Start from the root

	// Move down to the level 0 right boundary node of the given timestamp
	for node.down != nil {
		if node.left != nil && node.left.timestamp >nodeToFind.timestamp {
			node = node.left
		} else {
			node = node.down
		}
	}

	// Move left until the left element is the timestamp or lower than the timestamp
	for node.left != nil && node.left.timestamp >nodeToFind.timestamp {
		node = node.left
	}

	return node
}

// insertNodeAtLevel inserts a node at a given level and creates a new level if necessary.
func (pt *ProllyTree) insertNodeAtLevel(newNode *Node, levelIndex int, rightOfNewNode *Node) {
	if levelIndex >= len(pt.levels) {
		panic("Adding on a level that doesn't exist")
	}

	// Insert the new node to the right of the rightOfNewNode
	ourLeftNode := rightOfNewNode.left
	rightOfNewNode.left = newNode
	newNode.right = rightOfNewNode
	newNode.left = ourLeftNode
	if ourLeftNode != nil {
		ourLeftNode.right = newNode
	}

	if newNode.IsBoundaryNode() {
		higherLevelNode := newNode.CreateHigherLevelNode()
		nextBoundaryNode := newNode.FindNextBoundaryNode()
		if levelIndex == len(pt.levels)-1 {
			pt.addEmptyLevel()
		}
		pt.insertNodeAtLevel(higherLevelNode, levelIndex+1, nextBoundaryNode.up)
	}

	lastLevel := pt.levels[len(pt.levels)-1]

	if len(lastLevel.ToList()) > 1 {
		pt.addEmptyLevel()
	}
}

// updatePropagateMerkelHash updates the Merkel hash of the node and propagates it up.
func (pt *ProllyTree) updatePropagateMerkelHash(node *Node) {
	node.FillMerkelHash()
	if node.up == nil {
		if node.isTail {
			return
		}
		nextBoundaryNode := node.FindNextBoundaryNode()
		pt.updatePropagateMerkelHash(nextBoundaryNode.up)
	} else {
		pt.updatePropagateMerkelHash(node.up)
	}
}

func (pt *ProllyTree) addEmptyLevel() {
	levelIndex := len(pt.levels)
	newLevel := NewLevel(levelIndex)
	pt.levels = append(pt.levels, newLevel)
	upgradedTail := pt.levels[levelIndex-1].tail.CreateHigherLevelNode()
	newLevel.tail = upgradedTail
}

// Message represents the data structure to be added to the tree.
type Message struct {
	data      string
	timestamp int
}

// NewMessage creates and initializes a new Message.
func NewMessage(data string, timestamp int) *Message {
	return &Message{data: data, timestamp: timestamp}
}

// String provides a string representation of the message, similar to Python's __repr__.
func (m *Message) String() string {
	return fmt.Sprintf("Message(%s, %s)", m.data, m.timestamp)
}

// to Test the prolly tree
func main() {
    fmt.Println("Creating 9 messages: ")
	var messages []*Message
	for i := 0; i < 10; i++ {
		messages = append(messages, NewMessage(strconv.Itoa(i), i))
	}

	fmt.Println("Creating a prolly tree with the messages")
	tree := NewProllyTree(messages)

	fmt.Println("Printing the tree")
	fmt.Println(tree)

	fmt.Println("#############################################")
	fmt.Println("Searching a node with timestamp 5")
	foundNode := tree.Search(5)
	if foundNode != nil {
		fmt.Println("Key Found inside the tree: Node(", foundNode.data, ",", foundNode.timestamp, ")")
	} else {
		fmt.Println("Key not found in the tree.")
	}

	fmt.Println("#############################################")
	fmt.Println("Deleting a node with timestamp 6")
	tree.Delete(6)
	fmt.Println("Printing the tree")
	fmt.Println(tree)

	fmt.Println("#############################################")
	fmt.Println("Inserting a node with timestamp 12")
	tree.Insert(NewMessage("12", 12))
	fmt.Println("Printing the tree")
	fmt.Println(tree)
}