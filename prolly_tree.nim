import tables, strutils, math, std/strutils, algorithm
import
  sequtils,
  nimcrypto/sha2,
  nimcrypto/utils

type
  Node* = ref object
    data, nodeHash, merkelHash: string
    timestamp, level: int
    up, down, left, right: Node
    boundary: bool
    boundarySet: bool
    isTail: bool

  Message* = object
    data: string
    timestamp: int

  Level* = ref object
    level: int
    tail: Node

  ProllyTree* = ref object
    levels: seq[Level]

proc greaterThan*(n: Node, other: Node): bool =
  ## Checks if one node is 'greater than' another.
  return n.isTail or n.timestamp > other.timestamp

# Define the methods on the Node type
proc newMessage*(data: string, timestamp: int): Message =
  result.data = data
  result.timestamp = timestamp

proc calculateHash*(hashableStr: string): string =
  var ctx: sha256
  ctx.init()  # Initialize the context
  ctx.update(hashableStr)  # Update the context with the input string
  let digest = ctx.finish()  # Finalize the context and get the digest

  # Convert the digest to a hexadecimal string using the nimcrypto utils
  result = toHex(digest.data).toLowerAscii()

proc bucketHash(nodes: seq[Node]): string =
  ## Calculate the Merkel hash of a bucket of nodes.
  ## The bucket is a sequence of nodes. The Merkel hash is calculated
  ## by concatenating the Merkel hashes of the nodes in the bucket
  ## and then calculating the hash of the concatenated string.
  
  var concatenatedHashes = ""
  for node in nodes:
    concatenatedHashes &= node.merkelHash  # Concatenate the Merkel hashes.

  return calculateHash(concatenatedHashes)

proc isBoundaryHash(hashValue: string, threshold = 7): bool =
  # Check if the last hex digit of the hash is below the threshold
  let lastChar = hashValue[hashValue.len - 1]
  # convert the last hex digit to an integer
  var hashInt:int = fromHex[int]($lastChar)
  result = hashInt < threshold

proc newNode*(data: string, timestamp: int, isTail: bool): Node =
  new(result)
  result.timestamp = timestamp
  result.data = data
  result.isTail = isTail
  result.nodeHash = calculateHash(data & $timestamp)
  result.merkelHash = calculateHash(data & $timestamp)
  result.up = nil
  result.down = nil
  result.left = nil
  result.right = nil
  result.boundary = false  # Initialize to false.
  result.boundarySet = false  # Initialize to false, indicating boundary not explicitly set.

proc createHigherLevelNode*(n: Node): Node =
  let newNode = newNode("", n.timestamp, n.isTail)
  newNode.level = n.level + 1
  newNode.down = n
  n.up = newNode
  newNode.nodeHash = calculateHash($(n.nodeHash))
  return newNode

proc isBoundaryNode*(n: Node): bool =
  if not n.boundarySet:  # Check if the boundary has been explicitly set.
    # Calculate the boundary value based on the node's properties.
    n.boundary = n.isTail or isBoundaryHash(n.nodeHash)
    n.boundarySet = true  # Mark the boundary as explicitly set.
  return n.boundary

proc findNextBoundaryNode*(n: Node): Node =
  var node = n
  while node.right != nil:
    if node.right.isBoundaryNode:
      return node.right
    node = node.right
  return node

proc newLevel*(lvl: int): Level =
  new(result)
  result.level = lvl

proc toList*(l: Level): seq[Node] =
  result = @[]
  var node = l.tail
  
  while node != nil:
    result.add(node)
    node = node.left

  # Reverse the sequence in place
  reverse(result)

proc `$`*(l: Level): string =
  result = " <=> " & $l.toList().mapIt(it.timestamp)

proc linkNodes*(nodes: seq[Node]): seq[Node] =
  for i in 0..<nodes.len-1:
    nodes[i].right = nodes[i+1]
    nodes[i+1].left = nodes[i]
  return nodes

proc baseLevel*(messages: seq[Message]): Level =
  let level = newLevel(0)
  var nodes: seq[Node]
  for m in messages:
    nodes.add(newNode(m.data, m.timestamp, false))
  let fakeTail = newNode("Tail", -1, true)
  nodes.add(fakeTail)
  level.tail = linkNodes(nodes)[^1]
  return level

proc fillMerkelHash*(n: Node) =
  if n.isNil:
    return

  var bucketNodes: seq[Node]  # Make sure the sequence can hold mutable Nodes  
  var node = n.down

  if node != nil:
    bucketNodes.add(node)  # Ensure that 'node' is mutable when added
  
  while node != nil and node.left != nil:
    if node.left.isBoundaryNode():
      break
    node = node.left  # Ensure 'left' returns a mutable Node if it's supposed to be modified
    bucketNodes.add(node)  # Ensure that 'node' is mutable when added

  for node in bucketNodes:
    if node.merkelHash == "":
      fillMerkelHash(node)  # This should be fine if 'node' is indeed a mutable Node

  # Reverse the sequence of nodes.
  reverse(bucketNodes)

  n.merkelHash = bucketHash(bucketNodes)  # Ensure that 'bucketHash' can accept the modified sequence

proc nextLevel*(prevLevel: Level): Level =
  let nodes = prevLevel.toList()
  var eligibleNodes: seq[Node]
  for node in nodes:
    if node.isBoundaryNode:
      eligibleNodes.add(node.createHigherLevelNode())
  let linkedNodes = linkNodes(eligibleNodes)
  for node in linkedNodes:
    node.fillMerkelHash()
  let newLevel = newLevel(prevLevel.level + 1)
  newLevel.tail = linkedNodes[^1]
  return newLevel


proc newProllyTree*(messages: seq[Message]): ProllyTree =
  ## Creates a new ProllyTree instance with the given messages.
  new(result)  # Allocate memory for the ProllyTree.
  result.levels = @[]  # Initialize levels as an empty sequence.
  
  var level = baseLevel(messages)  # Create the base level.
  result.levels.add(level)  # Add the base level to the tree.
  
  while level.toList().len > 1:  # While the level has more than one node.
    level = nextLevel(level)  # Create the next level.
    result.levels.add(level)
    

proc findNodeGreaterThan*(pt: ProllyTree, timestamp: int): Node =
  let nodeToFind = newNode("", timestamp, false)
  var node = pt.levels[^1].tail  # Start from the root

  # Move down to the level 0 right boundary node of the given timestamp
  while not node.down.isNil:
    if not node.left.isNil and node.left.greaterThan(nodeToFind):
      node = node.left
    else:
      node = node.down

  # Move left until the left element is the timestamp or lower than the timestamp
  while not node.left.isNil and node.left.greaterThan(nodeToFind):
    node = node.left
  #  print a deubg statement and print the node timestamp
  return node

proc addEmptyLevel*(pt: ProllyTree) =
  ## Adds an empty level to the ProllyTree.
  let levelIndex = pt.levels.len
  let newLevel = newLevel(levelIndex)  # Create a new level.
  pt.levels.add(newLevel)  # Add the new level to the tree.

  if pt.levels[levelIndex - 1].tail != nil:  # Check if the previous level's tail exists.
    let upgradedTail = createHigherLevelNode(pt.levels[levelIndex - 1].tail)  # Create a higher-level node from the tail.
    newLevel.tail = upgradedTail  # Set the new level's tail to the upgraded tail.

proc insertNodeAtLevel*(pt: ProllyTree, newNode: Node, levelIndex: int, rightOfNewNode: Node) =
  ## Inserts a node at a given level and creates a new level if necessary.
  if levelIndex >= len(pt.levels):
    raise newException(ValueError, "Adding on a level that doesn't exist")

  # Insert the new node to the right of the rightOfNewNode
  let ourLeftNode = rightOfNewNode.left
  rightOfNewNode.left = newNode
  newNode.right = rightOfNewNode
  newNode.left = ourLeftNode
  if ourLeftNode != nil:
    ourLeftNode.right = newNode

  if newNode.isBoundaryNode():  # Assuming 'isBoundaryNode' is a method or property of Node.
    let higherLevelNode = newNode.createHigherLevelNode()  # Assuming 'createHigherLevelNode' is a method of Node.
    let nextBoundaryNode = newNode.findNextBoundaryNode()  # Assuming 'findNextBoundaryNode' is a method of Node.
    if levelIndex == len(pt.levels) - 1:
      pt.addEmptyLevel()  # Assuming 'addEmptyLevel' is a method of ProllyTree.
    pt.insertNodeAtLevel(higherLevelNode, levelIndex + 1, nextBoundaryNode.up)

  let lastLevel = pt.levels[len(pt.levels) - 1]

  if len(lastLevel.toList()) > 1:  # Assuming 'toList' is a method or property of Level.
    pt.addEmptyLevel()

proc updatePropagateMerkelHash*(pt: ProllyTree, node: Node) =
  if node.isNil:
    return
  node.fillMerkelHash()
  
  if node.up.isNil:
    if node.isTail:
      return
    let nextBoundaryNode = node.findNextBoundaryNode()
    pt.updatePropagateMerkelHash(nextBoundaryNode.up)
  else:
    pt.updatePropagateMerkelHash(node.up)

proc insert*(pt: ProllyTree, message: Message) =
  let newNode = newNode(message.data, message.timestamp, false)
  let rightOfNewNode = pt.findNodeGreaterThan(newNode.timestamp)
  pt.insertNodeAtLevel(newNode, 0, rightOfNewNode)

proc search*(pt: ProllyTree, timestamp: int): Node =
  let rightNode = pt.findNodeGreaterThan(timestamp)
  if not rightNode.left.isNil and rightNode.left.timestamp == timestamp:
    return rightNode.left
  return nil

proc delete*(pt: ProllyTree, timestamp: int): Node =
  
  let originalNode = pt.search(timestamp)
  var node = originalNode
  if originalNode.isNil:
    return nil

  let rightBoundaryNode = originalNode.findNextBoundaryNode()
  
  while not node.isNil:
    var leftNode = node.left
    var rightNode = node.right
    if not rightNode.isNil:
      rightNode.left = leftNode
    if not leftNode.isNil:
      leftNode.right = rightNode
    node = node.up

  # Clean up empty levels
  var levelsToRemove: seq[int]
  for i in countdown(len(pt.levels) - 1, 2):
    let levelBelow = pt.levels[i - 1]
    if levelBelow.tail.left.isNil:  # Only has tail node
      levelsToRemove.add(i)
  
  for levelIndex in levelsToRemove:
    pt.levels.delete(levelIndex)

  pt.updatePropagateMerkelHash(rightBoundaryNode.up)
  
  return originalNode

proc `$`*(pt: ProllyTree): string =
  # This procedure will return a string representation of the ProllyTree,
  # showing nodes' timestamps at each level.
  for lvl, level in pt.levels.pairs:
    result.add("Level " & $lvl & ": ")
    var node = level.tail
    # Iterate through nodes at this level from right to left.
    while node != nil:
      result.add($node.timestamp & " ")
      node = node.left  # Move to the next node on the left.
    result.add("\n")  # Add a newline after each level.

proc main() =
  echo "Creating 9 messages: "
  var messages: seq[Message]
  for i in 0..<10:
    messages.add(newMessage($i, i))

  echo "Creating a prolly tree with the messages"
  let tree = newProllyTree(messages)
  echo "Printing the tree"
  echo tree

  echo "#############################################"
  echo "Searching a node with timestamp 5"
  let foundNode = tree.search(5)
  if foundNode != nil:
    echo "Key Found inside the tree: Node(", foundNode.data, ",", foundNode.timestamp, ")"
  else:
    echo "Key not found in the tree."

  echo "#############################################"
  echo "Deleting a node with timestamp 6"
  discard tree.delete(6)
  echo "Printing the tree"
  echo tree

  echo "#############################################"
  echo "Inserting a node with timestamp 12"
  tree.insert(newMessage("12", 12))
  echo "Printing the tree"
  echo tree

when isMainModule:
  main()
