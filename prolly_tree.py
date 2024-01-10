import hashlib
import ipdb 

def calculate_hash(hashable_str):
    hasher = hashlib.sha256()
    hasher.update(hashable_str.encode())
    hash_value = hasher.hexdigest()
    return hash_value


def is_boundary_hash(
        hash_value,
        threshold=7):  # modify this function if you want to measure in bits
    # Check if the last hex word of the hash is below the threshold
    hash_int = int(hash_value[-1:], 16)
    return hash_int < threshold


def bucket_hash(nodes):
    """Calculate the merkel hash of a bucket of nodes. The bucket is a list of nodes. The merkel hash is calculated by concatenating the merkel hashes of the nodes in the bucket and then calculating the hash of the concatenated string."""
    return calculate_hash("".join([n.merkel_hash for n in nodes]))


class Node:
    def __init__(self, data, timestamp, is_tail=False):
        # equivalent to key value, timestamp is key, data is value
        self.timestamp = timestamp
        self.data = data
        self.node_hash = calculate_hash(str(data) + str(timestamp))
        self.level = 0
        self.up = None
        self.down = None
        self.left = None
        self.right = None
        self.merkel_hash = self.node_hash
        self.boundary = None
        # tail is a special node which is the right most of any level
        # it has special properties such as it is considered as highest key
        self.is_tail = is_tail

    def create_higher_level_node(self):
        """Create a new node with the same timestamp as the current node and create parent-child relationship between them."""

        n = Node(None, self.timestamp)
        n.is_tail = self.is_tail
        n.level = self.level + 1
        n.down = self
        self.up = n
        n.node_hash = calculate_hash(self.node_hash)
        return n

    def __repr__(self):
        return f"Node({self.data}, {self.timestamp})"

    def fill_merkel_hash(self):
        """Fill the merkel hash of the node by going down walking left till you hit the boundary."""
        # go down and traverse left till you hit the boundary
        node = self.down
        bucket_nodes = [node]

        while node.left is not None:
            if node.left.is_boundary_node():
                break
            node = node.left
            bucket_nodes.append(node)
        
        for node in bucket_nodes:
            if node.merkel_hash is None:
                node.fill_merkel_hash()

        bucket_nodes.reverse()
        self.merkel_hash = bucket_hash(bucket_nodes)
        return self

    def is_boundary_node(self):
        """Check if the node is a boundary node. If the node is a tail node, it is a boundary node. If the node is not a tail node, check if the hash of the node is a boundary hash. Cache the result for future use."""
        if self.boundary is not None:
            return self.boundary
        self.boundary = self.is_tail or is_boundary_hash(self.node_hash)
        return self.boundary

    def find_next_boundary_node(self):
        """Find the next boundary node by going right till you hit the boundary node."""
        node = self
        while node.right is not None:
            if node.right.is_boundary_node():
                return node.right
            node = node.right
        return node

    def __gt__(self, other):
        return self.is_tail or self.timestamp > other.timestamp

    def __lt__(self, other):
        return (not self.is_tail) and self.timestamp < other.timestamp

    def __eq__(self, other):
        return self.timestamp == other.timestamp

    def __ne__(self, other):
        return self.timestamp != other.timestamp

    def __ge__(self, other):
        return self.is_tail or self.timestamp >= other.timestamp

    def __le__(self, other):
        return (not self.is_tail) and self.timestamp <= other.timestamp


class Level:
    def __init__(self, level):
        self.level = level
        self.tail = None

    def to_list(self):

        nodes = []
        node = self.tail
        while node is not None:
            nodes.append(node)
            node = node.left
        nodes.reverse()
        return nodes

    def __repr__(self):
        return " <=> ".join([str(n.timestamp) for n in self.to_list()])

    # Static method to create a base level from a list of messages
    @staticmethod
    def BaseLevel(messages):
        level = Level(0)
        nodes = [Node(m.data, m.timestamp) for m in messages]
        fake_tail = Node("Tail", "Tail", is_tail=True)
        nodes.append(fake_tail)
        linked_nodes = Level.link_nodes(nodes)
        level.tail = linked_nodes[-1]
        return level

    # Static method to create a level from previous level
    @staticmethod
    def NextLevel(prev_level):
        nodes = prev_level.to_list()
        elegible_nodes = [
            n.create_higher_level_node() for n in nodes
            if n.is_boundary_node()
        ]
        linked_nodes = Level.link_nodes(elegible_nodes)
        merkel_hash_added = [n.fill_merkel_hash()
                             for n in linked_nodes]  # account for tail
        new_level = Level(prev_level.level + 1)
        new_level.tail = merkel_hash_added[-1]
        return new_level

    @staticmethod
    def link_nodes(nodes):
        for node, next_node in zip(nodes, nodes[1:]):
            node.right = next_node
            next_node.left = node
        return nodes


class ProllyTree:
    def __init__(self, messages):
        self.levels = []
        level = Level.BaseLevel(messages)
        self.levels.append(level)
        while len(level.to_list()) > 1:
            level = Level.NextLevel(level)
            self.levels.append(level)

    def __iter__(self):
        return iter(self.levels)

    def __getitem__(self, item):
        return self.levels[item]

    def __len__(self):
        return len(self.levels)

    def insert(self, message):
        """Insert a message into the tree. Create a new level if necessary."""
        new_node = Node(message.data, message.timestamp)
        right_of_new_node = self._find_node_greater_than(new_node.timestamp)
        return self._insert_node_at_level(new_node,
                                         right_of_new_node=right_of_new_node)

    def _insert_node_at_level(self, new_node, level_index=0, right_of_new_node=None):
        """Insert a node at a given level. Create a new level if after insertion, the top level has more than one node."""
        assert level_index < len(self), "Adding on a level that doesn't exist"

        level = self[level_index]

        our_left_node = right_of_new_node.left
        right_of_new_node.left = new_node
        new_node.right = right_of_new_node
        new_node.left = our_left_node
        if our_left_node:
            our_left_node.right = new_node

        if new_node.is_boundary_node():
            higher_level_node = new_node.create_higher_level_node()
            next_boundary_node = new_node.find_next_boundary_node()
            if level_index == len(self) - 1:  # we are at top level
                self._add_empty_level()
            self._insert_node_at_level(higher_level_node, level_index + 1,
             next_boundary_node.up)

        last_level = self[-1]

        if len(last_level.to_list()) > 1:
            # add one more level
            tree = self._add_empty_level()

        return new_node

    def delete(self, timestamp):
        original_node = self.search(timestamp)
        node = original_node
        if original_node is None:
            return None
        right_boundary_node = original_node.find_next_boundary_node()

        while node is not None:
            left_node = node.left
            right_node = node.right
            right_node.left = left_node
            if left_node:
                left_node.right = right_node
            node = node.up

        # now clean up empty levels
        levels_to_remove = []
        for i in range(len(self) - 1, 1, -1):
            current_level = self[i]
            level_below = self[i - 1]
            if level_below.tail.left is None:  # only has tail node
                levels_to_remove.append(i)

        for level_index in levels_to_remove:
            self.levels.pop(level_index)

        self._update_propagate_merkel_hash(right_boundary_node.up)

        return original_node

    def search(self, timestamp):
        right_node = self._find_node_greater_than(timestamp)

        if right_node.left and right_node.left.timestamp == timestamp:
            return right_node.left
        else:
            return None

    def __repr__(self):
        return "\n".join([str(l) for l in self.levels])

    def _find_node_greater_than(self, timestamp):
        """Find the node with the smallest timestamp that is greater than the given timestamp."""
        node_to_find = Node(None, timestamp)
        node = self[-1].tail  # root

        # get to the level 0 right boundary node of subjected timestamp
        while node.down is not None:
            if node.left and node.left > node_to_find:
                node = node.left
            else:
                node = node.down

        # start moving left until the left element is timestamp or lower than timestamp
        while node.left and node.left > node_to_find:
            node = node.left

        return node

    def _update_propagate_merkel_hash(self, node):
        """Update the merkel hash of the node and propage it up."""
        node.fill_merkel_hash()
        # Then update your parent if you have one
        if node.up is None:
            # find next boundary node
            if node.is_tail:
                return True
            next_boundary_node = node.find_next_boundary_node()
            return self._update_propagate_merkel_hash(next_boundary_node.up)
        else:
            return self._update_propagate_merkel_hash(node.up)

    def _add_empty_level(self):
        level_index = len(self)
        new_level = Level(level_index)
        self.levels.append(new_level)
        upgraded_tail = self[level_index - 1].tail.create_higher_level_node()
        new_level.tail = upgraded_tail
        return self

# This is a simple wrapper used to test Prolly tree
class Message:
    def __init__(self, data, timestamp):
        self.data = data
        self.timestamp = timestamp

    def __repr__(self):
        return f"Message({self.data}, {self.timestamp})"

# to Test the prolly tree
print("Creating 9 messages: ")
messages = [Message(i, i) for i in range(0, 10)]
print("Creating a prolly tree with the messages")
tree = ProllyTree(messages)
print("Printing the tree")
print(tree)
print("#############################################")
print("Searching a node with timestamp 5")
print("Key Found inside the tree " + str(tree.search(5)))
print("#############################################")
print("Deleting a node with timestamp 6")
tree.delete(6)
print("Printing the tree")
print(tree)
print("#############################################")
print("inserting a node with timestamp 12")
tree.insert(Message(12, 12))
print("Printing the tree")
print(tree)
    
print("#########################################################")

print("Diff protocol in action")
def get_non_boundary_nodes(list_of_nodes):
    """
    get all the nodes that are not boundary nodes starting from the first node itself
    """
    non_boundary_nodes = []
    for node in list_of_nodes:
        if node.down is None:
            continue
        counter_node = node.down
        non_boundary_nodes.append(counter_node)
        counter_node= counter_node.left

        while counter_node is not None:
            if counter_node.boundary:
                break
            non_boundary_nodes.insert(0,counter_node)
            counter_node = counter_node.left
    return non_boundary_nodes

def get_non_boundary_nodes_for_level_0(list_of_nodes):
    """
    get all the nodes that are not boundary nodes starting from the first node itself
    """
    non_boundary_nodes = []
    for node in list_of_nodes:
        while node.down is not None:
            node = node.down
        counter_node = node
        non_boundary_nodes.append(counter_node)
        counter_node= counter_node.left

        while counter_node is not None:
            if counter_node.boundary:
                break
            non_boundary_nodes.insert(0,counter_node)
            counter_node = counter_node.left
    return non_boundary_nodes

def find_diff_between_2_prolly_trees(root1,root2):
    """
    we need to find what all keys are there in root2 that are not in root1
    """
 
    missing_keys =[]

    node_list1 = [root1]
    node_list2 = [root2]

    # function to check diff at each level
    def check_diff_at_each_level(node_list1,node_list2, level):
        if level < 0:
            return
        
        next_level_keys_to_fetch_node1 = []
        next_level_keys_to_fetch_node2 = []
        level_ptr1, level_ptr2 = node_list1[-1], node_list2[-1]
        while level_ptr1 and level_ptr2 :
            # if timestamps match then only check for merkel hash and if they do not match then add them to missing keys
            if level_ptr1.timestamp == level_ptr2.timestamp:
                # mismatch in merkel hash so add it to missing keys
                if level_ptr1.merkel_hash != level_ptr2.merkel_hash:
                    # insert at first position always
                    next_level_keys_to_fetch_node1.insert(0,level_ptr1)
                    next_level_keys_to_fetch_node2.insert(0,level_ptr2)
                # advance the pointers
                level_ptr1 = level_ptr1.left
                level_ptr2 = level_ptr2.left
            # tree 1 has no such key so add it to missing keys
            elif level_ptr1.timestamp < level_ptr2.timestamp:
                # do not let non level 0 nodes to be part of the misisng keys
                if level_ptr2.level ==0:
                    missing_keys.append(level_ptr2)
                next_level_keys_to_fetch_node2.insert(0,level_ptr2)
                # move the pointer of tree2 may be the next key is present in tree1
                level_ptr2 = level_ptr2.left
            # tree1 has extra keys than tree2, skip the tree1 key and see if next one matches
            else:
                level_ptr1 = level_ptr1.left

        # get the corresponding keys where the merkel hash did not match
        node_list1 = get_non_boundary_nodes(next_level_keys_to_fetch_node1)
        node_list2 = get_non_boundary_nodes(next_level_keys_to_fetch_node2)
        # meaning that all nodes match the hashes at this level so no further check required
        if len(node_list1) == 0 and len(node_list2) ==0:
            return missing_keys
        elif len(node_list1) == 0:
            res = get_non_boundary_nodes_for_level_0(node_list2)
            [missing_keys.append(node) for node in res]
            return missing_keys

        check_diff_at_each_level(node_list1, node_list2, level-1)

    check_diff_at_each_level(node_list1, node_list2, root1.level)
    return missing_keys

#######################
# Use case 1 where the tree1 is a subset of tree2
print("#############################################")
print("Use case 1 where the tree1 is a subset of tree2")
messages = [Message((i), i) for i in range(0, 11)]
tree1 = ProllyTree(messages)
print(tree1)

messages = [Message((i), i) for i in range(0, 15)]
tree2 = ProllyTree(messages)
print(tree2)

# Check if the height of the tree is equal
height_of_tree1 = len(tree1)
height_of_tree2 = len(tree2)
print("level of tree1 is ", tree1[-1].tail.level)
print("level of tree2 is ", tree2[-1].tail.level)
root_of_tree1 = tree1[-1].tail
root_of_tree2 = tree2[-1].tail
print("level of tree1 before is ", root_of_tree1.level)
if height_of_tree1 > height_of_tree2:
    height_diff = height_of_tree1 - height_of_tree2
    while height_diff > 0:
        root_of_tree1 = root_of_tree1.down
        height_diff -= 1
    print ("root of tree1 now is ", root_of_tree1.level)
elif height_of_tree2 > height_of_tree1:
    height_diff = height_of_tree2 - height_of_tree1
    while height_diff > 0:
        root_of_tree2 = root_of_tree2.down
        height_diff -= 1
    print ("root of tree2 now is ", root_of_tree2.level)
print("Diff of the Tree1 and Tree2 is ", find_diff_between_2_prolly_trees(root_of_tree1, root_of_tree2))

# Use case 2 where the tree1 is a superset of tree2
print("#############################################")
print("Use case 2 where the tree1 is a superset of tree2")
messages = [Message((i), i) for i in range(0, 11)]
tree1 = ProllyTree(messages)
print(tree1)

messages = [Message((i), i) for i in range(0, 10)]
tree2 = ProllyTree(messages)
print(tree2)

# Check if the height of the tree is equal
height_of_tree1 = len(tree1)
height_of_tree2 = len(tree2)
print("level of tree1 is ", tree1[-1].tail.level)
print("level of tree2 is ", tree2[-1].tail.level)
root_of_tree1 = tree1[-1].tail
root_of_tree2 = tree2[-1].tail
print("level of tree1 before is ", root_of_tree1.level)
if height_of_tree1 > height_of_tree2:
    height_diff = height_of_tree1 - height_of_tree2
    while height_diff > 0:
        root_of_tree1 = root_of_tree1.down
        height_diff -= 1
    print ("root of tree1 now is ", root_of_tree1.level)
elif height_of_tree2 > height_of_tree1:
    height_diff = height_of_tree2 - height_of_tree1
    while height_diff > 0:
        root_of_tree2 = root_of_tree2.down
        height_diff -= 1
    print ("root of tree2 now is ", root_of_tree2.level)
print("Diff of the Tree1 and Tree2 is ", find_diff_between_2_prolly_trees(root_of_tree1, root_of_tree2))

# Usecase 3 partially filled tree tree1 which is subset of tree2
print("#############################################")
print("Usecase 3 partially filled tree tree1 which is subset of tree2")
messages = [Message((i), i) for i in range(0, 8) if i != 5]
tree1 = ProllyTree(messages)
print(tree1)

messages = [Message((i), i) for i in range(0, 18)]
tree2 = ProllyTree(messages)
print(tree2)

# Check if the height of the tree is equal
height_of_tree1 = len(tree1)
height_of_tree2 = len(tree2)
print("level of tree1 is ", tree1[-1].tail.level)
print("level of tree2 is ", tree2[-1].tail.level)
root_of_tree1 = tree1[-1].tail
root_of_tree2 = tree2[-1].tail
print("level of tree1 before is ", root_of_tree1.level)
if height_of_tree1 > height_of_tree2:
    height_diff = height_of_tree1 - height_of_tree2
    while height_diff > 0:
        root_of_tree1 = root_of_tree1.down
        height_diff -= 1
    print ("root of tree1 now is ", root_of_tree1.level)
elif height_of_tree2 > height_of_tree1:
    height_diff = height_of_tree2 - height_of_tree1
    while height_diff > 0:
        root_of_tree2 = root_of_tree2.down
        height_diff -= 1
    print ("root of tree2 now is ", root_of_tree2.level)
print("Diff of the Tree1 and Tree2 is ", find_diff_between_2_prolly_trees(root_of_tree1, root_of_tree2))