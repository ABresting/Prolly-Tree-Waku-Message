import hashlib
import copy
import ipdb


class Message:
    def __init__(self, data, timestamp):
        self.data = data
        self.timestamp = timestamp

    def __repr__(self):
        return f"Message({self.data}, {self.timestamp})"


def should_split(hash_value, threshold=8):
    # Check if the last hex word of the hash is below the threshold
    hash_int = int(hash_value[-1:], 16)
    return hash_int < threshold


def calculate_hash(hashable_str):
    hasher = hashlib.sha256()
    hasher.update(hashable_str.encode())
    hash_value = hasher.hexdigest()
    return hash_value


def is_boundary_hash(hash_value, n_bits=6, q=4):
    return should_split(hash_value)


class Node:
    def __init__(self, data, timestamp, is_tail=False):
        self.data = data
        self.timestamp = timestamp
        self.node_hash = calculate_hash(str(data) + str(timestamp))
        self.level = 0
        self.up = None
        self.down = None
        self.left = None
        self.right = None
        self.merkel_hash = self.node_hash
        self.boundary = None
        self.is_tail = is_tail

    def create_higher_level_node(self):
        n = copy.copy(self)
        n.boundary = None  #### IMPORTANT
        n.level += 1
        n.down = self
        self.up = n
        n.up = None
        n.left = None
        n.right = None
        n.node_hash = calculate_hash(self.node_hash)
        n.merkel_hash = None
        return n

    def __repr__(self):
        return f"Node({self.data}, {self.timestamp})"

    def is_upgradeable(self):
        if self.boundary is not None:
            return self.boundary
        self.boundary = self.is_tail or is_boundary_hash(self.node_hash)
        return self.boundary

        # return self.is_tail or is_boundary_hash(self.node_hash)

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
        self.head = None
        self.tail = None

    def to_list(self):

        # Traverse right to left

        nodes = []
        node = self.tail
        while node is not None:
            nodes.append(node)
            node = node.left
        nodes.reverse()
        return nodes

    def __repr__(self):
        return " <=> ".join([str(n.timestamp) for n in self.to_list()])


def link_nodes(nodes):
    for node, next_node in zip(nodes, nodes[1:]):
        # import ipdb
        # ipdb.set_trace()
        node.right = next_node
        next_node.left = node
    return nodes


def merkel_hash(nodes):
    return calculate_hash("".join([n.merkel_hash for n in nodes]))


# def add_merkel_hash_for_bucket(node):
#     original_node = node
#     # go down and traverse left till you hit the boundary
#     node = node.down
#     bucket_nodes = [node]
#     while node.left is not None:
#         if node.left.is_upgradeable():
#             break
#         node = node.left
#         bucket_nodes.append(node)
#     original_node.merkel_hash = merkel_hash(bucket_nodes)
#     return original_node


def add_merkel_hash_for_bucket(node):
    original_node = node
    # go down and traverse left till you hit the boundary
    node = node.down
    bucket_nodes = [node]
    while node.left is not None:
        if node.left.is_upgradeable():
            break
        node = node.left
        bucket_nodes.append(node)

    for node in bucket_nodes:
        if node.merkel_hash is None:
            add_merkel_hash_for_bucket(node)

    original_node.merkel_hash = merkel_hash(bucket_nodes)
    return original_node


def create_next_level(level):
    nodes = level.to_list()
    elegible_nodes = [
        n.create_higher_level_node() for n in nodes if n.is_upgradeable()
    ]
    linked_nodes = link_nodes(elegible_nodes)
    merkel_hash_added = [add_merkel_hash_for_bucket(n)
                         for n in linked_nodes]  # account for tail
    new_level = Level(level.level + 1)
    new_level.head = merkel_hash_added[0]
    new_level.tail = merkel_hash_added[-1]
    return new_level


def create_base_level(messages):
    level = Level(0)
    nodes = [Node(m.data, m.timestamp) for m in messages]
    fake_tail = Node("fake", None, is_tail=True)
    nodes.append(fake_tail)
    linked_nodes = link_nodes(nodes)
    level.head = linked_nodes[0]
    level.tail = linked_nodes[-1]
    return level


def create_tree(messages):
    level = create_base_level(messages)
    levels = [level]
    while len(level.to_list()) > 1:
        level = create_next_level(level)
        levels.append(level)
    return levels


# Insert node in level which is a sorted doubly linked list


def find_next_boundary_node(node):
    while node.right is not None:
        if node.right.is_upgradeable():
            return node.right
        node = node.right
    return node


def find_previous_boundary_node(node):
    while node.left is not None:
        if node.left.is_upgradeable():
            return node.left
        node = node.left
    return None


# def propage_merkel_hash(node):
#     while node.up is not None:
#         node.up.merkel_hash = add_merkel_hash_for_bucket(node.up)
#         node = node.up

# def insert_node(level, new_node):
#     current = level.head
#     while current.right and current.right < new_node:
#         current = current.right

#     # Insert the new node between 'current' and 'current.right'
#     new_node.left = current
#     new_node.right = current.right

#     # Update links for surrounding nodes
#     if current.right:
#         current.right.left = new_node
#     current.right = new_node

#     # Update merkel hash for the bucket

#     # check if new node is boundary node

#     if new_node.is_upgradeable():
#         higher_level_node = new_node.create_higher_level_node()
#         higher_level_node.merkel_hash = add_merkel_hash_for_bucket(
#             higher_level_node)
#         new_node.up = higher_level_node
#         insert_node(level.up, higher_level_node)
#     else:
#         next_boundary_node = find_next_boundary_node(new_node)
#         propage_merkel_hash(next_boundary_node)
#         # propage merkel hash up
#     return new_node


def add_empty_level(tree):
    # import ipdb
    # ipdb.set_trace()
    level_index = len(tree)
    new_level = Level(level_index)
    tree.append(new_level)
    upgraded_tail = tree[level_index - 1].tail.create_higher_level_node()
    # import ipdb
    # ipdb.set_trace()
    # add_merkel_hash_for_bucket(upgraded_tail)
    new_level.head = upgraded_tail
    new_level.tail = upgraded_tail
    return tree

def find_right_node_for_insertion(tree, new_node):
    node = tree[-1].tail
    # import ipdb
    # ipdb.set_trace()
    while node.down is not None:
        if node.left and node.left > new_node:
            node = node.left
        else:
            node = node.down

    while node.left and node.left > new_node:
        node = node.left

    return node


def insert_node_in(tree, new_node, level_index=0, right_of_new_node=None):

    # if three is not high enough, create a new level

    if len(tree) == level_index:
        tree = add_empty_level(tree)

    level = tree[level_index]

    our_left_node = right_of_new_node.left
    right_of_new_node.left = new_node
    new_node.right = right_of_new_node
    new_node.left = our_left_node
    if our_left_node:
        our_left_node.right = new_node

    if new_node.is_upgradeable():
        higher_level_node = new_node.create_higher_level_node()
        next_boundary_node = find_next_boundary_node(new_node)
        insert_node_in(tree, higher_level_node, level_index + 1,
                       next_boundary_node.up)

    last_level = tree[-1]

    if len(last_level.to_list()) > 1:
        # add one more level
        tree = add_empty_level(tree)

    return new_node


def remove_node(tree, node):
    original_node = node
    right_boundary_node = find_next_boundary_node(original_node)

    while node is not None:
        left_node = node.left
        right_node = node.right
        right_node.left = left_node
        if left_node:
            left_node.right = right_node
        node = node.up

    update_propage_merkel_hash(right_boundary_node.up)

    return original_node


def update_propage_merkel_hash(node):
    # First update yourself
    # import ipdb
    # ipdb.set_trace()

    add_merkel_hash_for_bucket(node)
    # Then update your parent if you have one
    if node.up is None:
        # find next boundary node
        if node.is_tail:
            return True
        next_boundary_node = find_next_boundary_node(node)
        return update_propage_merkel_hash(next_boundary_node.up)
    else:
        return update_propage_merkel_hash(node.up)


def insert_and_upgrade_merkel(tree, message):
    new_node = Node(message.data, message.timestamp)
    new_node = insert_node(tree, new_node)
    if new_node.is_upgradeable():
        update_propage_merkel_hash(new_node.up)
    next_boundary_node = find_next_boundary_node(new_node)
    update_propage_merkel_hash(next_boundary_node.up)



def find_bigger_node(tree, timestamp):
    node_to_find = Node(None, timestamp)
    node = tree[-1].tail

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


def get_key(tree, timestamp):
    right_node = find_bigger_node(tree, timestamp)

    if right_node.left and right_node.left.timestamp == timestamp:
        return right_node.left
    else:
        return None

# messages = [Message(i, i) for i in range(0, 10)]
# tree = create_tree(messages)
# # print((tree))
# for i in range(1,10):
#     if get_key(tree,i) is None:
#         print(i)
    
