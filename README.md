# Prolly-Tree-Waku-Message

The provided code implements a Prolly (Probabilistic) Tree, a data structure designed for efficient storage and retrieval of key-valued data across a pair of nodes. It utilizes Merkel hashes to ensure data integrity and individual node's hash as boundary hashes to optimize the structure dynamically. The tree is constructed with multiple levels, with each higher level acting as a summarized version of the level below, allowing for efficient operations.

Each node in the tree individually can be upgraded to the next level. This is done by calculating the sha-256 hash of the node (key-value pair) and comparing it to a threshold. If the hash is below the threshold, the node is promoted to the next level. This process is repeated until the node reaches the top level, at which point it is considered a boundary node. In the framework of ordered data it is desirable for us since rolling merkel hash of bucket of nodes and sha-256 of individual nodes provides us with same type of probabilistic gurantee of a node being boundary node. And it has constant spill overs than rolling merkel hash of bucket of nodes.

### Components

1. Hash Functions:

- `calculate_hash(hashable_str)`: Generates a SHA-256 hash for the given string.
- `is_boundary_hash(hash_value, threshold)`: Determines if the last hex word of the hash is below a certain threshold, indicating a boundary node.
- `bucket_hash(nodes)`: Calculates the Merkel hash of a bucket of nodes by rolling their hashes starting from the left of the bucket (outdated data) to the right of the bucket (latest data).

2. Node Class:
   Represents a single node in the Prolly Tree with properties like data, timestamp, level, and merkel_hash. It acts like a linked list, with each node pointing to the next/previous node in the same level and the corresponding node in the level above/down.

- `create_higher_level_node()`: Generates a new node at the next level up from an existing node since this node has been promoted to the next level based on its hash (for non-leaf nodes it's hash of it's own hash).
- `fill_merkel_hash()`: Calculates the Merkel hash for the node by considering its down leftward nodes until a boundary is reached. It can be understand as a collective hash of all the nodes to the left of the node which was promoted to the next level.
- `is_boundary_node`(): Determines whether a node is a boundary node based on its hash or if it's a tail node.
- `find_next_boundary_node()`: Locates the next boundary node to the right.

3. Level Class:
   Represents a level in the Prolly Tree. It's a doubly linked list of nodes, with each node pointing to the next/previous node in the same level and the corresponding node in the level above/down.

- `BaseLevel(messages)`: Constructs the base level from a list of messages. It is done when we have a list of messages and we want to create a Prolly Tree from it.
- `NextLevel(prev_level)`: Creates a new level from the previous one, linking only the boundary nodes.
- `link_nodes(nodes)`: Connects a list of nodes linearly (left-right).

4. ProllyTree Class:
   The main class representing the Prolly Tree. It's a multi-level data structure that stores timestamped data in a probabilistic manner. It's designed for efficient insertion, deletion, and retrieval operations while ensuring the integrity and consistency of the data through its multi-level hash-based structure.

- `insert(message)`: Inserts a new message into the tree.
- `search(timestamp)`: Retrieves a node by its timestamp. Returns null if the node doesn't exist.
- `delete(timestamp)`: Removes a node based on its timestamp, adjusting/balancing the tree accordingly.

#### Private Methods

- `_insert_node_at_level(new_node, level_index, right_of_new_node)`: Inserts a node at a specified level, possibly creating new levels as needed.
- `_find_node_greater_than(timestamp)`: Finds the node with the smallest timestamp greater than a given timestamp.
- `_update_propagate_merkel_hash(node)`: Updates the Merkel hash for a node and propagates the change upwards.
- `_add_empty_level()`: Adds a new, empty level to the top of the tree.

### How It Works

1. Initialization:

- A Prolly Tree is initialized with a list of messages. Each message is converted into a Node, and the base level is created using these nodes.
- The base level is then used to create the next level, and so on. until there is only one node left at the top level. In our case it is a special node called `tail` which is always a boundary node.
- Operations such insertion, deletion and search are performed in the order of O(log n) where n is the number of nodes in the tree.

2. Insertion:

- To insert a new message, a new Node is created and placed in the correct position based on its timestamp.
- If the new node is a boundary node, a corresponding node is created in the level above, and the process is repeated until the tree is balanced again.
- Merkel hashes are updated only on impacted parts of the tree as necessary to reflect the changes.

3. Deletion:

- To delete a node, it's first located using its timestamp. The node and its upward links are removed.
- The Merkel hashes are recalculated, and empty levels are removed.

4. Search:

- To find a specific node by timestamp, the tree starts at the top level and works its way down to the base, narrowing down the search range until the desired node is found or determined to be absent.

5. Merkel Hash Propagation:

- Whenever a node is inserted or deleted, the Merkel hashes for the affected nodes and their ancestors are updated to maintain data integrity.
- The Merkel hash for a node is calculated by rolling the hashes of all the nodes to the down left of the node until previous boundary node (chunk before) is reached.

### How to run/test

To test the Prolly tree implementation, run `python3 -m unittest discover -p 'test_*.py'` in the root directory. This will run a series of tests to ensure the tree is working as expected.

The Prolly tree is tested based on the following scenarios:

- Insertion of a single node
- Delete a single node
- Diff between two trees/nodes
