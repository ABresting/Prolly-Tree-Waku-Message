import unittest
from prolly_tree import ProllyTree, Message, find_diff_between_2_prolly_trees

class TestProllyTreeDiffProtocol(unittest.TestCase):

    def test_use_case_1_subset(self):
        # Step 1: Create an initial ProllyTree
        # Create trees for Node 1 and Node 2
        # 1.1 Node 1 initializing its Prolly tree
        messages_node1 = [Message(i, i) for i in range(11)]
        tree1 = ProllyTree(messages_node1)

        # 1.2 remote Node 2 (peer node) initializing its Prolly tree
        messages_node2 = [Message(i, i) for i in range(15)]
        # here tree2 is working as a remote connection to the peer node
        tree2 = ProllyTree(messages_node2)

        # Step 2: Get the roots first for comparison
        # 2.1 getting the root of the local Prolly tree
        root_of_tree1 = tree1.get_root()

        # 2.2 requesting the node 2 to send its root
        root_of_tree2 = tree2.get_root()

        # Step 3: Check if the height of the tree is equal
        # 3.1 get the height of the local Prolly tree using root node
        height_of_tree1 = root_of_tree1.level+1

        # 3.2 get the height of the remote Prolly tree using already received root node
        height_of_tree2 = root_of_tree2.level+1

        # 3.3 get the same height for both the trees
        # if height of tree1 is greater than tree2 then get the root of tree1 at the same height as tree2
        if height_of_tree1 > height_of_tree2:
            height_diff = height_of_tree1 - height_of_tree2
            # get root for a given height_diff
            root_of_tree1 = tree1.get_root_at_height(height_diff)
        elif height_of_tree2 > height_of_tree1:
            height_diff = height_of_tree2 - height_of_tree1
            root_of_tree2 = tree2.get_root_at_height(height_diff)
        
        # Step 4: Find the difference between the two trees
        diff_result = find_diff_between_2_prolly_trees(root_of_tree1, root_of_tree2)

        # Step 5: Assert the expected outcome (in this case, the missing keys should be [11, 12, 13, 14])
        expected_missing_keys = [Message(i, i) for i in range(11, 15)]
        # In this case, Node 1 is a subset of Node 2
        self.assertEqual(sorted([node.timestamp for node in diff_result]), sorted([11, 12, 13, 14]))

    def test_use_case_2_superset(self):
        # Step 1: Create an initial ProllyTree
        # Create trees for Node 1 and Node 2
        # 1.1 Node 1 initializing its Prolly tree
        messages_node1 = [Message(i, i) for i in range(11)]
        tree1 = ProllyTree(messages_node1)

        # 1.2 remote Node 2 (peer node) initializing its Prolly tree
        messages_node2 = [Message(i, i) for i in range(10)]
        # here tree2 is working as a remote connection to the peer node
        tree2 = ProllyTree(messages_node2)

        # Step 2: Get the roots first for comparison
        # 2.1 getting the root of the local Prolly tree
        root_of_tree1 = tree1.get_root()

        # 2.2 requesting the node 2 to send its root
        root_of_tree2 = tree2.get_root()

        # Step 3: Check if the height of the tree is equal
        # 3.1 get the height of the local Prolly tree using root node
        height_of_tree1 = root_of_tree1.level+1

        # 3.2 get the height of the remote Prolly tree using already received root node
        height_of_tree2 = root_of_tree2.level+1

        # 3.3 get the same height for both the trees
        # if height of tree1 is greater than tree2 then get the root of tree1 at the same height as tree2
        if height_of_tree1 > height_of_tree2:
            height_diff = height_of_tree1 - height_of_tree2
            # get root for a given height_diff
            root_of_tree1 = tree1.get_root_at_height(height_diff)
        elif height_of_tree2 > height_of_tree1:
            height_diff = height_of_tree2 - height_of_tree1
            root_of_tree2 = tree2.get_root_at_height(height_diff)
        
        # Step 4: Find the difference between the two trees
        diff_result = find_diff_between_2_prolly_trees(root_of_tree1, root_of_tree2)

        # Step 5: Assert the expected outcome
        # (In this case, no missing keys as tree1 is a superset of tree2)
        expected_missing_keys = []
        self.assertEqual(sorted([node.timestamp for node in diff_result]), sorted([node.timestamp for node in expected_missing_keys]))

    def test_use_case_3_partially_filled_subset(self):
        # Step 1: Create an initial ProllyTree
        # Create trees for Node 1 and Node 2
        # 1.1 Node 1 initializing its Prolly tree
        messages_node1 = [Message(i, i) for i in range(8) if i != 5]
        tree1 = ProllyTree(messages_node1)

        # 1.2 remote Node 2 (peer node) initializing its Prolly tree
        messages_node2 = [Message(i, i) for i in range(18)]
        # here tree2 is working as a remote connection to the peer node
        tree2 = ProllyTree(messages_node2)

        # Step 2: Get the roots first for comparison
        # 2.1 getting the root of the local Prolly tree
        root_of_tree1 = tree1.get_root()

        # 2.2 requesting the node 2 to send its root
        root_of_tree2 = tree2.get_root()

        # Step 3: Check if the height of the tree is equal
        # 3.1 get the height of the local Prolly tree using root node
        height_of_tree1 = root_of_tree1.level+1

        # 3.2 get the height of the remote Prolly tree using already received root node
        height_of_tree2 = root_of_tree2.level+1

        # 3.3 get the same height for both the trees
        # if height of tree1 is greater than tree2 then get the root of tree1 at the same height as tree2
        if height_of_tree1 > height_of_tree2:
            height_diff = height_of_tree1 - height_of_tree2
            # get root for a given height_diff
            root_of_tree1 = tree1.get_root_at_height(height_diff)
        elif height_of_tree2 > height_of_tree1:
            height_diff = height_of_tree2 - height_of_tree1
            root_of_tree2 = tree2.get_root_at_height(height_diff)
        
        # Step 4: Find the difference between the two trees
        diff_result = find_diff_between_2_prolly_trees(root_of_tree1, root_of_tree2)

        # Step 5: Assert the expected outcome
        # (In this case, there will be a missing key in Node 1 and some missing keys that only Node 2 has)
        expected_missing_keys = [Message(5, 5)] + [Message(i, i) for i in range(8, 18)]
        self.assertEqual(sorted([node.timestamp for node in diff_result]), sorted([node.timestamp for node in expected_missing_keys]))

if __name__ == '__main__':
    unittest.main()