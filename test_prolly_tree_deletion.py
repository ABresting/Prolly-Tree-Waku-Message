import unittest
from prolly_tree import ProllyTree, Message

class TestProllyTreeDeletion(unittest.TestCase):

    def test_deletion(self):
        # Step 1: Create an initial ProllyTree
        initial_messages = [Message(i, i) for i in range(5)]
        tree = ProllyTree(initial_messages)

        # Ensure the node to be deleted exists before deletion
        node_to_delete = tree.search(3)
        self.assertIsNotNone(node_to_delete, "The node to be deleted does not exist in the tree.")

        # Step 2: Delete a message from the ProllyTree
        tree.delete(3)

        # Step 3: Verify that the tree structure is updated correctly
        
        # 3.1: Check if the node is removed
        deleted_node = tree.search(3)
        self.assertIsNone(deleted_node, "The node was not removed from the tree after deletion.")

if __name__ == '__main__':
    unittest.main()
