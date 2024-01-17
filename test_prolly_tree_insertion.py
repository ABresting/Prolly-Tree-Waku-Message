import unittest
from prolly_tree import ProllyTree, Message

class TestProllyTreeInsertion(unittest.TestCase):

    def test_insertion(self):
        # Step 1: Create an initial ProllyTree
        initial_messages = [Message(i, i) for i in range(5)]
        tree = ProllyTree(initial_messages)

        # Step 2: Insert a new message into the ProllyTree
        new_message = Message(10, 10)  # Feel free to choose your own data and timestamp
        tree.insert(new_message)

        # Step 3: Verify that the tree structure is updated correctly
        
        # 3.1: Check if the new node is present
        inserted_node = tree.search(10)
        self.assertIsNotNone(inserted_node, "The new node was not found in the tree after insertion.")
        
        # 3.2: Verify the node's data and timestamp
        self.assertEqual(inserted_node.data, 10, "The data of the new node does not match the inserted value.")
        self.assertEqual(inserted_node.timestamp, 10, "The timestamp of the new node does not match the inserted value.")


if __name__ == '__main__':
    unittest.main()
