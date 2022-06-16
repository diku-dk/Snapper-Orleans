namespace Utilities
{
    public class DLinkedList<T>
    {
        public Node<T> head;
        public Node<T> tail;
        public int size = 0;

        public DLinkedList()
        {
            head = null;
            tail = null;
        }

        public DLinkedList(Node<T> node)
        {
            head = node;
            tail = node;
        }

        public Node<T> Append(T value)
        {
            var node = new Node<T>(value);
            if (head == null && tail == null)
            {
                head = node;
                tail = node;
            }
            else
            {
                tail.InsertNext(value);
                tail = tail.next;
            }
            size++;
            return tail;
        }

        public bool Remove(Node<T> node)
        {

            if (!Contains(node)) return false;
            size --;
            if (head == node) head = node.next;
            if (tail == node) tail = node.prev;
            if (node.prev != null) node.prev.next = node.next;
            if (node.next != null) node.next.prev = node.prev;
            return true;
        }

        public bool Contains(Node<T> node)
        {
            bool isFound = false;
            if (head == null) return isFound;
            Node<T> next = head;
            while (next != null)
            {
                if (next == node)
                {
                    isFound = true;
                    break;
                }
                next = next.next;
            }
            return isFound;
        }
    }

    public class Node<T>
    {
        public T data;
        public Node<T> next;
        public Node<T> prev;

        public Node(T value)
        {
            data = value;
            next = null;
            prev = null;
        }

        public Node<T> InsertNext(T value)
        {
            var node = new Node<T>(value);
            if (next == null)
            {
                // Easy to handle 
                node.prev = this;
                next = node;
            }
            else
            {
                // Insert in the middle 
                Node<T> temp = this.next;
                node.prev = this;
                node.next = temp;
                next = node;
                temp.prev = node;
                // temp.next does not have to be changed 
            }
            return node;
        }
    }
}
