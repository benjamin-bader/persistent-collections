package com.bendb.kotlin.persistent

interface PersistentList<T> : Collection<T> {
    val indices: IntRange
        get() = IntRange(0, size - 1)

    fun add(element: T): PersistentList<T>
    operator fun get(index: Int): T
    fun set(index: Int, element: T): PersistentList<T>
}

fun <T> persistentListOf(vararg elements: T): PersistentList<T> {
    return elements.fold(CljPersistentList.empty(), PersistentList<T>::add)
}

fun <T> emptyPersistentList(): PersistentList<T> = CljPersistentList.empty()

fun <T> Iterable<T>.toPersistentList(): PersistentList<T> {
    return fold(CljPersistentList.empty(), PersistentList<T>::add)
}

fun <T> Sequence<T>.toPersistentList(): PersistentList<T> {
    return fold(CljPersistentList.empty(), PersistentList<T>::add)
}

/**
 * A Kotlin port of Clojure's PersistentVector class.
 *
 * The list is structured as a tree with a branch factor of 32; that is, each
 * node has 32 children, as opposed to the usual two (left and right) for a
 * binary tree.  This wide branch factor leads to very shallow trees, and a
 * favorable complexity of O(log32(n)).
 *
 * An interesting feature of the structure is that list contents are contained
 * _in leaf nodes only_; internal nodes only point to other nodes.  This allows
 * us to use the height of the tree, in combination with an item index, to find
 * any item without performing any type-specific comparisons along the way.
 *
 * The tree height is stored in a "shift" value.  Each level is represented as
 * an increment of bits.  Because each node can hold 32 children, our shift
 * increment is five bits - that is, the number of bits that mask an integer to
 * a value between 0 and 32 (exclusive).  So, a 1-high tree would have a shift
 * of 5, meaning "level == 1"; a 2-high tree would have a shift of 10, etc.
 *
 * An item's zero-based index encodes the path through the tree, from root
 * to tail, in groups of [SHIFT_INCREMENT] bits.  The least significant group
 * is the index in the leaf node's array of the item; the leaf node's location
 * in its parent's array is the next-most-significant group of bits, etc.
 *
 * Trees will almost never have more than six or seven levels; 32^8 is over
 * one trillion elements.  Most trees will not approach one trillion elements.
 *
 * As an optimization, the tailing 32 elements are stored directly in the
 * list object itself, and not in a node.  In the common cases of small lists,
 * this saves a good deal of memory.
 *
 * This data structure was designed and implemented by Rich Hickey and the
 * Clojure contributors; the Kotlin port was done by Ben Bader.
 */
internal class CljPersistentList<T> private constructor(
        /**
         * The number of elements in this list.
         */
        override val size: Int,

        /**
         * The height of the tree, as increments of [SHIFT_INCREMENT].
         *
         * An empty list is defined as having a height of 1 * [SHIFT_INCREMENT].
         */
        val shift: Int,

        /**
         * The root of the tree.  If it is a leaf node, its children will be
         * elements of type [T]; if not, its children will be other [nodes][Node].
         */
        val root: Node,

        /**
         * The final elements of the list, up to [NODE_WIDTH].  When the tree
         * contains fewer than [NODE_WIDTH] elements, all of the elements will
         * be stored in this array.
         */
        val tail: Array<Any?>
): PersistentList<T> {
    companion object {
        private const val SHIFT_INCREMENT = 5
        private const val NODE_WIDTH = 32
        private const val NODE_WIDTH_MASK = 0x1F

        private val EMPTY_NODE = Node()

        private val EMPTY = CljPersistentList<Any>(0, SHIFT_INCREMENT, EMPTY_NODE, emptyArray())

        @Suppress("UNCHECKED_CAST")
        fun <T> empty(): PersistentList<T> = EMPTY as PersistentList<T>
    }

    /**
     * A node is the internal structure of a bitmapped persistent list.  It has a
     * branch factor of [NODE_WIDTH], currently 32.  Its child array will point to
     * either child [nodes][Node] or list elements, and never both.  Leaf
     * [nodes][Node] will have elements of type [T]; internal nodes will have other
     * [nodes][Node].
     */
    internal class Node(val children: Array<Any?> = arrayOfNulls(32))

    /**
     * The list index of the first element of [tail].
     *
     * If there are more than 32 elements in the list, only the final
     * 32 will be contained in [tail]; the rest, in [nodes][Node].
     */
    internal val tailOffset: Int
        get() = if (size < NODE_WIDTH) 0 else ((size - 1) ushr SHIFT_INCREMENT) shl SHIFT_INCREMENT

    internal fun arrayFor(index: Int): Array<Any?> {
        if (index in indices) {
            if (index >= tailOffset) {
                return tail
            }

            var node = root
            var level = shift
            while (level > 0) {
                node.children.indices
                node = node.children[(index ushr level) and NODE_WIDTH_MASK] as Node
                level -= SHIFT_INCREMENT
            }
            return node.children
        }
        throw IndexOutOfBoundsException()
    }

    override fun isEmpty() = size == 0

    @Suppress("UNCHECKED_CAST")
    override operator fun get(index: Int): T {
        val array = arrayFor(index)
        return array[index and NODE_WIDTH_MASK] as T
    }

    override fun add(element: T): PersistentList<T> {
        if (size - tailOffset < NODE_WIDTH) {
            val newTail = arrayOfNulls<Any>(tail.size + 1)
            System.arraycopy(tail, 0, newTail, 0, tail.size)
            newTail[tail.size] = element
            return CljPersistentList(size + 1, shift, root, newTail)
        }

        // tail is full; push it into the tree
        val tailNode = Node(tail)
        val node: Node
        val newShift: Int

        // Is root going to overflow?
        if ((size ushr SHIFT_INCREMENT) > (1 shl shift)) {
            node = Node()
            node.children[0] = root
            node.children[1] = newPath(shift, tailNode)
            newShift = shift + SHIFT_INCREMENT
        } else {
            newShift = shift
            node = pushTail(shift, root, tailNode)
        }

        return CljPersistentList(size + 1, newShift, node, arrayOf(element as Any?))
    }

    private fun pushTail(level: Int, parent: Node, tailNode: Node): Node {
        // if parent is leaf, insert node.
        // else does it map to an existing child? -> nodeToInsert = pushNode one more level
        // else alloc new path

        val subIndex = ((size - 1) ushr level) and NODE_WIDTH_MASK

        return Node(parent.children.clone()).apply {
            children[subIndex] = if (level == SHIFT_INCREMENT) {
                tailNode
            } else {
                val child = parent.children[subIndex] as? Node
                if (child != null) {
                    pushTail(level - SHIFT_INCREMENT, child, tailNode)
                } else {
                    newPath(level - SHIFT_INCREMENT, tailNode)
                }
            }
        }
    }

    private fun newPath(level: Int, node: Node): Node {
        return if (level == 0) {
            node
        } else {
            Node().apply {
                children[0] = newPath(level - SHIFT_INCREMENT, node)
            }
        }
    }

    override fun set(index: Int, element: T): PersistentList<T> {
        if (index in indices) {
            if (index >= tailOffset) {
                val newTail = arrayOfNulls<Any>(tail.size)
                System.arraycopy(tail, 0, newTail, 0, tail.size)
                newTail[index and NODE_WIDTH_MASK] = element
                return CljPersistentList(size, shift, root, newTail)
            }

            return CljPersistentList(size, shift, doSet(shift, root, index, element), tail)
        }
        throw IndexOutOfBoundsException()
    }

    private fun doSet(shift: Int, node: Node, index: Int, element: T): Node {
        return Node(node.children.clone()).apply {
            if (shift == 0) {
                children[index and NODE_WIDTH_MASK] = element
            } else {
                val subIndex = (index ushr shift) and NODE_WIDTH_MASK
                children[subIndex] = doSet(
                        shift - SHIFT_INCREMENT,
                        node.children[subIndex] as Node,
                        index,
                        element)
            }
        }
    }

    override fun contains(element: T): Boolean {
        for (e in this) {
            if (element == e) {
                return true
            }
        }
        return false
    }

    override fun containsAll(elements: Collection<T>): Boolean {
        val itemsToCheck = elements.toMutableSet()
        for (e in this) {
            itemsToCheck.remove(e)
        }
        return itemsToCheck.isEmpty()
    }

    override fun iterator(): Iterator<T> {
        return object : Iterator<T> {
            var i = 0
            lateinit var array: Array<Any?>

            override fun hasNext() = i < size

            @Suppress("UNCHECKED_CAST")
            override fun next(): T {
                if (i in indices) {
                    if (i % NODE_WIDTH == 0) {
                        array = arrayFor(i)
                    }
                    return array[i++ and NODE_WIDTH_MASK] as T
                } else {
                    throw NoSuchElementException()
                }
            }
        }
    }
}
