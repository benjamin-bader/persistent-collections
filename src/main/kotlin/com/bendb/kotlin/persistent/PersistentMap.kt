package com.bendb.kotlin.persistent

import java.util.concurrent.atomic.AtomicBoolean

interface PersistentMap<K, V> : Map<K, V> {
    override fun isEmpty(): Boolean {
        return size == 0
    }

    fun put(key: K, value: V): PersistentMap<K, V>

    fun remove(key: K): PersistentMap<K, V>
}

fun <K, V> persistentMapOf(vararg pairs: Pair<K, V>): PersistentMap<K, V> {
    val seed: PersistentMap<K, V> = if (pairs.size > 8) {
        HashPersistentMap.empty()
    } else {
        ArrayPersistentMap.empty()
    }

    return pairs.fold(seed) { m, (k, v) ->
        m.put(k, v)
    }
}

fun <K, V> emptyPersistentMap(): PersistentMap<K, V> = ArrayPersistentMap.empty()

internal data class MapEntry<out K, out V>(
        override val key: K,
        override val value: V): Map.Entry<K, V>

@Suppress("UNCHECKED_CAST")
internal class ArrayPersistentMap<K, V> private constructor(
        private val array: Array<Any?> = emptyArray()
) : PersistentMap<K, V> {

    companion object {
        private val EMPTY: ArrayPersistentMap<Any, Any> = ArrayPersistentMap(emptyArray())

        internal fun <K, V> empty(): ArrayPersistentMap<K, V> = EMPTY as ArrayPersistentMap<K, V>
    }

    override val size = array.size ushr 1

    override val keys: Set<K>
        get() {
            val set = HashSet<K>(size)
            for (i in array.indices step 2) {
                val k = array[i] as K
                set.add(k)
            }
            return set
        }

    override val values: Collection<V>
        get() {
            val list = ArrayList<V>(size)
            for (i in array.indices step 2) {
                val v = array[i+1] as V
                list.add(v)
            }
            return list
        }

    override val entries: Set<Map.Entry<K, V>>
        get() {
            val set = HashSet<Map.Entry<K, V>>(size)
            for (i in array.indices step 2) {
                val k = array[i] as K
                val v = array[i+1] as V
                set.add(MapEntry(k, v))
            }
            return set
        }

    private fun indexOf(key: K): Int {
        return array.indices
                .step(2)
                .firstOrNull { array[it] == key } ?: -1
    }

    override fun containsKey(key: K) = indexOf(key) != -1

    override fun containsValue(value: V): Boolean {
        return (1..(array.size - 1) step 2).any { array[it] == value }
    }

    @Suppress("UNCHECKED_CAST")
    override fun put(key: K, value: V): PersistentMap<K, V> {
        val index = indexOf(key)

        val newArray: Array<Any?>
        if (index >= 0) {
            if (array[index + 1] == value) {
                return this
            }
            newArray = array.clone().also { it[index + 1] = value }
        } else {
            if (size > 8) {
                var hashmap: PersistentMap<Any?, Any?> = HashPersistentMap.empty()
                for (i in array.indices step 2) {
                    hashmap = hashmap.put(array[i], array[i + 1])
                }
                return hashmap as PersistentMap<K, V>
            }

            newArray = arrayOfNulls<Any>(array.size + 2).also {
                System.arraycopy(array, 0, it, 0, array.size)
                it[array.size + 0] = key
                it[array.size + 1] = value
            }
        }

        return ArrayPersistentMap(newArray)
    }

    @Suppress("UNCHECKED_CAST")
    override fun get(key: K): V? {
        val index = indexOf(key)
        return if (index >= 0) array[index + 1] as V else null
    }

    override fun remove(key: K): PersistentMap<K, V> {
        val index = indexOf(key)
        return if (index >= 0) {
            val newArray = arrayOfNulls<Any>(array.size - 2)
            System.arraycopy(array, 0, newArray, 0, index)
            System.arraycopy(array, index + 2, newArray, index, array.size - index - 2)
            return ArrayPersistentMap(newArray)
        } else {
            this
        }
    }
}

/**
 * A port of Clojure's Hash Array-Mapped Trie adaptation, implementing a persistent
 * hash-map.
 *
 * The structure is represented as a tree of [MapNode] objects.  There are
 * three [MapNode] implementations; here, polymorphic dispatch replaces the
 * more explicit conditionals required in Bagwell's original HAMT design.
 * The three implementations are [BitmapNode], [ArrayNode], and [CollisionNode].
 *
 * The basic idea of this data structure is the combination of hashcodes of keys
 * with a trie.  When traversing the trie to locate an element, its hashcode
 * is essentially split into chunks of five bits per "level" of the tree.  Each
 * tree node has up to 32 children, and each child is uniquely addressable by
 * a five-bit value.
 *
 * [BitmapNode] is the workhorse of the [HashPersistentMap], being the most common
 * node type and primarily where elements are stored.  It is implemented as a
 * 32-bit bitmap, identifying the number and indexes of elements in an accompanying
 * array.
 */
@Suppress("UNCHECKED_CAST")
class HashPersistentMap<K, V> private constructor(
        override val size: Int,
        private val root: MapNode<K, V>?
): PersistentMap<K, V> {

    companion object {
        private val EMPTY = HashPersistentMap<Nothing, Nothing>(0, null)

        fun <K, V> empty(): HashPersistentMap<K, V> = EMPTY as HashPersistentMap<K, V>

        /**
         * Combines two key-value pairs into a [MapNode], taking hash collisions
         * into account.
         */
        private fun <K, V> createNode(shift: Int, keyOne: K, valueOne: V, keyTwoHash: Int, keyTwo: K, valueTwo: V): MapNode<K, V> {
            val keyOneHash = hashOf(keyOne)
            if (keyOneHash == keyTwoHash) {
                return CollisionNode(keyOneHash, 2, arrayOf(keyOne, valueOne, keyTwo, valueTwo))
            }
            val didAddLeaf = AtomicBoolean()
            return BitmapNode.empty<K, V>()
                    .put(shift, keyOneHash, keyOne, valueOne, didAddLeaf)
                    .put(shift, keyTwoHash, keyTwo, valueTwo, didAddLeaf)
        }

        internal inline fun bitCount(n: Int): Int = java.lang.Integer.bitCount(n)

        /**
         * Returns a bitmask with a single bit set for the position
         * in a 32-element array identified by the given [hash] and
         * [shift] values.
         *
         * For example, if the mask(hash, shift) is 16, then the bitpos
         * value will be 0b00000000_00000000_10000000_00000000, or 65536.
         */
        private inline fun bitpos(hash: Int, shift: Int): Int {
            return 1 shl mask(hash, shift)
        }

        /**
         * Obtains the bits of the given [hash] pertaining to the
         * given [shift] level.
         */
        private inline fun mask(hash: Int, shift: Int): Int {
            return (hash ushr shift) and 0x1F
        }

        private fun <T> hashOf(obj: T): Int = 31 + (obj?.hashCode() ?: 0)

        /**
         * Increase the shift by one level, indicating a descent into the next level
         * of the tree.
         */
        private inline fun shiftDown(shift: Int): Int = shift + 5
    }

    override fun containsKey(key: K): Boolean {
        return root?.find(0, hashOf(key), key) != null
    }

    override fun containsValue(value: V): Boolean {
        val iter = valIterator() ?: return false
        return iter.asSequence().any { it == value}
    }

    override val keys: Set<K>
        get() = keyIterator()?.asSequence()?.toSet() ?: emptySet()

    override val values: Collection<V>
        get() = valIterator()?.asSequence()?.toList() ?: emptySet()

    override val entries: Set<Map.Entry<K, V>>
        get() = entryIterator()?.asSequence()?.toSet() ?: emptySet()

    override fun put(key: K, value: V): PersistentMap<K, V> {
        val didAddLeafNode = AtomicBoolean()
        val currentRoot = root ?: BitmapNode.empty()
        val newRoot = currentRoot.put(
                shift = 0,
                hash = hashOf(key),
                key = key,
                value = value,
                didAddLeafNode = didAddLeafNode)

        return if (root === newRoot) {
            this
        } else {
            HashPersistentMap(
                    size = if (didAddLeafNode.get()) size + 1 else size,
                    root = newRoot)
        }
    }

    override fun get(key: K): V? {
        return root?.find(shift = 0, hash = hashOf(key), key = key)
    }

    override fun remove(key: K): PersistentMap<K, V> {
        if (root == null) {
            return this
        }

        val newRoot = root.remove(0, hashOf(key), key)
        if (newRoot === root) {
            return this
        }

        return HashPersistentMap(size - 1, newRoot)
    }

    private fun keyIterator(): Iterator<K>? {
        return root?.iterator { k, _ -> k as K }
    }

    private fun valIterator(): Iterator<V>? {
        return root?.iterator { _, v -> v as V }
    }

    private fun entryIterator(): Iterator<Map.Entry<K, V>>? {
        return root?.iterator { k, v -> MapEntry(k as K, v as V) }
    }

    internal interface MapNode<K, V> {
        /**
         * @param shift the number of bits to shift the given [hash], to obtain
         *              the key's index within this node.
         * @param hash the hash code of the given [key].
         * @param key the key to add
         * @param value the value to add
         * @param didAddLeafNode An output parameter; when this method returns,
         *                       its value will be `true` if a new node was
         *                       added, otherwise `false`.
         */
        fun put(shift: Int, hash: Int, key: K, value: V, didAddLeafNode: AtomicBoolean): MapNode<K, V>
        fun find(shift: Int, hash: Int, key: K): V?
        fun remove(shift: Int, hash: Int, key: K): MapNode<K, V>?
        fun <T> iterator(fn: (Any?, Any?) -> T): Iterator<T>?
    }

    /**
     * A node that stores both values and sub-nodes in a single array of up to
     * 32 elements, partitioned by a five-bit hash value.
     *
     * The array itself contains only enough space for its contents; if this
     * node contains only two key-value pairs, the array will have space for
     * four elements.  This makes locating the array index of a key slightly
     * complicated; if the array were always 32 elements wide, it would be quite
     * simple - `(hash ushr mask) and 0x1F`.  Because we don't want to spend so
     * much space, we instead use a 32-bit bitmap.
     *
     * Each bit corresponds to an array element - that is, bit 0 corresponds to
     * array element 0, 1 to 1, and so on.  If a given bit is set, that means
     * the node has a value for that slot.  The trick that enables us to
     * compress the array is the observation that we can use the number of set
     * bits to the "right" of a given bit to determine where an arbitrary slot
     * would be located, no matter how the array is currently sized.
     *
     * let's imagine that a node contains three elements, in slots 2, 6, and 11;
     * the bitmap would then look like so (in big-endian notation, with leading
     * zero-bytes omitted):
     *
     * `0b00000010_01000100`
     *
     * The corresponding array would have only six elements:
     * `arrayOf(k2, v2, k6, v6, k11, v11)`.
     *
     * Suppose, then, that we want to add an element in slot 5.  Intuitively, we
     * know that it will go between slots 2 and 6; we can arrive algorithmically
     * at the same conclusion efficiently by counting the number of bits to the
     * "right" of slot 5 that are set to 1.  Most processors have an instruction
     * called POPCNT, for "population count (of 1-bits in a word)", and this is
     * available to us via [java.lang.Integer.bitCount].
     *
     * To get this value, we can rely on a neat feature of binary arithmetic.
     * Our 'slots' are represented at runtime as an integer with one bit set,
     * e.g. `0b00100000` for slot 5.  If we subtract one from this value, we'll
     * have `0b00011111`, which is exactly the value we need to mask only the
     * bits we need to count in the bitmap.
     *
     * So, finally, the expression `2 * bitCount((slotBit - 1) and bitmap)` tells
     * us exactly where an element should be inserted.  Recall that the array
     * stores both keys and values, so we need to multiply by 2 to arrive at
     * the correct index.
     *
     * Once we have the index, the rest of insertion is mechanical.
     */
    internal class BitmapNode<K, V> internal constructor(
            private val bitmap: Int,
            private val array: Array<Any?>
    ): MapNode<K, V> {

        companion object {
            private val EMPTY = BitmapNode<Unit, Unit>(0, emptyArray())

            private const val ARRAY_NODE_THRESHOLD = 16

            fun <K, V> empty(): BitmapNode<K, V> = EMPTY as BitmapNode<K, V>
        }

        private fun indexOf(bit: Int): Int {
            return bitCount(bitmap and (bit - 1))
        }

        private inline fun isSet(bit: Int): Boolean {
            return (bitmap and bit) != 0
        }

        override fun put(shift: Int, hash: Int, key: K, value: V, didAddLeafNode: AtomicBoolean): MapNode<K, V> {
            val bit = bitpos(hash, shift)
            val index = indexOf(bit)
            if (isSet(bit)) {
                val maybeKey = array[index * 2 + 0]
                val maybeVal = array[index * 2 + 1]
                if (maybeKey == null) {
                    assert(maybeVal is MapNode<*,*>)
                    val node = maybeVal as MapNode<K, V>
                    val newNode = node.put(shiftDown(shift), hash, key, value, didAddLeafNode)
                    return if (node === newNode) {
                        this
                    } else {
                        BitmapNode(bitmap, array.clone().apply { this[index * 2 + 1] = newNode })
                    }
                }

                if (maybeKey == key) {
                    return if (maybeVal == value) {
                        this
                    } else {
                        BitmapNode(bitmap, array.clone().apply { this[index * 2 + 1] = value })
                    }
                }

                didAddLeafNode.set(true)
                return BitmapNode(
                        bitmap,
                        array.clone().apply {
                            this[index * 2 + 0] = null
                            this[index * 2 + 1] = createNode(shiftDown(shift), maybeKey, maybeVal, hash, key, value)
                        }
                )
            } else {
                val numElements = bitCount(bitmap)

                if (numElements >= ARRAY_NODE_THRESHOLD) {
                    // We've crossed our fill factor threshold; make a new ArrayNode
                    val nodes = arrayOfNulls<MapNode<K, V>>(32)
                    val elementIndex = mask(hash, shift)
                    nodes[elementIndex] = empty<K, V>().put(shiftDown(shift), hash, key, value, didAddLeafNode)

                    var j = 0
                    for (i in nodes.indices) {
                        if (((bitmap ushr i) and 1) != 0) {
                            nodes[i] = if (array[j] == null) {
                                // null key == value is already a node
                                array[j + 1] as MapNode<K, V>
                            } else {
                                empty<K, V>().put(shiftDown(shift), hashOf(array[j]), array[j] as K, array[j + 1] as V, didAddLeafNode)
                            }
                            j += 2
                        }
                    }
                    return ArrayNode(numElements + 1, nodes)
                } else {
                    val newArray = arrayOfNulls<Any>((numElements + 1) * 2)
                    System.arraycopy(array, 0, newArray, 0, index * 2)
                    System.arraycopy(array, index * 2, newArray, (index + 1) * 2, (numElements - index) * 2)
                    newArray[index * 2 + 0] = key
                    newArray[index * 2 + 1] = value

                    didAddLeafNode.set(true)

                    return BitmapNode(bitmap or bit, newArray)
                }
            }
        }

        override fun find(shift: Int, hash: Int, key: K): V? {
            val bit = bitpos(hash, shift)
            if (isSet(bit)) {
                val index = indexOf(bit)
                val maybeKey   = array[index * 2 + 0]
                val maybeValue = array[index * 2 + 1]
                if (maybeKey === null) {
                    assert(maybeValue is MapNode<*,*>)
                    return (maybeValue as MapNode<K, V>).find(shiftDown(shift), hash, key)
                }
                if (maybeKey == key) {
                    assert(maybeValue !is MapNode<*,*>)
                    return maybeValue as V
                }
            }
            return null
        }

        override fun remove(shift: Int, hash: Int, key: K): MapNode<K, V>? {
            val bit = bitpos(hash, shift)
            if (!isSet(bit)) {
                return this
            }

            val index = indexOf(bit)
            val maybeKey = array[index * 2 + 0]
            val maybeVal = array[index * 2 + 1]
            if (maybeKey == null) {
                // maybeVal is an interior node, and we will be delegating
                // to its remove function.  There will be four possible
                // outcomes.

                val newNode = (maybeVal as MapNode<K, V>).remove(shiftDown(shift), hash, key)
                if (newNode === maybeVal) {
                    // The key wasn't present in the node, and nothing changed.
                    return this
                }

                if (newNode != null) {
                    // The key _was_ present, was removed, and the node has more
                    // key/value pairs.
                    return BitmapNode(bitmap, array.clone().apply { this[index * 2 + 1] = newNode })
                }

                // The key was present, removed, and the node had nothing else
                // in it.

                if (bitmap == bit) {
                    // Moreover, that node was the only thing in _this_ node!
                    // bye bye.
                    return null
                }

                // We've still got other things; shrink the array and unset the bit!
                val newArray = arrayOfNulls<Any>(array.size - 2)
                System.arraycopy(array, 0, newArray, 0, index * 2)
                System.arraycopy(array, (index + 1) * 2, newArray, index * 2, newArray.size - (index * 2))

                return BitmapNode(
                        bitmap xor bit,
                        newArray)
            }

            // maybeVal is an actual value; remove it, if the key matches.
            if (key == maybeKey) {
                if (bitmap == bit) {
                    // This is the final value contained herein
                    return null
                }

                val newArray = arrayOfNulls<Any>(array.size - 2)
                System.arraycopy(array, 0, newArray, 0, index * 2)
                System.arraycopy(array, (index + 1) * 2, newArray, index * 2, newArray.size - (index * 2))

                return BitmapNode(
                        bitmap xor bit,
                        newArray
                )
            }

            return this
        }

        override fun <T> iterator(fn: (Any?, Any?) -> T): Iterator<T>? {
            return NodeIterator(array, fn)
        }
    }

    /**
     * An interior trie node; it contains links to sub-nodes, and does not
     * directly contain values.
     */
    internal class ArrayNode<K, V> internal constructor(
            val count: Int,
            val nodes: Array<MapNode<K, V>?>
    ): MapNode<K, V> {
        override fun put(shift: Int, hash: Int, key: K, value: V, didAddLeafNode: AtomicBoolean): MapNode<K, V> {
            val index = mask(hash, shift)
            val node = nodes[index] ?: return ArrayNode(count + 1, nodes.clone().apply {
                this[index] = BitmapNode.empty<K, V>().put(shift + 5, hash, key, value, didAddLeafNode)
            })

            return node.put(shift + 5, hash, key, value, didAddLeafNode).let {
                if (it === node) {
                    this
                } else {
                    ArrayNode(count, nodes.clone().apply { this[index] = it })
                }
            }
        }

        override fun find(shift: Int, hash: Int, key: K): V? {
            val index = mask(hash, shift)
            return nodes[index]?.find(shift + 5, hash, key)
        }

        override fun remove(shift: Int, hash: Int, key: K): MapNode<K, V>? {
            val index = mask(hash, shift)
            val node = nodes[index] ?: return this
            val newNode = node.remove(shift + 5, hash, key)
            return if (newNode === this) {
                this
            } else if (newNode === null) {
                if (count < 8) {
                    val newArray = arrayOfNulls<Any>(nodes.size - 2)
                    var j = 1
                    var bitmap = 0
                    for (i in nodes.indices) {
                        if (i == index) { continue }
                        nodes[i]?.let {
                            newArray[j] = it
                            bitmap = bitmap or (1 shl i)
                            j += 2
                        }
                    }
                    return BitmapNode(bitmap, newArray)
                }
                ArrayNode(count - 1, nodes.clone().apply { this[index] = newNode })
            } else {
                ArrayNode(count, nodes.clone().apply { this[index] = newNode })
            }
        }

        override fun <T> iterator(fn: (Any?, Any?) -> T): Iterator<T>? {
            return object : Iterator<T> {
                private var index = 0
                private var nextIterator: Iterator<T>? = null

                override fun hasNext(): Boolean {
                    while (true) {
                        val iter = nextIterator
                        if (iter != null && iter.hasNext()) {
                            return true
                        }

                        if (index in nodes.indices) {
                            val node = nodes[index++]
                            nextIterator = node?.iterator(fn)
                        } else {
                            return false
                        }
                    }
                }

                override fun next(): T {
                    return if (hasNext()) {
                        nextIterator!!.next()
                    } else {
                        throw NoSuchElementException()
                    }
                }
            }
        }
    }

    /**
     * Implements a simple chain of key-value pairs where keys share a common
     * hashcode value.
     *
     * The chain is an array of alternating key-value entries, all keys having
     * the same hash.
     *
     * A [CollisionNode] is always a leaf node.
     */
    internal class CollisionNode<K, V> internal constructor(
            private val hash: Int,
            private val count: Int,
            private val array: Array<Any?>
    ): MapNode<K, V> {

        private fun <K> indexOf(key: K): Int {
            return (array.indices step 2).firstOrNull { key == array[it] } ?: -1
        }

        override fun put(shift: Int, hash: Int, key: K, value: V, didAddLeafNode: AtomicBoolean): MapNode<K, V> {
            // Two cases:
            // 1. The given hash matches this node's hash exactly.  In this
            //    case, the value belongs here at this level, and we will
            //    be returning a suitably-augmented CollisionNode.
            //
            // 2. The given hash does _not_ match; this collision is not final
            //    at this shift level.  The tree will need to diverge here,
            //    so we will return a BitmapNode that contains both this
            //    node and the given key-value pair.

            if (this.hash == hash) {
                val index = indexOf(key)

                if (index != -1) {
                    if (array[index + 1] == value) {
                        return this
                    }

                    return CollisionNode(hash, count, array.clone().apply { this[index + 1] = value })
                }

                // This key is new to us; put the kvp at the end of a new array.
                val newArray = arrayOfNulls<Any>((count + 1) * 2)
                System.arraycopy(array, 0, newArray, 0, array.size)
                newArray[array.size + 0] = key
                newArray[array.size + 1] = value

                didAddLeafNode.set(true)

                return CollisionNode(hash, count + 1, newArray)
            }

            return BitmapNode<K, V>(bitpos(hash, shift), arrayOf(null, this))
                    .put(shift, hash, key, value, didAddLeafNode)
        }

        override fun find(shift: Int, hash: Int, key: K): V? {
            val index = indexOf(key)
            if (index != -1) {
                return array[index + 1] as V
            }
            return null
        }

        override fun remove(shift: Int, hash: Int, key: K): MapNode<K, V>? {
            val index = indexOf(key)
            if (index == -1) {
                return this
            }

            if (count == 1) {
                return null
            }

            val newArray = arrayOfNulls<Any>(array.size - 2)
            System.arraycopy(array, 0, newArray, 0, index)
            System.arraycopy(array, index + 2, newArray, index, newArray.size - index)
            return CollisionNode(hash, count - 1, newArray)
        }

        override fun <T> iterator(fn: (Any?, Any?) -> T): Iterator<T>? {
            return NodeIterator(array, fn)
        }
    }

    internal class NodeIterator<T>(
            private val array: Array<Any?>,
            private val fn: (Any?, Any?) -> T
    ): Iterator<T> {

        private var index = 0
        private var nextValue: T? = null
        private var nextIterator: Iterator<T>? = null


        override fun hasNext(): Boolean {
            if (nextIterator != null || nextValue != null) {
                return true
            }

            return advance()
        }

        private fun advance(): Boolean {
            while (index < array.size) {
                val maybeKey = array[index]
                val maybeVal = array[index + 1]

                index += 2

                if (maybeKey != null) {
                    nextValue = fn(maybeKey, maybeVal)
                    return true
                }

                if (maybeVal != null) {
                    val iter = (maybeVal as MapNode<*, *>).iterator(fn)
                    if (iter != null && iter.hasNext()) {
                        nextIterator = iter
                        return true
                    }
                }
            }
            return false
        }

        override fun next(): T {
            val value = nextValue
            if (value != null) {
                nextValue = null
                return value
            }

            val iter = nextIterator
            if (iter != null) {
                val result = iter.next()
                if (!iter.hasNext()) {
                    nextIterator = null
                }
                return result
            }

            if (advance()) {
                return next()
            }

            throw NoSuchElementException()
        }
    }
}