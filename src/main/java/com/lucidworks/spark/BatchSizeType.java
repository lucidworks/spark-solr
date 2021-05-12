package com.lucidworks.spark;

/**
 * Specifices what type of Solr batch you are using.
 * A) based on the number of documents in the batch.
 * Or B) based on the number of bytes in the batch.
 */
public enum BatchSizeType {
    NUM_DOCS,
    NUM_BYTES
}
