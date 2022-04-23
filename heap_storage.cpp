/*
  heap_storage.cpp

  Authors: Alex Larsen alarsen@seattleu.edu,
           Yao Yao yyao@seattleu.edu

*/

#include "heap_storage.h"
bool test_heap_storage() {return true;}

typedef u_int16_t u16;


// SlottedPage

SlottedPage::SlottedPage(Dbt &block, BlockID block_id, bool is_new) : DbBlock(block, block_id, is_new) {
    if (is_new) {
        this->num_records = 0;
        this->end_free = DbBlock::BLOCK_SZ - 1;
        put_header();
    } else {
        get_header(this->num_records, this->end_free);
    }
}

// Add a new record to the block. Return its id.
RecordID SlottedPage::add(const Dbt* data) {
    if (!has_room(data->get_size()))
        throw DbBlockNoRoomError("not enough room for new record");
    u16 id = ++this->num_records;
    u16 size = (u16) data->get_size();
    this->end_free -= size;
    u16 loc = this->end_free + 1;
    put_header();
    put_header(id, size, loc);
    memcpy(this->address(loc), data->get_data(), size);
    return id;
}

// TODO
Dbt* SlottedPage::get(RecordID record_id);

// TODO
void SlottedPage::put(RecordID record_id, const Dbt &data);

// TODO
void SlottedPage::del(RecordID record_id);

// TODO
RecordIDs* SlottedPage::ids(void);

// TODO
virtual void SlottedPage::get_header(u_int16_t &size, u_int16_t &loc, RecordID id = 0);

// Store the size and offset for given id. For id of zero, store the block header.
void SlottedPage::put_header(RecordID id, u16 size, u16 loc) {
    if (id == 0) { // called the put_header() version and using the default params
        size = this->num_records;
        loc = this->end_free;
    }
    put_n(4*id, size);
    put_n(4*id + 2, loc);
}

// TODO
bool SlottedPage::has_room(u_int16_t size);

// TODO
void SlottedPage::slide(u_int16_t start, u_int16_t end);

// Get 2-byte integer at given offset in block.
u16 SlottedPage::get_n(u16 offset) {
    return *(u16*)this->address(offset);
}

// Put a 2-byte integer at given offset in block.
void SlottedPage::put_n(u16 offset, u16 n) {
    *(u16*)this->address(offset) = n;
}

// Make a void* pointer for a given offset into the data block.
void* SlottedPage::address(u16 offset) {
    return (void*)((char*)this->block.get_data() + offset);
}


// HeapFile

// Creates the database file that will store the blocks for a relation
void HeapFile::create(void) {
    db_open(DB_CREATE | DB_TRUNCATE);
    SlottedPage* block = get_new();
    put(block);
}

// Deletes the database file
void HeapFile::drop(void) {
    string file_name = dbfilename.c_str();
    int result = std::remove(file_name);
    if (result != 0) {
        throw "failed to remove the physical file " + file_name;
}

// Opens the database file
void HeapFile::open(void) {
    db_open();
}

// Closes the database file
void HeapFile::close(void) {
    db.close(0);
    closed = true;
}

// Allocate a new block for the database file.
// Returns the new empty DbBlock that is managing the records in this block and its block id.
SlottedPage* HeapFile::get_new(void) {
    char block[DbBlock::BLOCK_SZ];
    std::memset(block, 0, sizeof(block));
    Dbt data(block, sizeof(block));

    int block_id = ++this->last;
    Dbt key(&block_id, sizeof(block_id));

    // write out an empty block and read it back in so Berkeley DB is managing the memory
    SlottedPage* page = new SlottedPage(data, this->last, true);
    this->db.put(nullptr, &key, &data, 0); // write it out with initialization applied
    this->db.get(nullptr, &key, &data, 0);
    return page;
}

// Gets a block from the database file for a given block id
// The client code can then read or modify the block via the DbBlock interface
SlottedPage* HeapFile::get(BlockID block_id) {
    Dbt key(&block_id, sizeof(block_id));
    Dbt data;
    db.get(nullptr, &key, &data, 0);
    SlottedPage* page = new SlottedPage(data, block_id);
    return page;
}

// Writes a block to the file
void HeapFile::put(DbBlock *block) {
    BlockID block_id(block->get_block_id());
    Dbt key(&block_id, sizeof(block_id));
    db.put(nullptr, &key, block->get_block(), 0);
}

// Iterates through all the block ids in the file
BlockIDs* HeapFile::block_ids() {
    BlockIDs* ids = new BlockIDs;
    for (BlockID i = 1; i <= last; i++)
        ids->push_back(i);
    return ids;
}

// Wrapper for Berkeley DB open
void HeapFile::db_open(uint flags = 0) {
    if (closed) {
        db.set_message_stream(&std::cout);
        db.set_error_stream(&std::cerr);
        db.set_re_len(DbBlock::BLOCK_SZ);
        dbfilename = name + ".db";
        int result = db.open(nullptr, dbfilename.c_str(), nullptr, DB_RECNO, flags, 0);
        if(result != 0) {
            close();
        }
        closed = false;
    }
}


// HeapTable

// TODO
HeapTable::HeapTable(Identifier table_name, ColumnNames column_names, ColumnAttributes column_attributes) {}

// TODO
void HeapTable::create() {
    try {
        file.create();
    }
    catch (...) {
        std::cerr << "Failed to create table" << std::endl;
    }
}

// TODO
void HeapTable::create_if_not_exists() {
    try {
        file.create();
    } catch (...) {
        file.open();
    }
}

// TODO
void HeapTable::drop() {
    file.drop();
}

// TODO
void HeapTable::open() {
    file.open();
}

// TODO
void HeapTable::close() {
    file.close()
}

// TODO
Handle HeapTable::insert(const ValueDict *row);

// TODO
void HeapTable::update(const Handle handle, const ValueDict *new_values) {
    throw "Not implemented"
}

// TODO
void HeapTable::del(const Handle handle) {
    throw "Not implemented"
}

// TODO
Handles* HeapTable::select() {
    throw "Not implemented"
}

// TODO
Handles* HeapTable::select(const ValueDict *where) {
    throw "Not implemented"
}

// TODO
ValueDict* HeapTable::project(Handle handle) {
    throw "Not implemented"
}

// TODO
ValueDict* HeapTable::project(Handle handle, const ColumnNames *column_names) {
    throw "Not implemented"
}

// TODO
ValueDict* HeapTable::validate(const ValueDict *row);

// TODO
Handle HeapTable::append(const ValueDict *row);

// TODO
Dbt* HeapTable::marshal(const ValueDict *row);

// TODO
ValueDict* HeapTable::unmarshal(Dbt *data);
