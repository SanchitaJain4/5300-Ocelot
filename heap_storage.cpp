/*
  heap_storage.cpp

  Implementation of a rudimentary storage engine.
  Uses heap file organization.
  Uses slotted page block architecture (using RecNo file type).
  Each block corresponds to 1 numbered record in the Berkeley DB file.
  Berkeley DB's buffer manager will handle disk read/write.
  Made up of 3 layers:
    SlottedPage: A specific page/block
    HeapFile: Handles the collection of blocks in the relation
              Handles file creation, deletion, and access
    HeapTable: Represents the relation (logical view) of the table

  Authors: Alex Larsen alarsen@seattleu.edu,
           Yao Yao yyao@seattleu.edu

*/

#include "heap_storage.h"
#include <cstring>

typedef u_int16_t u16;


// Test function -- returns true if all tests pass
bool test_heap_storage() {
	ColumnNames column_names;
	column_names.push_back("a");
	column_names.push_back("b");
	ColumnAttributes column_attributes;
	ColumnAttribute ca(ColumnAttribute::INT);
	column_attributes.push_back(ca);
	ca.set_data_type(ColumnAttribute::TEXT);
	column_attributes.push_back(ca);
    HeapTable table1("_test_create_drop_cpp", column_names, column_attributes);
    table1.create();
    std::cout << "create ok" << std::endl;
    table1.drop();  // drop makes the object unusable because of BerkeleyDB restriction -- maybe want to fix this some day
    std::cout << "drop ok" << std::endl;

    HeapTable table("_test_data_cpp", column_names, column_attributes);
    table.create_if_not_exists();
    std::cout << "create_if_not_exsts ok" << std::endl;

    ValueDict row;
    row["a"] = Value(12);
    row["b"] = Value("Hello!");
    std::cout << "try insert" << std::endl;
    table.insert(&row);
    std::cout << "insert ok" << std::endl;
    Handles* handles = table.select();
    std::cout << "select ok " << handles->size() << std::endl;
    ValueDict *result = table.project((*handles)[0]);
    std::cout << "project ok" << std::endl;
    Value value = (*result)["a"];
    if (value.n != 12)
    	return false;
    value = (*result)["b"];
    if (value.s != "Hello!")
		return false;
    table.drop();

    return true;
}


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
    db_open(DB_CREATE | DB_EXCL);
    SlottedPage* block = get_new();
    put(block);
}

// Deletes the database file
void HeapFile::drop(void) {
    close();
    const char* file_name = dbfilename.c_str();
    int result = std::remove(file_name);
    if (result != 0)
        throw std::string("Failed to delete file: ") + file_name;
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
void HeapFile::db_open(uint flags) {
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

// HeapTable constructor
// Takes the name of the relation, the columns (in order), and all the column attributes
// This information is tracked by the schema storage
HeapTable::HeapTable(Identifier table_name, ColumnNames column_names, ColumnAttributes column_attributes)
    : DbRelation(table_name, column_names, column_attributes), file(table_name) {}

// Sets up the DbFile and calls its create method
// Corresponds to the SQL command CREATE TABLE
void HeapTable::create() {
    try {
        file.create();
    }
    catch (...) {
        std::cerr << "Failed to create table" << std::endl;
    }
}

// Open the table if it already exists
// Otherwise, sets up the DbFile and calls its create method
// Corresponds to the SQL command CREATE TABLE IF NOT EXISTS
void HeapTable::create_if_not_exists() {
    try {
        file.create();
    } catch (...) {
        file.open();
    }
}

// Deletes the underlying DbFile
// Corresponds to the SQL command DROP TABLE
void HeapTable::drop() {
    file.drop();
}

// Opens the table for insert, update, delete, select, and project methods
void HeapTable::open() {
    file.open();
}

// Closes the table, temporarily disabling insert, update, delete, select, and project methods
void HeapTable::close() {
    file.close();
}

// Takes a proposed row and adds it to the table
// Corresponds to the SQL command INSERT INTO TABLE
Handle HeapTable::insert(const ValueDict *row) {
    file.open();

    // Determine the block to write to, marshal the data, and write it to the block
    return append(validate(row));
}

// TODO: implement update
void HeapTable::update(const Handle handle, const ValueDict *new_values) {
    throw "Not implemented";
}

// TODO: implement del
void HeapTable::del(const Handle handle) {
    throw "Not implemented";
}

// Returns handles to the matching rows
// Corresponds to the SQL query SELECT * FROM...
Handles* HeapTable::select() {
    Handles* handles = new Handles();
    BlockIDs* block_ids = file.block_ids();
    for (auto const& block_id: *block_ids) {
        SlottedPage* block = file.get(block_id);
        RecordIDs* record_ids = block->ids();
        for (auto const& record_id: *record_ids)
            handles->push_back(Handle(block_id, record_id));
        delete record_ids;
        delete block;
    }
    delete block_ids;
    return handles;
}

// TODO: implement select with where clause
Handles* HeapTable::select(const ValueDict *where) {
    throw "Not implemented";
}

// Extracts all fields from a row handle
ValueDict* HeapTable::project(Handle handle) {
    BlockID block_id = handle.first;
    RecordID record_id = handle.second;
    SlottedPage *block = file.get(block_id);
    Dbt *data = block->get(record_id);
    ValueDict *row = unmarshal(data);
    return row;
}

// Extracts specific fields from a row handle
ValueDict* HeapTable::project(Handle handle, const ColumnNames *column_names) {
    BlockID block_id = handle.first;
    RecordID record_id = handle.second;
    SlottedPage *block = file.get(block_id);
    Dbt *data = block->get(record_id);
    ValueDict *row = unmarshal(data);
    if (column_names->empty())
        return row;
    ValueDict *result = new ValueDict;
    for (auto const& column_name : *column_names) {
        (*result)[column_name] = (*row)[column_name];
    }
    return result;
}

// Check whether a row is valid to insert into
ValueDict* HeapTable::validate(const ValueDict *row) {
    ValueDict* full_row = new ValueDict;
    for (auto const& column_name : column_names) {
        ValueDict::const_iterator column = row->find(column_name);
        if(column == row->end()) {
            throw "Column name not found in row: " + column_name;
        }
        else {
            Value val = column->second;
            (*full_row)[column_name] = val;
        }
    }
    return full_row;
}

// Appends a record to a file
Handle HeapTable::append(const ValueDict *row) {
    Dbt *data = marshal(row);
    SlottedPage *block = file.get(file.get_last_block_id());
    RecordID record_id;
    try {
        record_id = block->add(data);
    }
    catch (...) {
        block = file.get_new();
        record_id = block->add(data);
    }
    file.put(block);
    Handle result = Handle(file.get_last_block_id(), record_id);
    return result;
}

// Serialize a row into bits to go into the file
// Caller responsible for freeing the returned Dbt and its enclosed ret->get_data().
Dbt* HeapTable::marshal(const ValueDict* row) {
    char *bytes = new char[DbBlock::BLOCK_SZ]; // more than we need (we insist that one row fits into DbBlock::BLOCK_SZ)
    uint offset = 0;
    uint col_num = 0;
    for (auto const& column_name: this->column_names) {
        ColumnAttribute ca = this->column_attributes[col_num++];
        ValueDict::const_iterator column = row->find(column_name);
        Value value = column->second;
        if (ca.get_data_type() == ColumnAttribute::DataType::INT) {
            *(int32_t*) (bytes + offset) = value.n;
            offset += sizeof(int32_t);
        } else if (ca.get_data_type() == ColumnAttribute::DataType::TEXT) {
            uint size = value.s.length();
            *(u16*) (bytes + offset) = size;
            offset += sizeof(u16);
            memcpy(bytes+offset, value.s.c_str(), size); // assume ascii for now
            offset += size;
        } else {
            throw DbRelationError("Only know how to marshal INT and TEXT");
        }
    }
    char *right_size_bytes = new char[offset];
    memcpy(right_size_bytes, bytes, offset);
    delete[] bytes;
    Dbt *data = new Dbt(right_size_bytes, offset);
    return data;
}

// Deserializes the marshaled data
ValueDict* HeapTable::unmarshal(Dbt *data) {
    ValueDict *row = new ValueDict;
    char *bytes = (char *)data->get_data();
    uint index = 0;
    uint offset = 0;
    for (auto const& column_name : column_names) {
        ColumnAttribute attr = column_attributes[index++];
        Value val;
        if (attr.get_data_type() == ColumnAttribute::DataType::INT) {
            val.n = *(int32_t *)(bytes + offset);
            offset += sizeof(int32_t);
        }
        else if (attr.get_data_type() == ColumnAttribute::DataType::TEXT) {
            u16 size = *(u16 *)(bytes + offset);
            offset += sizeof(u16);
            val.s = std::string(bytes + offset);
            offset += size;
        }
        else
            throw DbRelationError("Marshal only supports INT and TEXT");

        (*row)[column_name] = val;
    }
    return row;
}
