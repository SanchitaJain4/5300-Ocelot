# 5300-Ocelot

## Sprint Verano

### Team members

Alex Larsen  
Yao Yao  

### Build

Run `make` to build.  

### Milestone 1

SQL statement parser that prints the parse tree of a given SQL statement.  
To run:  
```$ ./sql5300 ~/cpsc5300/data```  
Then execute any valid SQL statement.  
To quit:  
```SQL> quit```  

### Milestone 2

A rudimentary storage engine with heap file organization.  
Uses slotted page block architecture (RecNo file type).  
Made up of 3 layers:  
- SlottedPage: A specific page/block  
- HeapFile: Handles the collection of blocks in the relation  
- HeapTable: Represents the relation (logical view) of the table  

To test the rudimentary storage engine, first run sql5300 (see Milestone 1)  
Then:  
```SQL> test```  

### Hand-Off Video

https://seattleu.instructuremedia.com/embed/444354bf-61e4-4e79-978a-8313b74d6de4