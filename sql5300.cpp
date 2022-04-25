/*
  SQLParser.cpp

  SQL statement parser that prints the parse tree of a given SQL statement.
  Referenced sqlHelper.cpp for implementation.

  Authors: Alex Larsen alarsen@seattleu.edu,
           Yao Yao yyao@seattleu.edu

*/

#include <iostream>
#include <string>
#include <db_cxx.h>
#include "SQLParser.h"
#include "heap_storage.h"

using namespace std;
using namespace hsql;

DbEnv *_DB_ENV;

string execute(const SQLStatement* parseTree);
string parseSelect(SelectStatement* stmt);
string parseCreate(CreateStatement* stmt);
string parseExpression(Expr* expr);
string parseOperator(Expr* expr);
string parseTableRef(TableRef* table);
string columnDefToString(ColumnDefinition* col);


// User input loop program
// Takes user input and prints the converted parse tree
// The user must supply the BerkelyDB environment path as a parameter
int main(int argc, char* argv[]) {
	
	// Invalid usage
    if (argc != 2) {        
        cerr << "Usage: ./sql5300 env_path" << endl;
        exit(-1);
    }
    
	// Create the DB environment
    DbEnv env(0U);
	env.set_message_stream(&cout);
	env.set_error_stream(&cerr);
	try {
		env.open(argv[1], (DB_CREATE | DB_INIT_MPOOL), 0);
	}
	catch (...) {
		cerr << "Exception when opening database environment" << endl;
		exit(-1);
	}

    // Begin the user input loop
    while (true) 
    {	
		cout << "SQL> ";
        string input;
        getline(cin, input);

        // Skip blank lines
        if (input.length() < 1)
            continue;

        // End loop if user specifies "break"
        if (input == "quit")
            break;

        // Test rudimentary storage engine
        if (input == "test") {
            cout << "test_heap_storage: " << (test_heap_storage() ? "ok" : "failed") << endl;
            continue;
        }

        // Parse the response with SQLParser's ParseSQLString function
        SQLParserResult *result = SQLParser::parseSQLString(input);

        // If valid, parse the statement
		if (!result->isValid())
			cout << "Invalid SQL: " << input << endl;
		else {
			for (long unsigned int i = 0; i < result->size(); i++) {	
				cout << execute(result->getStatement(i)) << endl;
			}
		}
    }

	return 0;
}


// Parses the parseTree parameter into a valid SQL statement and returns it
// TODO: Implement insert, import, and show
string execute(const SQLStatement* parseTree) {
    string result = "";
    
    switch (parseTree->type()) {
        case kStmtSelect: {
            result += parseSelect((SelectStatement*)parseTree);
            break;
		}
		case kStmtCreate: {
			result += parseCreate((CreateStatement*)parseTree);
            break;
		}
        default:
            result = "Statement type not implemented: ";
            result += parseTree->type();
    }

    return result;
}

// Parses a valid SQL select statement
string parseSelect(SelectStatement* stmt) {
    string result = "SELECT ";

    // Loop through each statement
    for(long unsigned int i = 0; i < stmt->selectList->size(); i++) {
        result += parseExpression(stmt->selectList->at(i));
        // Create a comma separated list
        if(i < (stmt->selectList->size() - 1)) {
            result += ", ";
        }
    }

    // Parse the from table
    result += " FROM ";
    result += parseTableRef(stmt->fromTable);

    // Parse the where clause expression
    if (stmt->whereClause != NULL) {
        result += " WHERE ";
        result += parseExpression(stmt->whereClause);
    }
    
    return result;
}

// Parses a valid SQL create statement
string parseCreate(CreateStatement* stmt) {
    string result = "CREATE TABLE ";

    result += string(stmt->tableName);

    // Get a comma separated list of the columns
    if (stmt->columns != nullptr) {
        result += " (";
        for (ColumnDefinition* col: *stmt->columns) {
            result += columnDefToString(col);
            result += ", ";
        }
        result.resize(result.size() - 2);
        result += ")";
    }
    
    return result;
}

// Parses a SQL expression
string parseExpression(Expr* expr) {
    
    // Return a blank string if the expression is null
    if(!expr)
		return "";

    string result = "";

    switch(expr->type) {
        case kExprStar:
            result += "*";
            break;
        case kExprColumnRef:
            if(expr->table != nullptr) {
                result += expr->table;
				result += ".";
            }
            result += expr->name;
            break;
        case kExprLiteralFloat:
            result += to_string(expr->fval);
            break;
        case kExprLiteralInt:
            result += to_string(expr->ival);
            break;
        case kExprLiteralString:
            result += string(expr->name);
            break;
        case kExprFunctionRef:
            result += string(expr->name);
            result += string(expr->expr->name);
            break;
        case kExprOperator:
            if(expr == nullptr) {
                result += "NULL ";
            }
            else {
                // Parse the lefthand side of the expression
                if (expr->expr != nullptr)
                    result += parseExpression(expr->expr) + " ";
                else
                    result += "NULL ";
                // Parse the operator
                result += parseOperator(expr) + " ";
                // Parse the righthand side of the expression
                if(expr->expr2 != nullptr) {
                    result += parseExpression(expr->expr2);
                }
                else if(expr->exprList != nullptr) {
                    for(Expr* e : *expr->exprList) {
                        result += parseExpression(e);
                    }
                }
            }
            break;
        default:
            cerr << "Unrecognized expression type: " << expr->type << endl;
    }

    return result;
}

// Parses an operator as part of a SQL expression
string parseOperator(Expr* expr)
{
	string result;
	
	if (expr == nullptr)
		return "null";
	
	switch (expr->opType) {
		case Expr::SIMPLE_OP:
			result += expr->opChar;
			break;
		case Expr::AND:
			result += "AND";
			break;
		case Expr::OR:
			result += "OR";
			break;
		case Expr::NOT:
			result += "NOT";
			break;
		default:
			result += expr->opType;
			break;
	}

	return result;
}

// Parses a SQL table reference
string parseTableRef(TableRef* table)
{
	string result;
    
    switch (table->type) {
        // Parse the name of the table
        case kTableName:
            result += table->name;
            break;
        // Parse a select statement placed on the table
        case kTableSelect:
            result += parseSelect(table->select);
            break;          
        // Parse a join
        case kTableJoin:
            result += parseTableRef(table->join->left);
            switch (table->join->type) {
                case kJoinInner:
                    result += " JOIN ";
                    break;
                case kJoinOuter:
                    result += " OUTER JOIN ";
                    break;
                case kJoinLeft:
                    result += " LEFT JOIN ";
                    break;
                case kJoinRight:
                    result += " RIGHT JOIN ";
                    break;
                default:
                    result += " ? "; // Not recognized/implemented
                    break;
            }
            
            result += parseTableRef(table->join->right);

            if (table->join->condition != nullptr)
            {
                result += " ON " + parseExpression(table->join->condition);
            }
            break;
        // Parses a cross product between tables
        case kTableCrossProduct:
            for (TableRef* tbl : *table->list) {
                result += parseTableRef(tbl);
                result += ", ";
            }
            result.resize(result.size() - 2);
            break;
    }

    // If the table uses an alias, parse it
    if (table->alias != nullptr)
    {
        result += " AS ";
        result += table->alias;
    }

    return result;
}

// Converts a ColumnDefinition variable to a string
string columnDefToString(ColumnDefinition* col)
{
	string result;
	result += col->name;
	
	switch (col->type) {
		case ColumnDefinition::DOUBLE:
			result += " DOUBLE";
			break;
		case ColumnDefinition::INT:
			result += " INT";
			break;
		case ColumnDefinition::TEXT:
			result += " TEXT";
			break;
		default:
			result += " ?";
			break;
	}

	return result;
}
