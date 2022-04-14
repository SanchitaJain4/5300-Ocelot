/*
  SQLParser.cpp

  SQL statement parser that prints the parse tree of a given SQL statement

  Authors: Alex Larsen alarsen@seattleu.edu,
           Yao Yao yyao@seattleu.edu

*/

#include <iostream>
#include <string>
#include <db_cxx.h>
#include "SQLParser.h"

using namespace std;
using namespace hsql;

string execute(const SQLStatement* parseTree);
string parseExpression(Expr* expr);
string parseOperator(Expr* expr);
string parseTableRef(TableRef* table);
string columnDefToString(ColumnDefinition* col);

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
        cin >> input;
        cout << endl;

        // End loop if user specifies "break"
        if (input == "quit")
            break;
		
		// Skip blank lines
        if (input.length() < 1)
            continue;
		
		// Parse the response with SQLParser's ParseSQLString function
		SQLParserResult *result = SQLParser::parseSQLString(input);

		if (!result->isValid())
			cout << "invalid SQL: " << input << endl;
		else {
			for (long unsigned int i = 0; i < result->size(); i++) {	
				cout << execute(result->getStatement(i)) << endl;
			}
		}
    }

	return 0;
}


string execute(const SQLStatement* parseTree) {
    string result = "";
    
    switch (parseTree->type()) {
		case kStmtCreate: {
			CreateStatement* createSmt = (CreateStatement*)parseTree;
            result += "CREATE TABLE ";
			result += string(createSmt->tableName);
			if (createSmt->columns != nullptr) {
				result += " (";
				for (ColumnDefinition* col: *createSmt->columns) {
					result += columnDefToString(col);
					result += ", ";
				}
				result.resize(result.size() - 2);
				result += ")";
			}
            break;
		}
        case kStmtSelect: {
            SelectStatement* selectsmt = (SelectStatement*)parseTree;
            result += "SELECT ";
			for(long unsigned int i = 0; i < selectsmt->selectList->size(); i++) {
				result += parseExpression(selectsmt->selectList->at(i));
				if(i < (selectsmt->selectList->size() - 1)) {
					result += ", ";
				}
			}
            result += " FROM ";
            result += parseTableRef(selectsmt->fromTable);
			if (selectsmt->whereClause != NULL) {
				result += " WHERE ";
				result += parseExpression(selectsmt->whereClause);
			}
            break;
		}
        default:
            result = "Not implemented";
    }

    return result;
}

string parseExpression(Expr* expr) {
    if(!expr)
		return "";

    string str = "";

    switch(expr->type) {
        case kExprStar:
            str += "*";
            break;
        case kExprColumnRef:
            if(expr->table != nullptr) {
                str += expr->table;
				str += ".";
            }
            str += expr->name;
            break;
        case kExprOperator:
            if(expr == nullptr) {
                str += "null ";
            }
            else {
                str += parseOperator(expr) + " ";
                if(expr->expr2 != nullptr) {
                    str += parseExpression(expr->expr2);
                }
                else if(expr->exprList != nullptr) {
                    for(Expr* e : *expr->exprList) {
                        str += parseExpression(e);
                    }
                }
            }
            break;
        default:
            cerr << "Expression type not found: " << expr->type << endl;
    }

    return str;
}

string parseOperator(Expr* expr)
{
	string str;
	
	if (expr == nullptr)
		return "null";
	
	if (expr->expr != nullptr)
		str += parseExpression(expr->expr) + " ";
	else
		str += "null ";
	
	switch (expr->opType) {
		case Expr::SIMPLE_OP:
			str += expr->opChar;
			break;
		case Expr::AND:
			str += "AND";
			break;
		case Expr::OR:
			str += "OR";
			break;
		case Expr::NOT:
			str += "NOT";
			break;
		default:
			str += expr->opType;
			break;
	}
	
	if (expr->expr2 != nullptr)
		str += " " + parseExpression(expr->expr2);
	else
		str += " null";
	
	return str;
}

string parseTableRef(TableRef* table)
{
	//TODO
	return "NOTIMPL";
}

string columnDefToString(ColumnDefinition* col)
{
	string str;
	str += col->name;
	
	switch (col->type) {
		case ColumnDefinition::DOUBLE:
			str += " DOUBLE";
			break;
		case ColumnDefinition::INT:
			str += " INT";
			break;
		case ColumnDefinition::TEXT:
			str += " TEXT";
			break;
		default:
			str += " ?";
			break;
	}
	
	return str;
}