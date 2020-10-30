
#include <iostream>
#include <string>
#include <fstream>
#include <streambuf>
#include <regex>
#include <fstream>
#include <streambuf>
#include <chrono>
#include <stdio.h>

#include <sqlite3.h>

extern "C" {
extern int sqlite3_csv_init(sqlite3 *db, char **pzErrMsg, const sqlite3_api_routines *pApi);
extern int sqlite3_tbl_init(sqlite3 *db, char **pzErrMsg, const sqlite3_api_routines *pApi);
}

static int record_counter = 0;

static int db_callback(void* data, int argc, char** argv, char** azColName);

int main (int argc, char *argv[]) 
{
    sqlite3 *db;    
    int rc;
    char  * errmsg;
    std::string sqlFileName;
    std::string sqlQuery;
    
    if(argc < 4){
        std::cerr << "Usage: " << argv[0] << " 3 file.tbl \"SQL query\" " << std::endl;
        return 1;
    }

    sqlFileName = std::string(argv[2]);
    sqlQuery = std::string(argv[3]);
    
    //sqlite3_soft_heap_limit64(1<<30);
    //sqlite3_hard_heap_limit64(2<<30);

    sqlite3_config(SQLITE_CONFIG_MEMSTATUS, 0); // Disable memory statistics
    //sqlite3_config(SQLITE_CONFIG_SINGLETHREAD);

    rc = sqlite3_open(":memory:", &db);

    if( rc ) {
        std::cerr << "Can't open database: " << sqlite3_errmsg(db) << std::endl;
        return(1);
    }

    rc = sqlite3_csv_init(db, &errmsg, NULL);
    if(rc != SQLITE_OK) {
        std::cerr << "Can't load csv extention: " << errmsg << std::endl;
        sqlite3_free(errmsg);
        return 1;
    }

    rc = sqlite3_tbl_init(db, &errmsg, NULL);
    if(rc != SQLITE_OK) {
        std::cerr << "Can't load tbl extention: " << errmsg << std::endl;
        sqlite3_free(errmsg);
        return 1;
    }

    std::string sqlCreateVirtualTable;

    bool tbl_mode = false;
    size_t extIndex = sqlFileName.find(".tbl");
    if (extIndex != std::string::npos) {
        tbl_mode = true;
    }

    if(!tbl_mode){
        sqlCreateVirtualTable = "CREATE VIRTUAL TABLE S3Object USING csv(filename='";
        sqlCreateVirtualTable += sqlFileName + "'";
        sqlCreateVirtualTable += ", header=true" ;
    }

    if(tbl_mode){         
        std::string schemaFileName = sqlFileName.substr(0, extIndex) + ".schema";                   

        sqlCreateVirtualTable = "CREATE VIRTUAL TABLE S3Object USING tbl(filename='";
        sqlCreateVirtualTable += sqlFileName + "'";
        std::ifstream fs(schemaFileName);
        std::string schema((std::istreambuf_iterator<char>(fs)), std::istreambuf_iterator<char>());
        
        if(!schema.empty()) {
            std::replace(schema.begin(), schema.end(), '\n', ' ');
            sqlCreateVirtualTable += ", schema=CREATE TABLE S3Object (" + schema + ")";
        }
    }

    sqlCreateVirtualTable += ");";

    std::chrono::high_resolution_clock::time_point t1 =  std::chrono::high_resolution_clock::now();

    rc = sqlite3_exec(db, sqlCreateVirtualTable.c_str(), NULL, NULL, &errmsg);
    if(rc != SQLITE_OK) {
        std::cerr << "Can't create virtual table: " << errmsg << std::endl;
        sqlite3_free(errmsg);
        sqlite3_close(db);
        return 1;
    }

    std::chrono::high_resolution_clock::time_point t2 =  std::chrono::high_resolution_clock::now();

    sqlite3_stmt *sqlRes;
    rc = sqlite3_prepare_v2(db, sqlQuery.c_str(), -1, &sqlRes, 0);        
    if (rc != SQLITE_OK) {
        std::cerr << "Can't execute query: " << sqlite3_errmsg(db) << std::endl;        
        sqlite3_close(db);
        return 1;
    }            

    while ( (rc = sqlite3_step(sqlRes)) == SQLITE_ROW) {
        record_counter++;

        if( record_counter > 5 && record_counter < 6001211){
            continue;
        }

        int data_count = sqlite3_data_count(sqlRes);

        for(int i = 0; i < data_count; i++) {
            const unsigned char* text = sqlite3_column_text(sqlRes, i);
            if(text){
                if(!strchr((const char*)text, ',')){
                    std::cout << text << ",";
                } else {
                    std::cout << "\"" << text << "\"" << ",";
                }
            }
        }
        std::cout << std::endl;
    }
    
    std::chrono::high_resolution_clock::time_point t3 =  std::chrono::high_resolution_clock::now();

    std::chrono::duration<double, std::milli> create_time = t2 - t1;
    std::chrono::duration<double, std::milli> select_time = t3 - t2;

    std::cout << "Records " << record_counter;
    std::cout << " create_time " << create_time.count()/ 1000 << " sec" ;
    std::cout << " select_time " << select_time.count()/ 1000 << " sec" << std::endl;

    sqlite3_finalize(sqlRes);
    sqlite3_close(db);

    return(0);
}

static int db_callback(void* data, int argc, char** argv, char** azColName) 
{ 
    int i; 
    //fprintf(stderr, "%s: ", (const char*)data); 
  
    record_counter++;
    if(record_counter > 5){
        return 0;
    }
    

    for (i = 0; i < argc; i++) { 
        //printf("%s = %s\n", azColName[i], argv[i] ? argv[i] : "NULL");
        //std::cout << (argv[i] ? argv[i] : "NULL");
        if(argv[i]){
            if (std::string(argv[i]).find(',') != std::string::npos) {
                // Comma found in data
                std::cout << '"' << argv[i] << '"';
            } else {
                std::cout << argv[i];
            }
        } else {
            std::cout << "NULL";
        }
        if(i < argc -1) {
            std::cout << ",";
        }
    } 
  
    std::cout << std::endl;
    //printf("\n"); 
    return 0; 
} 
