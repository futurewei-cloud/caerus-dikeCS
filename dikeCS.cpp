#include <Poco/Net/ServerSocket.h>
#include <Poco/Net/HTTPServer.h>
#include <Poco/Net/HTTPRequestHandler.h>
#include <Poco/Net/HTTPRequestHandlerFactory.h>
#include <Poco/Net/HTTPResponse.h>
#include <Poco/Net/HTTPServerRequest.h>
#include <Poco/Net/HTTPServerResponse.h>
#include <Poco/Util/ServerApplication.h>

#include <Poco/Util/AbstractConfiguration.h>
#include <Poco/Util/XMLConfiguration.h>


#include <aws/event-stream/event_stream.h>
#include <aws/common/encoding.h>
#include <aws/checksums/crc.h> 

#include <iostream>
#include <fstream>
#include <string>
#include <vector>
#include <stdlib.h>
#include <ctype.h>
#include <memory.h>

#include <sqlite3.h>
#include <stdio.h>

extern "C" {
extern int sqlite3_csv_init(sqlite3 *db, char **pzErrMsg, const sqlite3_api_routines *pApi);
extern int sqlite3_tbl_init(sqlite3 *db, char **pzErrMsg, const sqlite3_api_routines *pApi);
}

using namespace Poco::Net;
using namespace Poco::Util;
using namespace std;

const char MESSAGE_TYPE_HEADER[] = ":message-type";
const char MESSAGE_TYPE_EVENT[] = "event";

const char CONTENT_TYPE_HEADER[] = ":content-type";
const char CONTENT_TYPE_OCTET_STREAM[] = "application/octet-stream";

const char EVENT_TYPE_HEADER[] = ":event-type";
const char EVENT_TYPE_RECORDS[] = "Records";
const char EVENT_TYPE_END[] = "End";
const char EVENT_TYPE_CONT[] = "Cont";

const char ERROR_CODE_HEADER[] = ":error-code";
const char ERROR_MESSAGE_HEADER[] = ":error-message";
const char EXCEPTION_TYPE_HEADER[] = ":exception-type";

int SendPayload(ostream& outStream, const char * data, int len)
{
  struct aws_array_list headers;
  struct aws_allocator *alloc = aws_default_allocator();
  struct aws_event_stream_message msg;    
  struct aws_byte_buf payload = aws_byte_buf_from_array(data, len);
  
  aws_event_stream_headers_list_init(&headers, alloc);        
  aws_event_stream_add_string_header(&headers, MESSAGE_TYPE_HEADER, sizeof(MESSAGE_TYPE_HEADER) - 1, MESSAGE_TYPE_EVENT, sizeof(MESSAGE_TYPE_EVENT) - 1, 0);
  aws_event_stream_add_string_header(&headers, CONTENT_TYPE_HEADER, sizeof(CONTENT_TYPE_HEADER) - 1, CONTENT_TYPE_OCTET_STREAM, sizeof(CONTENT_TYPE_OCTET_STREAM) - 1, 0);
  aws_event_stream_add_string_header(&headers, EVENT_TYPE_HEADER, sizeof(EVENT_TYPE_HEADER) - 1, EVENT_TYPE_RECORDS, sizeof(EVENT_TYPE_RECORDS) - 1, 0);

  aws_event_stream_message_init(&msg, alloc, &headers, &payload);

  //dike_debug_print_buffer(aws_event_stream_message_buffer(&msg), aws_event_stream_message_total_length(&msg));
  outStream.write((const char *)aws_event_stream_message_buffer(&msg), aws_event_stream_message_total_length(&msg));  

  aws_byte_buf_clean_up(&payload);
  aws_event_stream_message_clean_up(&msg);
  aws_event_stream_headers_list_cleanup(&headers);

  return 0;
}
int SendCont(ostream& outStream)
{
  struct aws_array_list headers;
  struct aws_allocator *alloc = aws_default_allocator();
  struct aws_event_stream_message msg;        
  
  aws_event_stream_headers_list_init(&headers, alloc);        
  aws_event_stream_add_string_header(&headers, MESSAGE_TYPE_HEADER, sizeof(MESSAGE_TYPE_HEADER) - 1, MESSAGE_TYPE_EVENT, sizeof(MESSAGE_TYPE_EVENT) - 1, 0);    
  aws_event_stream_add_string_header(&headers, EVENT_TYPE_HEADER, sizeof(EVENT_TYPE_HEADER) - 1, EVENT_TYPE_CONT, sizeof(EVENT_TYPE_CONT) - 1, 0);
  aws_event_stream_message_init(&msg, alloc, &headers, NULL);    
  outStream.write((const char *)aws_event_stream_message_buffer(&msg), aws_event_stream_message_total_length(&msg));    
  aws_event_stream_message_clean_up(&msg);
  aws_event_stream_headers_list_cleanup(&headers);

  return 0;
}

int SendEnd(ostream& outStream)
{
  struct aws_array_list headers;
  struct aws_allocator *alloc = aws_default_allocator();
  struct aws_event_stream_message msg;        
  
  aws_event_stream_headers_list_init(&headers, alloc);        
  aws_event_stream_add_string_header(&headers, MESSAGE_TYPE_HEADER, sizeof(MESSAGE_TYPE_HEADER) - 1, MESSAGE_TYPE_EVENT, sizeof(MESSAGE_TYPE_EVENT) - 1, 0);    
  aws_event_stream_add_string_header(&headers, EVENT_TYPE_HEADER, sizeof(EVENT_TYPE_HEADER) - 1, EVENT_TYPE_END, sizeof(EVENT_TYPE_END) - 1, 0);
  aws_event_stream_message_init(&msg, alloc, &headers, NULL);    
  outStream.write((const char *)aws_event_stream_message_buffer(&msg), aws_event_stream_message_total_length(&msg));    
  aws_event_stream_message_clean_up(&msg);
  aws_event_stream_headers_list_cleanup(&headers);

  return 0;
}

class DikeByteBuffer
{
  const int DIKE_BYTE_BUFFER_SIZE = 64<<10; 
  typedef int (*Callback_t)(DikeByteBuffer * buf, void * context);
  public:
    DikeByteBuffer() 
    {
      m_Buffer = (char*) malloc(DIKE_BYTE_BUFFER_SIZE);
      memset(m_Buffer, 0, DIKE_BYTE_BUFFER_SIZE);
      m_Pos = 0;
    }
    ~DikeByteBuffer(){
      free(m_Buffer);
    }

    void Write(ostream& outStream, const char * buf, int len){
      //cout << "Write: " <<  len << endl;
      if(m_Pos + len > DIKE_BYTE_BUFFER_SIZE){
        SendPayload(outStream, m_Buffer, m_Pos);
        m_Pos = 0;
      }
      memcpy(m_Buffer + m_Pos, buf, len);
      m_Pos += len;
    }

    void Flush(ostream& outStream) {         
      SendPayload(outStream, m_Buffer, m_Pos);
      m_Pos = 0;
    }

  private:
    char * m_Buffer;
    int m_Pos;
};

class SelectObjectContentHandler : public HTTPRequestHandler
{
public:
  
  virtual void handleRequest(HTTPServerRequest &req, HTTPServerResponse &resp)
  {
    resp.setStatus(HTTPResponse::HTTP_OK);
    resp.set("Content-Security-Policy", "block-all-mixed-content");
    resp.set("Vary", "Origin");
    resp.set("X-Amz-Request-Id", "1640125B8EDA3957");
    resp.set("X-Xss-Protection", "1; mode=block");

    resp.setContentType("application/octet-stream");    
    resp.setChunkedTransferEncoding(true);    
    resp.setKeepAlive(true);
    
    ostream& outStream = resp.send();    
    int rc;
    char  * errmsg;
 
    sqlite3 *db;
    rc = sqlite3_open(":memory:", &db);
    if( rc ) {
        cout << "Can't open database: " << sqlite3_errmsg(db);
        return;
    }

    rc = sqlite3_csv_init(db, &errmsg, NULL);
    if(rc != SQLITE_OK) {
        cout << "Can't load csv extention: " << errmsg << std::endl;
        sqlite3_free(errmsg);
        return;
    }

    rc = sqlite3_tbl_init(db, &errmsg, NULL);
    if(rc != SQLITE_OK) {
        cout << "Can't load tbl extention: " << errmsg << std::endl;
        sqlite3_free(errmsg);
        return;
    }

    string sqlPath = "/data";
    string sqlFileName = sqlPath + req.getURI().substr(0, req.getURI().find("?"));

    cout << sqlFileName << endl;
    
    string sqlCreateVirtualTable;

    bool tbl_mode = false;
    size_t extIndex = sqlFileName.find(".tbl");
    if (extIndex != string::npos) {
        tbl_mode = true;
    }

    if(!tbl_mode){
        sqlCreateVirtualTable = "CREATE VIRTUAL TABLE S3Object USING csv(filename='";
        sqlCreateVirtualTable += sqlFileName + "'";
        sqlCreateVirtualTable += ", header=true" ;
    }

    if(tbl_mode){         
        string schemaFileName = sqlFileName.substr(0, extIndex) + ".schema";                   

        sqlCreateVirtualTable = "CREATE VIRTUAL TABLE S3Object USING tbl(filename='";
        sqlCreateVirtualTable += sqlFileName + "'";
        ifstream fs(schemaFileName);
        string schema((istreambuf_iterator<char>(fs)), istreambuf_iterator<char>());
        
        if(!schema.empty()) {
            sqlCreateVirtualTable += ", schema=CREATE TABLE S3Object (" + schema + ")";
        }
    }

    sqlCreateVirtualTable += ");";

    rc = sqlite3_exec(db, sqlCreateVirtualTable.c_str(), NULL, NULL, &errmsg);
    if(rc != SQLITE_OK) {
        cout << "Can't create virtual table: " << errmsg << endl;
        sqlite3_free(errmsg);
        sqlite3_close(db);
        return;
    }

    AbstractConfiguration *cfg = new XMLConfiguration(req.stream());
    string sqlQuery = cfg->getString("Expression");
    cout << "SQL " << sqlQuery << endl;

    DikeByteBuffer dbb = DikeByteBuffer(); 

    sqlite3_stmt *sqlRes;
    rc = sqlite3_prepare_v2(db, sqlQuery.c_str(), -1, &sqlRes, 0);        
    if (rc != SQLITE_OK) {
        std::cerr << "Can't execute query: " << sqlite3_errmsg(db) << std::endl;        
        sqlite3_close(db);
        return;
    }            

    while ( (rc = sqlite3_step(sqlRes)) == SQLITE_ROW) {
      int data_count = sqlite3_data_count(sqlRes);

      for(int i = 0; i < data_count; i++) {
          const char* text = (const char*)sqlite3_column_text(sqlRes, i);            
          if(text){
            int len = strlen(text);
            if(strchr(text,',')){
                dbb.Write(outStream, "\"", 1);
                dbb.Write(outStream, text, len);
                dbb.Write(outStream, "\"", 1);
            } else {                  
                dbb.Write(outStream,text, len);
            }
          } else {            
              dbb.Write(outStream,"NULL", 4);
          }
          if(i < data_count -1) {                  
              dbb.Write(outStream,",", 1);
          }
      }
      dbb.Write(outStream,"\n", 1);     
    }

    /*
    rc = sqlite3_exec(db, sqlQuery.c_str(), db_callback, (void *) &dbb, &errmsg);
    if(rc != SQLITE_OK) {
        cout << "Can't execute query: " << errmsg << std::endl;
        sqlite3_free(errmsg);
        sqlite3_close(db);
        return;
    }
    */
    sqlite3_close(db);

    dbb.Flush(outStream);
    SendEnd(outStream);
    outStream.flush();    
  }
private:
  static int count;
};

int SelectObjectContentHandler::count = 0;

class MyRequestHandlerFactory : public HTTPRequestHandlerFactory
{
public:
  virtual HTTPRequestHandler* createRequestHandler(const HTTPServerRequest &)
  {
    return new SelectObjectContentHandler;
  }
};

class MyServerApp : public ServerApplication
{
protected:
  int main(const vector<string> &)
  {
    HTTPServer s(new MyRequestHandlerFactory, ServerSocket(9000), new HTTPServerParams);

    s.start();
    cout << endl << "Server started" << endl;

    waitForTerminationRequest();  // wait for CTRL-C or kill

    cout << endl << "Shutting down..." << endl;
    s.stop();

    return Application::EXIT_OK;
  }
};

int main(int argc, char** argv)
{
  MyServerApp app;

  return app.run(argc, argv);
}
