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
#include <assert.h>

#include <sqlite3.h>
#include <stdio.h>

#include "SelectObjectContent.hpp"
#include "TimeUtil.hpp"

extern "C" {
extern int sqlite3_csv_init(sqlite3 *db, char **pzErrMsg, const sqlite3_api_routines *pApi);
extern int sqlite3_tbl_init(sqlite3 *db, char **pzErrMsg, const sqlite3_api_routines *pApi);
}

#  define DIKE_ALWAYS_INLINE  __attribute__((always_inline)) inline
#  define DIKE_LIKELY(x)      __builtin_expect(!!(x), 1)
#  define DIKE_UNLIKELY(x)    __builtin_expect(!!(x), 0)

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

class DikeByteBuffer
{
  const int DIKE_BYTE_BUFFER_SIZE = 127<<10; 
  
  public:
    DikeByteBuffer() 
    {      
      struct aws_array_list headers;
      struct aws_allocator *alloc = aws_default_allocator();
      struct aws_event_stream_message msg;
      
      aws_event_stream_headers_list_init(&headers, alloc);        
      aws_event_stream_add_string_header(&headers, MESSAGE_TYPE_HEADER, sizeof(MESSAGE_TYPE_HEADER) - 1, MESSAGE_TYPE_EVENT, sizeof(MESSAGE_TYPE_EVENT) - 1, 0);
      aws_event_stream_add_string_header(&headers, CONTENT_TYPE_HEADER, sizeof(CONTENT_TYPE_HEADER) - 1, CONTENT_TYPE_OCTET_STREAM, sizeof(CONTENT_TYPE_OCTET_STREAM) - 1, 0);
      aws_event_stream_add_string_header(&headers, EVENT_TYPE_HEADER, sizeof(EVENT_TYPE_HEADER) - 1, EVENT_TYPE_RECORDS, sizeof(EVENT_TYPE_RECORDS) - 1, 0);  
      
      aws_event_stream_message_init(&msg, alloc, &headers, NULL);

      m_MsgLen = aws_event_stream_message_total_length(&msg);
      m_Msg.alloc = NULL;      
      m_Msg.message_buffer = (uint8_t *)aligned_alloc(128, m_MsgLen + DIKE_BYTE_BUFFER_SIZE);
      m_Msg.owns_buffer = 1;

      memcpy(m_Msg.message_buffer, aws_event_stream_message_buffer(&msg), m_MsgLen);
      aws_event_stream_message_clean_up(&msg);
      aws_event_stream_headers_list_cleanup(&headers);

      m_Payload = ( uint8_t *)aws_event_stream_message_payload(&m_Msg);
      m_Pos = 0;
    }

    ~DikeByteBuffer() {
        free(m_Msg.message_buffer);      
    }

    void Write(ostream& outStream, const char * buf, int len) {
        if(m_Pos + len > DIKE_BYTE_BUFFER_SIZE){        
            UpdateCrc(&m_Msg, m_Pos);
            outStream.write((const char *)(m_Msg.message_buffer) , m_MsgLen + m_Pos);
            m_Pos = 0;
        }
        memcpy(m_Payload + m_Pos, buf, len); // This cost 0.8 sec
        m_Pos += len;
    }

    void Flush(ostream& outStream) {      
        UpdateCrc(&m_Msg, m_Pos);      
        outStream.write((const char *)(m_Msg.message_buffer), m_MsgLen + m_Pos);
        m_Pos = 0;
    }

    void UpdateCrc( struct aws_event_stream_message *message, uint32_t payload_len) {
        uint32_t total_length = 0;
        uint32_t headers_length = aws_event_stream_message_headers_len(message);    
        
        total_length =
            (uint32_t)(AWS_EVENT_STREAM_PRELUDE_LENGTH + headers_length + payload_len + AWS_EVENT_STREAM_TRAILER_LENGTH);
            
        aws_write_u32(total_length, message->message_buffer);
        uint8_t *buffer_offset = message->message_buffer + sizeof(total_length);
        aws_write_u32(headers_length, buffer_offset);   
        buffer_offset += sizeof(headers_length);
    
        uint32_t running_crc = aws_checksums_crc32(message->message_buffer, (int)(buffer_offset - message->message_buffer), 0);

        const uint8_t *message_crc_boundary_start = buffer_offset;
        aws_write_u32(running_crc, buffer_offset);

        buffer_offset = message->message_buffer + AWS_EVENT_STREAM_PRELUDE_LENGTH + headers_length;    
        buffer_offset += payload_len;

        running_crc = aws_checksums_crc32(
            message_crc_boundary_start, (int)(buffer_offset - message_crc_boundary_start), running_crc);
        aws_write_u32(running_crc, buffer_offset);        
    }

    int SendEnd(ostream& outStream) {
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

    int SendCont(ostream& outStream) {  
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

  private:    
    int m_Pos;
    struct aws_event_stream_message m_Msg;
    uint8_t *m_Payload;
    int m_MsgLen;
};

void SelectObjectContent::handleRequest(Poco::Net::HTTPServerRequest &req, Poco::Net::HTTPServerResponse &resp)
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

    string dataPath = "/data";
    string uri = req.getURI();
    size_t pos = uri.find("%2F");
    if(pos != string::npos){
        uri.replace(pos,3, "/");
    }
    string sqlFileName = dataPath + uri.substr(0, uri.find("?"));

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

    int records = 0;
    while ( (rc = sqlite3_step(sqlRes)) == SQLITE_ROW) {      
        int data_count = sqlite3_data_count(sqlRes);
        
        for(int i = 0; i < data_count; i++) {
            const char* text = (const char*)sqlite3_column_text(sqlRes, i);
                
            if(text){
            int len = strlen(text);
            if(DIKE_UNLIKELY(strchr(text,','))){
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
        if(records%1000000 == 0){
        dbb.SendCont(outStream);
        }
        records++;
    }

    dbb.Flush(outStream);

    dbb.SendEnd(outStream);
    outStream.flush();

    sqlite3_finalize(sqlRes);
    sqlite3_close(db);
   
    cout << TimeUtil().Yellow() << TimeUtil().Now() << " Done " << TimeUtil().Reset();
    cout << req.getURI() << endl;
}

