#ifndef S3_HANDLERS_HPP
#define S3_HANDLERS_HPP

#include <Poco/Net/HTTPRequestHandler.h>
#include <Poco/Net/HTTPServerRequest.h>
#include <Poco/Net/HTTPServerResponse.h>

class ListObjectsV2 : public Poco::Net::HTTPRequestHandler {
public:  
   virtual void handleRequest(Poco::Net::HTTPServerRequest &req, Poco::Net::HTTPServerResponse &resp);
   virtual ~ListObjectsV2() {};
};

class SelectObjectContent : public Poco::Net::HTTPRequestHandler {
public:  
   virtual void handleRequest(Poco::Net::HTTPServerRequest &req, Poco::Net::HTTPServerResponse &resp);
   virtual ~SelectObjectContent() {};
};

class PutObject : public Poco::Net::HTTPRequestHandler {
public:  
   virtual void handleRequest(Poco::Net::HTTPServerRequest &req, Poco::Net::HTTPServerResponse &resp);
   virtual ~PutObject() {};
};

#endif /* S3_HANDLERS_HPP */