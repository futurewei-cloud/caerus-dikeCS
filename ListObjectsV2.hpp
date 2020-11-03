#ifndef LIST_OBJECTS_V2_HPP
#define LIST_OBJECTS_V2_HPP

#include <Poco/Net/HTTPRequestHandler.h>
#include <Poco/Net/HTTPServerRequest.h>
#include <Poco/Net/HTTPServerResponse.h>

class ListObjectsV2 : public Poco::Net::HTTPRequestHandler {
public:  
   virtual void handleRequest(Poco::Net::HTTPServerRequest &req, Poco::Net::HTTPServerResponse &resp);
   virtual ~ListObjectsV2() {};
};

#endif /* LIST_OBJECTS_V2_HPP */