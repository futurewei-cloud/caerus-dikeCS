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
#include <Poco/XML/XMLWriter.h>
#include "Poco/StreamCopier.h"
#include "Poco/Timespan.h"
#include "Poco/FileStream.h"


#include <iostream>
#include <sstream>
#include <fstream>
#include <map>

#include <dirent.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>

#include "TimeUtil.hpp"
#include "S3Handlers.hpp"

using namespace Poco::Net;
using namespace Poco::Util;
using namespace Poco::XML;
using namespace std;

#define FNAME_MAX 80
#define TSTAMP_MAX 40

void PutObject::handleRequest(HTTPServerRequest &req, HTTPServerResponse &resp)
{
    istream& istr = req.stream();
    string dataPath = "/data";
    string objectName = dataPath + req.getURI();

    Poco::FileOutputStream ostr(objectName, std::ios::trunc);

    //Poco::StreamCopier::copyStream(istr, ostr);
    string line;
    while(std::getline(istr, line)){
        if(string::npos == line.find(";chunk-signature=")){
            ostr << line << "\n";
        }
    }
    ostr.flush();
    ostr.close();

    resp.setStatus(HTTPResponse::HTTP_OK);
    resp.setContentLength(0);
    resp.setKeepAlive(true);
    
    /*
    resp.set("X-Xss-Protection", "1; mode=block");
    resp.set("X-Xss-Protection", "1; mode=block");
    resp.set("Accept-Ranges", "bytes");
    resp.set("Content-Security-Policy", "block-all-mixed-content");
    resp.set("ETag", "593373f70ed20e8e18ea9f2475979845");
    resp.set("Server", "dikeCS");
    resp.set("Vary", "Origin");
    resp.set("X-Amz-Request-Id", "1645E415E0040DA6");
    */

    ostream& outStream = resp.send();
    outStream.flush();

    cout << TimeUtil().Yellow() << TimeUtil().Now() << " Done " << TimeUtil().Reset();
    cout << req.getURI() << endl;
    //resp.write(cout);   
}