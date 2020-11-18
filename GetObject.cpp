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

void GetObject::handleRequest(HTTPServerRequest &req, HTTPServerResponse &resp)
{    
    string dataPath = "/data";
    string objectName = dataPath + req.getURI();

    Poco::FileInputStream istr(objectName, std::ios::in);

    //ostream& ostr = resp.send();
    //std::streamsize length = Poco::StreamCopier::copyStream(istr, ostr);
    //ostr.flush();
    
    resp.setStatus(HTTPResponse::HTTP_OK);    
    resp.setKeepAlive(true);
    //resp.setContentLength(length);
    //resp.setContentLength(53);
    resp.setChunkedTransferEncoding(true);
    resp.setContentType("text/plain");
    
    /*
    resp.set("X-Xss-Protection", "1; mode=block");
    resp.set("Accept-Ranges", "bytes");
    resp.set("Content-Security-Policy", "block-all-mixed-content");
    resp.set("ETag", "d0d3e7f5b6d9cb76ab54d478fe379a1c");
    resp.set("Server", "dikeCS");
    resp.set("Vary", "Origin");
    resp.set("X-Amz-Request-Id", "1646290C00F3B96E");    
    */
    /*
    172.18.0.2 200 OK
    172.18.0.2 Content-Security-Policy: block-all-mixed-content
    172.18.0.2 Content-Type: text/plain
    172.18.0.2 ETag: "d0d3e7f5b6d9cb76ab54d478fe379a1c"
    172.18.0.2 Server: MinIO/DEVELOPMENT.2020-10-16T18-15-32Z
    172.18.0.2 Vary: Origin
    172.18.0.2 X-Amz-Request-Id: 1646290C00F3B96E
    172.18.0.2 X-Xss-Protection: 1; mode=block
    172.18.0.2 Accept-Ranges: bytes
    172.18.0.2 Content-Length: 49
    172.18.0.2 Last-Modified: Tue, 10 Nov 2020 13:37:28 GMT
    */

    ostream& ostr = resp.send();
    std::streamsize length = Poco::StreamCopier::copyStream(istr, ostr);
    
    cout << TimeUtil().Yellow() << TimeUtil().Now() << " Done " << TimeUtil().Reset();
    cout << req.getURI() << endl;
    req.write(cout);
    resp.write(cout);   
}