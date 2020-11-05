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

#include <iostream>

#include "SelectObjectContent.hpp"
#include "ListObjectsV2.hpp"
#include "TimeUtil.hpp"

using namespace Poco::Net;
using namespace Poco::Util;
using namespace std;

class DikeRequestHandlerFactory : public HTTPRequestHandlerFactory {
public:
  virtual HTTPRequestHandler* createRequestHandler(const HTTPServerRequest & req) {
    cout << TimeUtil().Yellow() << TimeUtil().Now() << " Start " << TimeUtil().Reset();
    cout << req.getURI() << endl;
    if(string::npos != req.getURI().find("list-type=2&")){
      return new ListObjectsV2;
    }
    if(string::npos != req.getURI().find("select&select-type=2")){
      return new SelectObjectContent;
    }
    return NULL;
  }
};

class DikeServerApp : public ServerApplication
{
protected:
  int main(const vector<string> &)
  {
    HTTPServer s(new DikeRequestHandlerFactory, ServerSocket(9000), new HTTPServerParams);

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
  DikeServerApp app;
  return app.run(argc, argv);
}