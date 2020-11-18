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

#include <iostream>
#include <sstream>
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
void ListObjectsV2::handleRequest(HTTPServerRequest &req, HTTPServerResponse &resp)
{
    stringstream xmlstream;
    Poco::XML::XMLString element;
    Poco::XML::XMLString value;
    Poco::XML::XMLWriter writer(xmlstream, 0);
    map<string, string> args;
    char * uri;
    int pos = 0;
    int next_pos = 0;
    string key;    

    string dataPath = "/data";

    char * env = getenv("DIKECS_DATA_PATH");
    if(env != NULL){
        dataPath = string(env);
        cout << "dataPath set to " << dataPath << endl;
    }    

    //cout << req.getURI() << endl;
    //  /tpch-test/?list-type=2&max-keys=1024&fetch-owner=false

    next_pos = req.getURI().find("/", pos);
    pos = next_pos + 1;
    next_pos = req.getURI().find("/", pos);
    args["Name"] = req.getURI().substr(pos, next_pos - pos);
    pos = next_pos + 1;
    
    next_pos = req.getURI().find("?", pos);
    //args["Name"] = req.getURI().substr(pos, next_pos - pos);
    pos = next_pos + 1;    
    
    do {
        next_pos = req.getURI().find("=", pos);
        if(next_pos == string::npos){
            break;
        }
        key = req.getURI().substr(pos, next_pos - pos);
        pos = next_pos + 1;

        next_pos = req.getURI().find("&", pos);
        if(next_pos == string::npos){
            value = req.getURI().substr(pos);
            args[key] = value;
            //cout << key << " = " << value << endl;
            break;
        }
         
        value = req.getURI().substr(pos, next_pos - pos);
        pos = next_pos + 1;
        args[key] = value;
        //cout << key << " = " << value << endl;
    } while(pos < req.getURI().length());
    
    struct dirent *pDirent;
    DIR *pDir;
    string dir_path = dataPath + "/" + args["Name"] + "/";
    struct content {
        char name[FNAME_MAX];
        size_t size;
        char lastModified[TSTAMP_MAX];
    };
        
    struct content * contents = (struct content *)malloc(sizeof(struct content) * stoi(args["max-keys"]));
    if(contents == NULL) {
        cout << "Failed to allocate memory" << endl;
    }

    int KeyCount = 0;    
    
    pDir = opendir(dir_path.c_str());    
    if (pDir == NULL) {
        cout << "Can't open " << dir_path.c_str() << endl;
    }

    while ((pDirent = readdir(pDir)) != NULL) {
        if(pDirent->d_type != DT_REG) {
            continue;
        }        
        string name = pDirent->d_name;

        if(name.rfind(args["prefix"],0) != 0){
            continue;
        }

        struct stat s;                
        string p = dir_path + name;        
        if( stat( p.c_str() , &s) == 0 ) {
            if( s.st_mode & S_IFREG ) {                
                strcpy(contents[KeyCount].name, name.c_str());                                
                contents[KeyCount].size = s.st_size;    
                
                struct tm tms;
                localtime_r(&s.st_mtim.tv_sec, &tms);                
                // 2020-10-27T17:44:12.056Z
                std::strftime(contents[KeyCount].lastModified, TSTAMP_MAX, "%Y-%m-%dT%H:%M:%S.056Z", &tms);
                
                /*
                cout << contents[KeyCount].name << endl;
                cout << contents[KeyCount].lastModified << endl;
                cout << to_string(contents[KeyCount].size) << endl;
                */
                
                KeyCount++;
            }
        }
    }
    closedir (pDir);

    writer.startDocument();
    
    writer.startElement("", "", XMLString("ListBucketResult"));
    writer.characters(XMLString("xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\""));
    
        writer.startElement("", "", XMLString("Name"));
        writer.characters(args["Name"]);    
        writer.endElement("", "", XMLString("Name"));

        writer.startElement("", "", XMLString("Prefix"));
        writer.characters(args["prefix"]);
        writer.endElement("", "", XMLString("Prefix"));

        writer.startElement("", "", XMLString("KeyCount"));
        writer.characters(to_string(KeyCount));    
        writer.endElement("", "", XMLString("KeyCount"));

        writer.startElement("", "", XMLString("MaxKeys"));
        writer.characters(args["max-keys"]);    
        writer.endElement("", "", XMLString("MaxKeys"));

        writer.startElement("", "", XMLString("Delimiter"));        
        writer.endElement("", "", XMLString("Delimiter"));

        writer.startElement("", "", XMLString("IsTruncated"));
        writer.characters(XMLString("false"));    
        writer.endElement("", "", XMLString("IsTruncated"));

        // Loop starts here
        for(int i = 0; i < KeyCount; i++) {
            /*
            cout << contents[i].name << endl;
            cout << contents[i].lastModified << endl;
            cout << to_string(contents[i].size) << endl;
            */
            writer.startElement("", "", XMLString("Contents"));
                writer.startElement("", "", XMLString("Key"));
                writer.characters(XMLString(contents[i].name));
                writer.endElement("", "", XMLString("Key"));

                writer.startElement("", "", XMLString("LastModified"));
                writer.characters(XMLString(contents[i].lastModified));                
                //writer.characters(XMLString("2020-10-27T17:44:12.056Z"));                
                writer.endElement("", "", XMLString("LastModified"));

                writer.startElement("", "", XMLString("ETag"));
                writer.characters(XMLString("&#34;00000000000000000000000000000000-1&#34;"));    
                writer.endElement("", "", XMLString("ETag"));

                writer.startElement("", "", XMLString("Size"));
                writer.characters(to_string(contents[i].size));    
                writer.endElement("", "", XMLString("Size"));

                if(args["fetch-owner"] == "true")
                {
                    writer.startElement("", "", XMLString("Owner"));

                        writer.startElement("", "", XMLString("ID"));            
                        writer.endElement("", "", XMLString("ID"));

                        writer.startElement("", "", XMLString("DisplayName"));              
                        writer.endElement("", "", XMLString("DisplayName"));

                    writer.endElement("", "", XMLString("Owner"));
                }
                writer.startElement("", "", XMLString("StorageClass"));
                writer.characters(XMLString("STANDARD"));    
                writer.endElement("", "", XMLString("StorageClass"));

            writer.endElement("", "", XMLString("Contents"));
        }

    writer.endElement("", "", XMLString("ListBucketResult"));

    writer.endDocument();
    //cout << xmlstream.str() << endl;

    resp.setStatus(HTTPResponse::HTTP_OK);    
    resp.setContentType("application/xml");    

    ostream& outStream = resp.send();
    outStream.write(xmlstream.str().c_str(), xmlstream.str().length());
    outStream.flush();
    free(contents);

    cout << TimeUtil().Yellow() << TimeUtil().Now() << " Done " << TimeUtil().Reset();
    cout << req.getURI() << endl;
}