// This file uses the Boost library example source code (Copyright (c) 2016-2019 Vinnie Falco (vinnie dot falco at gmail dot com))
//
// Distributed under the Boost Software License, Version 1.0. (See accompanying
// file LICENSE_1_0.txt or copy at http://www.boost.org/LICENSE_1_0.txt)
//
// Official repository: https://github.com/boostorg/beast


#include <activemq/cmsutil/CmsAccessor.h>
#include <decaf/util/concurrent/CountDownLatch.h>
#include <activemq/core/ActiveMQConnectionFactory.h>
#include <activemq/util/Config.h>
#include <activemq/library/ActiveMQCPP.h>
#include <cms/Connection.h>
#include <cms/Session.h>
#include <cms/TextMessage.h>
#include <cms/QueueBrowser.h>
#include <activemq/core/ActiveMQQueueBrowser.h>
#include <activemq/core/ActiveMQMessageAudit.h>
#include <activemq/core/PrefetchPolicy.h>

#include <cstdio>
#include <memory>
#include <utility>

#include <iostream>
#include <string>
#include <boost/asio.hpp>
#include <boost/beast/core.hpp>
#include <boost/beast/http.hpp>
#include <boost/beast/version.hpp>

#include <miniocpp/client.h>
#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <thread>
#include <chrono>

#include <boost/uuid/random_generator.hpp>
#include <boost/uuid/uuid_io.hpp>
#include <boost/lexical_cast.hpp>
#include <boost/json.hpp>

using namespace activemq;
using namespace activemq::core;
using namespace decaf;
using namespace decaf::lang;
using namespace decaf::util;
using namespace decaf::util::concurrent;
using namespace cms;
using namespace std;

////////////////////////////////////////////////////////////////////////////////
class QueueProducer {
private:

    Connection* connection;
    Session* session;
    Destination* destination;
    MessageProducer* producer;
    MessageProducer* snapshotProducer;
    bool useTopic;
    bool clientAck;
    string brokerURI;
    string destURI;

    // MessageConsumer* consumer;

public:
    QueueProducer( const QueueProducer& ) = delete;
    QueueProducer& operator= ( const QueueProducer& ) = delete;

    QueueProducer( string  brokerURI, string  destURI, const bool useTopic = false, const bool clientAck = false ) :
        connection(nullptr),
        session(nullptr),
        destination(nullptr),
        producer(nullptr),
        useTopic(useTopic),
        clientAck(clientAck),
        brokerURI(std::move(brokerURI)),
        destURI(std::move(destURI))
        // consumer(nullptr)
    {

        const unique_ptr<ActiveMQConnectionFactory> connectionFactory(
                new ActiveMQConnectionFactory( this->brokerURI ) );

        connectionFactory->setCheckForDuplicates(true);

        connectionFactory->getPrefetchPolicy()->setQueuePrefetch(1);

        try{
            connection = connectionFactory->createConnection();
            connection->start();
        } catch( CMSException& e ) {
            e.printStackTrace();
            throw;
        }

        if( clientAck ) {
            session = connection->createSession( Session::CLIENT_ACKNOWLEDGE );
        } else {
            session = connection->createSession( Session::AUTO_ACKNOWLEDGE );
        }

        if( useTopic ) {
            destination = session->createTopic( this->destURI );
        } else {
            destination = session->createQueue( this->destURI );
        }

        producer = session->createProducer( destination );
        producer->setDeliveryMode( DeliveryMode::PERSISTENT );

        snapshotProducer = session->createProducer( session->createQueue( "SnapshotQueue" ) );
        snapshotProducer->setDeliveryMode( DeliveryMode::PERSISTENT );

        // consumer = session->createConsumer( destination );
    }

    virtual ~QueueProducer(){
        cleanup();
    }

    void close() const
    {
        this->cleanup();
    }

    void enqueue(const char* path, const char* created_at, const char* name, const char* description, const char* type, const int priority = 4, const bool toSnapshot = false, const bool expireSnapshot = false) {
        try {
            cout << "Try to enqueue" << endl;
            TextMessage* message = session->createTextMessage(name);
            message->setStringProperty("created_at", created_at);
            message->setStringProperty("description", description);
            message->setStringProperty("path", path);
            message->setStringProperty("type", type);

            if (!toSnapshot) {
                producer->setPriority(priority);
                producer->send( message );
                cout << "Sent" << endl;
            } else
            {
                std::filesystem::path stemName = name;
                while (stemName.has_extension()) {
                    stemName = stemName.stem();
                }
                message->setStringProperty("_AMQ_LVQ_NAME", stemName);
                snapshotProducer->setPriority(priority);
                cout << "Expire the snapshot? " << (expireSnapshot ? "yes" : "no") << endl;

                if (expireSnapshot) {
                    // message->setCMSExpiration(0);
                    snapshotProducer->setTimeToLive(1);
                } else {
                    snapshotProducer->setTimeToLive(240000);
                    // message->setCMSExpiration(240000);
                }
                snapshotProducer->send( message );
            }
            cout << "Message sent" << endl;
            delete message;

        } catch ( CMSException& e ) {
            e.printStackTrace();
        }
    }


    /*[[nodiscard]] const TextMessage* dequeue() const
    {
        string text;
        const auto* textMessage = dynamic_cast< const TextMessage* >( consumer->receive() );


        /*if( clientAck ) {
            textMessage->acknowledge();
        }#1#

        return textMessage;
    }*/

    void session_recover()
    {
        session->recover();
    }

    /*bool hasMessage()
    {
        return session->createBrowser(session->createQueue(this->destURI))->getEnumeration()->hasMoreMessages();
    }*/

private:

    void cleanup() const
    {


        try {
            if( producer != nullptr) {
                producer->close();
            }
        } catch ( CMSException& e ) {
            e.printStackTrace();
        }

        /*try {
            if( consumer != nullptr) {
                consumer->close();
            }
        } catch ( CMSException& e ) {
            e.printStackTrace();
        }*/

        try {
            if( session != nullptr) {
                session->close();
            }
        } catch ( CMSException& e ) {
            e.printStackTrace();
        }

        try {
            if( connection != nullptr) {
                connection->close();
            }
        } catch ( CMSException& e ) {
            e.printStackTrace();
        }

        delete destination;
        delete producer;
        // delete consumer;
        delete session;
        delete connection;
    }
};

// const string brokerURI = "tcp://127.0.0.1:61616";
const string brokerURI = getenv("WORKQUEUE_URL");
const string destURI = "WorkQueue";

boost::uuids::random_generator generator;


boost::beast::string_view
mime_type(boost::beast::string_view path)
{
    using boost::beast::iequals;
    auto const ext = [&path]
    {
        auto const pos = path.rfind(".");
        if(pos == boost::beast::string_view::npos)
            return boost::beast::string_view{};
        return path.substr(pos);
    }();
    if(iequals(ext, ".htm"))  return "text/html";
    if(iequals(ext, ".html")) return "text/html";
    if(iequals(ext, ".php"))  return "text/html";
    if(iequals(ext, ".css"))  return "text/css";
    if(iequals(ext, ".txt"))  return "text/plain";
    if(iequals(ext, ".js"))   return "application/javascript";
    if(iequals(ext, ".json")) return "application/json";
    if(iequals(ext, ".xml"))  return "application/xml";
    if(iequals(ext, ".swf"))  return "application/x-shockwave-flash";
    if(iequals(ext, ".flv"))  return "video/x-flv";
    if(iequals(ext, ".png"))  return "image/png";
    if(iequals(ext, ".jpe"))  return "image/jpeg";
    if(iequals(ext, ".jpeg")) return "image/jpeg";
    if(iequals(ext, ".jpg"))  return "image/jpeg";
    if(iequals(ext, ".gif"))  return "image/gif";
    if(iequals(ext, ".bmp"))  return "image/bmp";
    if(iequals(ext, ".ico"))  return "image/vnd.microsoft.icon";
    if(iequals(ext, ".tiff")) return "image/tiff";
    if(iequals(ext, ".tif"))  return "image/tiff";
    if(iequals(ext, ".svg"))  return "image/svg+xml";
    if(iequals(ext, ".svgz")) return "image/svg+xml";
    if(iequals(ext, ".zip"))  return "application/zip";
    if(iequals(ext, ".gz"))   return "application/gzip";
    if(iequals(ext, ".tar"))  return "application/x-tar";
    if(iequals(ext, ".tgz"))  return "application/gzip";

    return "application/octet-stream";
}

// Append an HTTP rel-path to a local filesystem path.
// The returned path is normalized for the platform.
std::string
path_cat(
    boost::beast::string_view base,
    boost::beast::string_view path)
{
    if (base.empty())
        return std::string(path);
    std::string result(base);
#ifdef BOOST_MSVC
    char constexpr path_separator = '\\';
    if(result.back() == path_separator)
        result.resize(result.size() - 1);
    result.append(path.data(), path.size());
    for(auto& c : result)
        if(c == '/')
            c = path_separator;
#else
    char constexpr path_separator = '/';
    if(result.back() == path_separator)
        result.resize(result.size() - 1);
    result.append(path.data(), path.size());
#endif
    return result;
}


template<typename Func>
auto retry_minio_op(Func&& op, int max_retries = 5) {
    int attempt = 0;
    while (true) {
        auto resp = op();
        if (resp) return resp;

        // Check for retryable error
        // MinIO/S3 returns "SlowDown" code for rate limiting (usually 503 or 429)
        if (resp.code == "SlowDown" || resp.status_code == 429 || resp.status_code == 503) {
            if (++attempt > max_retries) return resp;

            int sleep_ms = 500 * (1 << attempt); // Exponential backoff: 1000ms, 2000ms, 4000ms, 8000ms, 16000ms
            std::cerr << "Minio SlowDown detected, retrying in " << sleep_ms << "ms (attempt " << attempt << "/" << max_retries << ")" << std::endl;
            std::this_thread::sleep_for(std::chrono::milliseconds(sleep_ms));
            continue;
        }
        return resp;
    }
}

//Work with unique UUID
std::string upload_work(const std::shared_ptr<QueueProducer>& broker, const std::string& benchName) {
    cout << "Try to upload" << endl;
    minio::s3::BaseUrl base_url(getenv("WORKSERVER_URL"), false);
    minio::creds::StaticProvider provider(
        getenv("WORKSERVER_ACCESSKEY"), getenv("WORKSERVER_SECRETKEY"));
    minio::s3::Client client(base_url, &provider);

    minio::s3::UploadObjectArgs args;

    // Determine which bench file to upload
    std::string name = benchName.empty() ? "benchMEM" : benchName;
    // ensure it ends with .tpr
    if (name.size() < 4 || name.substr(name.size() - 4) != ".tpr")
        name += ".tpr";

    filesystem::path work_path = std::filesystem::path("/home/ubuntu/WorkManager/gromacs_topology") / name;
    filesystem::path object_name = boost::lexical_cast<std::string>(generator()) + "_" + work_path.filename().string();

    args.bucket = "gromacs-topology";
    args.object = object_name.string();
    args.filename = work_path;
    cout << "Begin upload " << work_path << endl;
    if (minio::s3::UploadObjectResponse resp = retry_minio_op([&]{ return client.UploadObject(args); }); !resp) {
        cerr << "Cannot upload: " << resp.message;
        exit(EXIT_FAILURE);
    }
    cout << "Finished" <<endl;
    broker->enqueue(object_name.c_str(), std::format("{:%F %T}", std::chrono::system_clock::now()).c_str(), object_name.c_str(), string(object_name.string() + " raw work").c_str(), "topology", 5, false);
    return object_name.string();
}

template <class Body, class Allocator>
boost::beast::http::message_generator
handle_request(
    boost::beast::string_view doc_root,
    boost::beast::http::request<Body, boost::beast::http::basic_fields<Allocator>>&& req,
    const std::shared_ptr<QueueProducer>& broker
    )
{
    auto const bad_request = [&req](boost::beast::string_view why)
    {
        boost::beast::http::response<boost::beast::http::string_body> res{boost::beast::http::status::bad_request, req.version()};
        res.set(boost::beast::http::field::server, BOOST_BEAST_VERSION_STRING);
        res.set(boost::beast::http::field::content_type, "text/html");
        res.keep_alive(req.keep_alive());
        res.body() = std::string(why);
        res.prepare_payload();
        return res;
    };
    auto const not_found = [&req](boost::beast::string_view target)
    {
        boost::beast::http::response<boost::beast::http::string_body> res{boost::beast::http::status::not_found, req.version()};
        res.set(boost::beast::http::field::server, BOOST_BEAST_VERSION_STRING);
        res.set(boost::beast::http::field::content_type, "text/html");
        res.keep_alive(req.keep_alive());
        res.body() = "The resource '" + std::string(target) + "' was not found.";
        res.prepare_payload();
        return res;
    };
    auto const server_error = [&req](boost::beast::string_view what)
    {
        boost::beast::http::response<boost::beast::http::string_body> res{boost::beast::http::status::internal_server_error, req.version()};
        res.set(boost::beast::http::field::server, BOOST_BEAST_VERSION_STRING);
        res.set(boost::beast::http::field::content_type, "text/html");
        res.keep_alive(req.keep_alive());
        res.body() = "An error occurred: '" + std::string(what) + "'";
        res.prepare_payload();
        return res;
    };

    if( req.method() != boost::beast::http::verb::get &&
        req.method() != boost::beast::http::verb::head &&
        req.method() != boost::beast::http::verb::post)
        return bad_request("Unknown HTTP-method");

    // Request path must be absolute and not contain "..".
    if( req.target().empty() ||
        req.target()[0] != '/' ||
        req.target().find("..") != boost::beast::string_view::npos)
        return bad_request("Illegal request-target");

    // Build the path to the requested file

    if (req.target().find("/upload_work") == 0)
    {
        cout << "Uploading Work" << endl;

        // Try to get bench name from query string
        std::string benchName;
        auto target = req.target();
        auto qpos = target.find('?');
        if (qpos != boost::beast::string_view::npos) {
            auto query = target.substr(qpos + 1);
            std::string q(query);
            auto pos = q.find("name=");
            if (pos != std::string::npos) {
                benchName = q.substr(pos + 5);
                auto amp = benchName.find('&');
                if (amp != std::string::npos) benchName.resize(amp);
            }
        }

        // Also accept JSON POST body with { "Name": "benchPEP" }
        std::string body_content;
        if constexpr (std::is_same_v<Body, boost::beast::http::string_body>) {
            body_content = req.body();
        }
        if (!body_content.empty()) {
            try {
                auto jv = boost::json::parse(body_content);
                boost::json::object obj = jv.as_object();
                if (obj.contains("Name")) benchName = obj["Name"].as_string();
                else if (obj.contains("Bench")) benchName = obj["Bench"].as_string();
            } catch(...) {
                // ignore parse errors and fall back to query/default
            }
        }

        if (benchName.empty()) benchName = "benchMEM"; // default

        std::string uploaded = upload_work(broker, benchName);
        boost::beast::http::response<boost::beast::http::string_body> res{boost::beast::http::status::ok, req.version()};

        res.set(boost::beast::http::field::server, BOOST_BEAST_VERSION_STRING);
        res.set(boost::beast::http::field::content_type, "text/plain");
        res.body() = std::string("[Server] Work uploaded: ") + uploaded;
        res.prepare_payload();
        res.keep_alive(req.keep_alive());
        return res;
    }

    if(req.target() == "/get_work")
    {
        std::string body_content;
        if constexpr (std::is_same_v<Body, boost::beast::http::string_body>) {
            body_content = req.body();
        } else {
            std::cout << "Received a file body, skipping JSON parse." << std::endl;
        }
        auto jv = boost::json::parse(body_content);
        boost::json::object mesBody = jv.as_object();

        // broker.enqueue("benchMEM/benchMEM.tpr", std::format("{:%F %T}", std::chrono::system_clock::now()).c_str(), "benchMEM.tpr", "benchmark topology", "topology", 4);

        std::cout << "--- Message Received ---" << std::endl;
        std::cout << "Name:  " <<           mesBody["Name"]        << std::endl;
        std::cout << "Path:        " <<     mesBody["Path"]         << std::endl;
        std::cout << "Timestamp: " <<       mesBody["Timestamp"]    << std::endl;
        std::cout << "Description: " <<     mesBody["Description"]  << std::endl;
        std::cout << "Type: " <<            mesBody["Type"]         << std::endl;
        std::cout << "------------------------" << std::endl;

        string save_path = "/tmp/" + std::string(mesBody.at("Name").as_string());

        if (mesBody["Type"]  == "topology")
        {
            minio::s3::BaseUrl base_url(getenv("WORKSERVER_URL"), false);
            minio::creds::StaticProvider provider(
                getenv("WORKSERVER_ACCESSKEY"), getenv("WORKSERVER_SECRETKEY"));
            minio::s3::Client client(base_url, &provider);

            minio::s3::DownloadObjectArgs args;

            args.bucket = "gromacs-topology";
            args.object = mesBody["Path"].as_string();
            // args.filename = path_cat(string(getenv("GROMACS_TOPOLOGY_DIR"))+ "/", text_message->getText());
            args.filename = save_path;
            args.overwrite = true;
            if (minio::s3::DownloadObjectResponse resp = retry_minio_op([&]{ return client.DownloadObject(args); }); !resp)
            {
                return server_error("Unable to download object" + resp.Error().String());
            }
        }
        else if (mesBody["Type"]  == "snapshot")
        {
            minio::s3::BaseUrl base_url(getenv("SNAPSHOTSERVER_URL"), false);
            minio::creds::StaticProvider provider(
                getenv("SNAPSHOTSERVER_ACCESSKEY"), getenv("SNAPSHOTSERVER_SECRETKEY"));
            minio::s3::Client client(base_url, &provider);

            minio::s3::DownloadObjectArgs args;

            args.bucket = "snapshot";
            args.object = mesBody["Path"].as_string()  ;
            // args.filename = path_cat(string(getenv("GROMACS_TOPOLOGY_DIR"))+ "/", text_message->getText());
            args.filename = save_path;
            args.overwrite = true;
            if (minio::s3::DownloadObjectResponse resp = retry_minio_op([&]{ return client.DownloadObject(args); }); !resp)
            {
                return server_error("Unable to download object" + resp.Error().String());
            }

            minio::s3::RemoveObjectArgs removeArgs;
            removeArgs.bucket = "snapshot";
            removeArgs.object = mesBody["Path"].as_string();
            if (minio::s3::RemoveObjectResponse resp = retry_minio_op([&]{ return client.RemoveObject(removeArgs); }); !resp)
            {
                return server_error("Unable to remove object" + resp.Error().String());
            }
        }
        else
        {
            return not_found(req.target());
        }

        boost::beast::error_code ec;
        boost::beast::http::file_body::value_type body;
        body.open(save_path.c_str() , boost::beast::file_mode::read, ec);

        // Handle the case where the file doesn't exist
        if(ec == boost::beast::errc::no_such_file_or_directory)
            return not_found(req.target());
        if(ec)
            return server_error(ec.message());

        auto const size = body.size();

        if(req.method() == boost::beast::http::verb::head)
        {
            boost::beast::http::response<boost::beast::http::empty_body> res{boost::beast::http::status::ok, req.version()};
            res.set(boost::beast::http::field::server, BOOST_BEAST_VERSION_STRING);
            res.set(boost::beast::http::field::content_type, mime_type(save_path));
            res.set("X-File-Type", mesBody["Type"]  == "snapshot" ? "snapshot" : "topology");
            res.content_length(size);
            res.keep_alive(req.keep_alive());

            // text_message->acknowledge();
            return res;
        }

        // Respond to GET request
        boost::beast::http::response<boost::beast::http::file_body> res{
            std::piecewise_construct,
            std::make_tuple(std::move(body)),
            std::make_tuple(boost::beast::http::status::ok, req.version())};
        res.set(boost::beast::http::field::server, BOOST_BEAST_VERSION_STRING);
        res.set(boost::beast::http::field::content_type, mime_type(save_path));
        res.set(boost::beast::http::field::content_disposition, "attachment; filename=" + string(mesBody["Name"].as_string()));
        res.set("X-File-Type", mesBody["Type"]  == "snapshot" ? "snapshot" : "topology");
        res.content_length(size);
        res.keep_alive(req.keep_alive());

        // text_message->acknowledge();
        return res;
    }
    if (req.target() == "/send_snapshot")
    {
        if (req.method() != boost::beast::http::verb::post)
            return bad_request("Unknown HTTP-method");

        boost::beast::http::response<boost::beast::http::string_body> res{boost::beast::http::status::ok, req.version()};
        res.set(boost::beast::http::field::server, BOOST_BEAST_VERSION_STRING);
        res.body() = "[Server] Snapshot uploaded successfully";
        res.prepare_payload();
        res.keep_alive(req.keep_alive());
        return res;
    }
    if (req.target() == "/send_result")
    {
        if (req.method() != boost::beast::http::verb::post)
            return bad_request("Unknown HTTP-method");

        boost::beast::http::response<boost::beast::http::string_body> res{boost::beast::http::status::ok, req.version()};
        res.set(boost::beast::http::field::server, BOOST_BEAST_VERSION_STRING);
        res.body() = "[Server] Result uploaded successfully";
        res.prepare_payload();
        res.keep_alive(req.keep_alive());
        return res;
    }
    return not_found(req.target());
}

// Report a failure
void
fail(boost::beast::error_code ec, char const* what)
{
    std::cerr << what << ": " << ec.message() << "\n";
}

// Handles an HTTP server connection
void
do_session(
    boost::asio::ip::tcp::socket& socket,
    std::shared_ptr<std::string const> const& doc_root,
    const std::shared_ptr<QueueProducer>& broker)
{
    boost::beast::error_code ec;
    boost::beast::multi_buffer buffer;

    for(;;)
    {
        // 1. Read ONLY the header first
        boost::beast::http::request_parser<boost::beast::http::empty_body> req_header_parser;
        req_header_parser.body_limit(boost::none);
        boost::beast::http::read_header(socket, buffer, req_header_parser, ec);

        if(ec == boost::beast::http::error::end_of_stream) break;
        if(ec) return fail(ec, "read header");

        bool keep_alive = false;

        // 2. Route based on the URL target

        if (const auto target = req_header_parser.get().target(); target == "/send_snapshot")
        {
            boost::beast::http::request_parser<boost::beast::http::file_body> file_parser(std::move(req_header_parser));
            file_parser.body_limit(boost::none);
            boost::beast::error_code file_ec;

            string file_name = string(file_parser.get()["X-Filename"]);
            std::filesystem::path store_path = "/tmp/" + file_name;

            file_parser.get().body().open(store_path.c_str(), boost::beast::file_mode::write, file_ec);

            if(file_ec) {
                fail(file_ec, "open file on disk");
                boost::beast::http::response<boost::beast::http::string_body> res{boost::beast::http::status::internal_server_error, file_parser.get().version()};
                res.body() = "Failed to open local file for writing: " + file_ec.message();
                res.prepare_payload();
                boost::beast::http::write(socket, res, ec);
                return;
            }

            boost::beast::http::read(socket, buffer, file_parser, ec);
            if(ec) {
                fail(ec, "read file body");
                return;
            }

            minio::s3::BaseUrl base_url(getenv("SNAPSHOTSERVER_URL"), false);
            minio::creds::StaticProvider provider(
                getenv("SNAPSHOTSERVER_ACCESSKEY"), getenv("SNAPSHOTSERVER_SECRETKEY"));
            minio::s3::Client client(base_url, &provider);

            string object_created_at = std::format("{:%F %T}", std::chrono::system_clock::now());
            string object_path = string(file_parser.get()["X-Device-ID"]) + "/" + file_name;

            minio::s3::UploadObjectArgs args;
            args.bucket = "snapshot";
            args.object = object_path;
            args.filename = store_path.string();

            if (const minio::s3::UploadObjectResponse resp = retry_minio_op([&]{ return client.UploadObject(args); }); !resp)
            {
                std::cerr << "Minio Upload Error: " << resp.Error().String() << std::endl;
                boost::beast::http::response<boost::beast::http::string_body> res{boost::beast::http::status::internal_server_error, file_parser.get().version()};
                res.body() = "Minio Upload Error: " + resp.Error().String();
                res.prepare_payload();
                boost::beast::http::write(socket, res, ec);
                return;
            }

            if (string(file_parser.get()["X-AutoCheckpoint"]) == "true")
            {
                broker->enqueue(object_path.c_str(), object_created_at.c_str(), file_name.c_str(), (file_name + " snapshot").c_str(), "snapshot", 6, true, false); //expiresnapshot == expire to move it as work queue immediately
            }
            else
            {
                broker->enqueue(object_path.c_str(), object_created_at.c_str(), file_name.c_str(), (file_name + " snapshot").c_str(), "snapshot", 6, true, true); //expiresnapshot == expire to move it as work queue immediately
            }

            boost::beast::http::message_generator msg = handle_request(*doc_root, file_parser.release(), broker);
            keep_alive = msg.keep_alive();
            boost::beast::write(socket, std::move(msg), ec);
        }

        else if (target == "/send_result")
        {
            boost::beast::http::request_parser<boost::beast::http::file_body> file_parser(std::move(req_header_parser));
            file_parser.body_limit(boost::none);
            boost::beast::error_code file_ec;

            auto file_name = string(file_parser.get()["X-Filename"]);
            std::filesystem::path store_path = "/tmp/" + file_name;

            file_parser.get().body().open(store_path.c_str(), boost::beast::file_mode::write, file_ec);

            if(file_ec) {
                fail(file_ec, "open file on disk");
                boost::beast::http::response<boost::beast::http::string_body> res{boost::beast::http::status::internal_server_error, file_parser.get().version()};
                res.body() = "Failed to open local file for writing: " + file_ec.message();
                res.prepare_payload();
                boost::beast::http::write(socket, res, ec);
                return;
            }

            boost::beast::http::read(socket, buffer, file_parser, ec);
            if(ec) {
                fail(ec, "read file body");
                return;
            }

            minio::s3::BaseUrl base_url(getenv("RESULTSERVER_URL"), false);
            minio::creds::StaticProvider provider(
                getenv("RESULTSERVER_ACCESSKEY"), getenv("RESULTSERVER_SECRETKEY"));
            minio::s3::Client client(base_url, &provider);

            string object_created_at = std::format("{:%F %T}", std::chrono::system_clock::now());
            string object_path = string(file_parser.get()["X-Device-ID"]) + "/" + file_name;

            // Check if object already exists and reject duplicates
            minio::s3::StatObjectArgs statArgs;
            statArgs.bucket = "results";
            statArgs.object = object_path;
            if (const minio::s3::StatObjectResponse statResp = client.StatObject(statArgs); statResp)
            {
                // Object exists -> reject as duplicate
                boost::beast::http::response<boost::beast::http::string_body> res{boost::beast::http::status::conflict, file_parser.get().version()};
                res.set(boost::beast::http::field::server, BOOST_BEAST_VERSION_STRING);
                res.body() = "Duplicate result";
                res.prepare_payload();
                res.keep_alive(file_parser.get().keep_alive());

                // remove temporary stored file
                std::error_code remove_ec;
                std::filesystem::remove(store_path, remove_ec);

                boost::beast::http::write(socket, res, ec);
                keep_alive = res.keep_alive();
            }
            else
            {
                minio::s3::UploadObjectArgs args;
                args.bucket = "results";
                args.object = object_path;
                args.filename = store_path.string();

                if (const minio::s3::UploadObjectResponse resp = client.UploadObject(args); !resp)
                {
                    std::cerr << "Minio Upload Error: " << resp.Error().String() << std::endl;
                    boost::beast::http::response<boost::beast::http::string_body> res{boost::beast::http::status::internal_server_error, file_parser.get().version()};
                    res.body() = "Minio Upload Error: " + resp.Error().String();
                    res.prepare_payload();
                    boost::beast::http::write(socket, res, ec);
                    return;
                }

                boost::beast::http::message_generator msg = handle_request(*doc_root, file_parser.release(), broker);
                keep_alive = msg.keep_alive();
                boost::beast::write(socket, std::move(msg), ec);
            }
        }

        else
        {

            boost::beast::http::request_parser<boost::beast::http::string_body> string_parser(std::move(req_header_parser));
            string_parser.body_limit(5 * 1024 * 1024); // 5MB limit for normal requests

            boost::beast::http::read(socket, buffer, string_parser, ec);
            if(ec) return fail(ec, "read string body");

            boost::beast::http::message_generator msg = handle_request(*doc_root, string_parser.release(), broker);
            keep_alive = msg.keep_alive();
            boost::beast::write(socket, std::move(msg), ec);
        }

        if(ec) return fail(ec, "write");
        if(!keep_alive) break;
    }

    socket.shutdown(boost::asio::ip::tcp::socket::shutdown_send, ec);
    socket.close();
}



////////////////////////////////////////////////////////////////////////////////
int main(int argc AMQCPP_UNUSED, char* argv[] AMQCPP_UNUSED) {
    cout << "=====================================================\n";
    cout << "Starting the ActiveMQ:" << endl;
    cout << "-----------------------------------------------------\n";
    activemq::library::ActiveMQCPP::initializeLibrary();
    auto broker = std::make_shared<QueueProducer>(brokerURI, destURI, false, true);

    // activemq::library::ActiveMQCPP::initializeLibrary();

    // Use "tcp://localhost:61616" for OpenWire
    // Use "stomp://localhost:61613" for STOMP
    /*std::string brokerURI = "tcp://172.31.104.113:61616";

    try
    {
        auto factory = cms::ConnectionFactory::createCMSConnectionFactory(brokerURI);
        auto connection = factory->createConnection();
        connection->start();

        auto session = connection->createSession(cms::Session::AUTO_ACKNOWLEDGE);
        auto destination = session->createQueue("WorkQueue");
        auto producer = session->createProducer(destination);

        auto message = session->createTextMessage("Hello Artemis - I've been held for 1 minute!");

        // 1. Calculate the delivery time (Current Time + 60,000ms)
        // Using std::chrono for modern C++ time handling
        auto now = std::chrono::system_clock::now().time_since_epoch();
        long long currentTimeMillis = std::chrono::duration_cast<std::chrono::milliseconds>(now).count();
        long long scheduledTime = currentTimeMillis + 30000; // 60,000ms = 1 minute

        // message->setLongProperty("_AMQ_SCHED_DELIVERY", scheduledTime);
        message->setStringProperty("_AMQ_LVQ_NAME", "myUniqueID_1min");
        producer->send(message);
        message->setText("This is the second");
        producer->send(message);

        std::cout << "Sent scheduled message. It will appear in WorkQueue in 60 seconds." << std::endl;

        // Cleanup...
        delete message;
        delete producer;
        delete destination;
        delete session;
        connection->close();
        delete connection;
        delete factory;
    }
    catch (cms::CMSException& e)
    {
        e.printStackTrace();
    }

    activemq::library::ActiveMQCPP::shutdownLibrary();
    exit(EXIT_SUCCESS);
    return 0;*/
    /////////////////////////////////////////////////////
    /*
    // const TextMessage* text_message = broker.dequeue();

    args.bucket = "gromacs-topology";
    // args.object = text_message->getStringProperty("path");
    args.object = "benchMEM/benchMEM.tpr";
    args.filename = string(getenv("GROMACS_TOPOLOGY_DIR")) + "/benchMEM.tpr";
    args.overwrite = true;
    const minio::s3::DownloadObjectResponse resp = client.DownloadObject(args);
    if (!resp) {
        // return server_error("Unable to download object" + resp.Error().String());
        cout << "Unable to download object " + resp.Error().String();
    }

    cout << resp.message;*/
    ////////////////////////////////////////////////////////////////////////////////
    auto const doc_root = std::make_shared<std::string>("./");

    try
    {
        boost::asio::io_context io_context;

        boost::asio::ip::tcp::acceptor acceptor(io_context, boost::asio::ip::tcp::endpoint(boost::asio::ip::tcp::v4(), 65096));
        for (;;)
        {
            boost::asio::ip::tcp::socket socket(io_context);
            acceptor.accept(socket);

            // Launch the session, transferring ownership of the socket
            std::thread{std::bind( &do_session, std::move(socket),doc_root, broker)}.detach();
        }
        activemq::library::ActiveMQCPP::shutdownLibrary();
    }
    catch (const std::exception& e)
    {
        std::cerr << "Error: " << e.what() << std::endl;
        activemq::library::ActiveMQCPP::shutdownLibrary();
        return EXIT_FAILURE;
    }

}
