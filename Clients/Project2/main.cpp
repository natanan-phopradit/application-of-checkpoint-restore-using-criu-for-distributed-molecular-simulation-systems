#include <array>
#include <memory>
#include <iostream>
#include <fcntl.h>
#include <unistd.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <sys/stat.h>
#include <criu/criu.h>
#include <string>
#include <csignal>
#include <filesystem>
#include <utility>
#include <fstream>
#include <regex>
#include <hwinfo/hwinfo.h>

#include <boost/asio.hpp>
#include <boost/beast/core.hpp>
#include <boost/beast/http.hpp>
#include <boost/beast/version.hpp>
#include <boost/process.hpp>
#include <boost/chrono.hpp>
#include <boost/json.hpp>


#include <activemq/cmsutil/CmsAccessor.h>
#include <decaf/util/concurrent/CountDownLatch.h>
#include <activemq/core/ActiveMQConnectionFactory.h>
#include <activemq/library/ActiveMQCPP.h>
#include <cms/Connection.h>
#include <cms/Session.h>
#include <cms/TextMessage.h>
#include <activemq/core/ActiveMQQueueBrowser.h>
#include <activemq/core/PrefetchPolicy.h>

#include <boost/beast/core/detail/base64.hpp>
#include <dotenv/dotenv.hpp>

#include <boost/log/trivial.hpp>
#include <boost/log/utility/setup/file.hpp>
#include <boost/log/utility/setup/common_attributes.hpp>
#include <boost/chrono/chrono.hpp>
#include <boost/date_time/posix_time/posix_time.hpp>
#include <chrono>
#include <format>

using namespace std;

static std::string now_ts() {
    const boost::posix_time::ptime now = boost::posix_time::microsec_clock::universal_time();
    return boost::posix_time::to_simple_string(now);
}

namespace {
    volatile std::sig_atomic_t gSignalStatus;
}

volatile std::atomic<pid_t> gromacs_pid;
std::string topology_file;
const std::string CHECKPOINT_DIR = "/dev/shm/checkpoint_imgs";

std::unique_ptr<cms::TextMessage> text_message;

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
    Destination* snapshotDestination;
    MessageProducer* producer;
    MessageProducer* snapshotProducer;
    bool useTopic;
    bool clientAck;
    string brokerURI;
    string destURI;

    MessageConsumer* consumer;

public:
    QueueProducer( const QueueProducer& ) = delete;
    QueueProducer& operator= ( const QueueProducer& ) = delete;

    QueueProducer( string  brokerURI, string  destURI, const bool useTopic = false, const bool clientAck = false ) :
        connection(nullptr),
        session(nullptr),
        destination(nullptr),
        snapshotDestination(nullptr),
        producer(nullptr),
        snapshotProducer(nullptr),
        useTopic(useTopic),
        clientAck(clientAck),
        brokerURI(std::move(brokerURI)),
        destURI(std::move(destURI)),
        consumer(nullptr)
    {

        const unique_ptr<ActiveMQConnectionFactory> connectionFactory(
                new ActiveMQConnectionFactory( this->brokerURI ) );

        connectionFactory->setCheckForDuplicates(true);

        connectionFactory->getPrefetchPolicy()->setQueuePrefetch(0);

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

        snapshotDestination = session->createQueue( "SnapshotQueue" );
        snapshotProducer = session->createProducer( snapshotDestination );
        snapshotProducer->setDeliveryMode( DeliveryMode::PERSISTENT );

        consumer = session->createConsumer( destination );
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

            TextMessage* message = session->createTextMessage(name);
            message->setStringProperty("created_at", created_at);
            message->setStringProperty("description", description);
            message->setStringProperty("path", path);
            message->setStringProperty("type", type);

            if (!toSnapshot) {
                producer->setPriority(priority);
                producer->send( message );
                BOOST_LOG_TRIVIAL(info) << "[QueueProducer] [" << now_ts() << "] Enqueued: " << name << " type=" << type << " path=" << path;
            } else
            {
                std::filesystem::path stemName = name;
                while (stemName.has_extension()) {
                    stemName = stemName.stem();
                }
                message->setStringProperty("_AMQ_LVQ_NAME", stemName);
                snapshotProducer->setPriority(priority);
                if (expireSnapshot) {
                    message->setCMSExpiration(0);
                } else {
                    message->setCMSExpiration(240000);
                }
                snapshotProducer->send( message );
                BOOST_LOG_TRIVIAL(info) << "[QueueProducer] [" << now_ts() << "] Enqueued snapshot: " << name << " type=" << type << " path=" << path;
            }
            delete message;

        } catch ( CMSException& e ) {
            e.printStackTrace();
        }
    }


    [[nodiscard]] TextMessage* dequeue() const
    {
        string text;
        auto* textMessage = dynamic_cast< TextMessage* >( consumer->receive(5000) );
        if (textMessage) {
            BOOST_LOG_TRIVIAL(info) << "[QueueProducer] [" << now_ts() << "] Dequeued message: " << textMessage->getText() << " type=" << textMessage->getStringProperty("type") << " path=" << textMessage->getStringProperty("path");
        } else {
            BOOST_LOG_TRIVIAL(error) << "[QueueProducer] [" << now_ts() << "] Dequeue returned null - queue is empty";
            throw std::runtime_error("Dequeue failed: No message in queue (empty queue)");
        }


        /*if( clientAck ) {
            textMessage->acknowledge();
        }*/

        return textMessage;
    }

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

        try {
            if( snapshotProducer != nullptr) {
                snapshotProducer->close();
            }
        } catch ( CMSException& e ) {
            e.printStackTrace();
        }

        try {
            if( consumer != nullptr) {
                consumer->close();
            }
        } catch ( CMSException& e ) {
            e.printStackTrace();
        }

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
        delete snapshotDestination;
        delete producer;
        delete snapshotProducer;
        delete consumer;
        delete session;
        delete connection;
    }
};

void setup_criu_opts(int dir_fd, pid_t pid, const char *log_file, const bool leave_running = false) {
    criu_init_opts();
    criu_set_pid(pid);
    criu_set_images_dir_fd(dir_fd);
    criu_set_log_level(4);
    criu_set_log_file(log_file);
    criu_set_shell_job(true);
    criu_set_unprivileged(true);
    criu_set_leave_running(leave_running);
}

int criu_checkpoint(const char *log_name, const string& checkpoint_dir = CHECKPOINT_DIR, const bool leave_running = false) {
    if (gromacs_pid >= 0) {
        cout << "[Main] Initiating checkpoint..." << endl;

        if (mkdir(checkpoint_dir.c_str(), 0755) == -1) {
            if (errno != EEXIST) {
                perror("Failed to create directory");
                return -1;
            }
        }

        const int fd1 = open(checkpoint_dir.c_str(), O_DIRECTORY | O_RDONLY);
        if (fd1 < 0) {
            cerr << "Failed to open checkpoint directory: " << checkpoint_dir << endl;
            BOOST_LOG_TRIVIAL(error) << "[CRIU] [" << now_ts() << "] Failed to open checkpoint directory: " << checkpoint_dir;
            return 1;
        }

        setup_criu_opts(fd1, gromacs_pid, log_name, leave_running);
        cout << "Leave running: " << (leave_running ? "yes" : "no") << endl;
        auto t0 = boost::chrono::steady_clock::now();
        const int ret = criu_dump();
        auto t1 = boost::chrono::steady_clock::now();
        auto elapsed_ms = boost::chrono::duration_cast<boost::chrono::milliseconds>(t1 - t0).count();

        if (ret < 0) {
            BOOST_LOG_TRIVIAL(error) << "[CRIU] [" << now_ts() << "] Checkpoint failed with code " << ret;
            return ret;
        }
        cout << "[Main] Checkpoint successful." << endl;
        BOOST_LOG_TRIVIAL(info) << "[CRIU] [" << now_ts() << "] Checkpoint successful for pid " << gromacs_pid << "(took " << elapsed_ms << " ms)";
        close(fd1);

        if (!leave_running){
            waitpid(gromacs_pid, nullptr, 0);
        }
    } else {
        cout << "[Main] Gromacs is not running, abort!";
    }
    return 0;
}

int criu_restore(const char *log_name, const string& checkpoint_dir = CHECKPOINT_DIR) {
    cout << "[Main] Initiating restore from first checkpoint..." << endl;

    const int fd1_restore = open(checkpoint_dir.c_str(), O_DIRECTORY);
    if (fd1_restore < 0) {
        cerr << "[Restore Wrapper] Failed to open checkpoint directory: " << checkpoint_dir << endl;
        BOOST_LOG_TRIVIAL(error) << "[CRIU] [" << now_ts() << "] Failed to open restore directory: " << checkpoint_dir;
        return 1;
    }

    setup_criu_opts(fd1_restore, gromacs_pid, log_name);
    auto t0 = boost::chrono::steady_clock::now();
    const int res = criu_restore_child();
    auto t1 = boost::chrono::steady_clock::now();
    auto elapsed_ms = boost::chrono::duration_cast<boost::chrono::milliseconds>(t1 - t0).count();
    if ( res < 0) {
        cerr << "[Restore Wrapper] Restore failed!" << endl;
        BOOST_LOG_TRIVIAL(error) << "[CRIU] [" << now_ts() << "] Restore failed with code " << res;
        return res;
    }
    BOOST_LOG_TRIVIAL(info) << "[CRIU] [" << now_ts() << "] Restore successful for pid " << gromacs_pid << "(took " << elapsed_ms << " ms)";
    return 0;
}

pid_t run_gromacs() {
    gromacs_pid = fork();
    if (gromacs_pid == 0) {
        setsid();
        sigset_t mask;
        sigemptyset(&mask);
        sigaddset(&mask, SIGTERM);
        sigaddset(&mask, SIGINT);
        sigprocmask(SIG_BLOCK, &mask, nullptr);

        cout << "[Child] Executing GROMACS..." << endl;
        BOOST_LOG_TRIVIAL(info) << "[Child] [" << now_ts() << "] Executing GROMACS with topology: " << topology_file;
        filesystem::path checkpoint_path = topology_file;
        filesystem::create_directories(CHECKPOINT_DIR + "/" + checkpoint_path.stem().string());
        int dev_null = open("/dev/null", O_RDWR);
        if (dev_null >= 0) {
            dup2(dev_null, STDIN_FILENO);
            dup2(dev_null, STDOUT_FILENO);
            dup2(dev_null, STDERR_FILENO);
            close(dev_null);
        }

        execl(dotenv::get<string>("GMX_PATH")->c_str(), "gmx", "mdrun",
              "-s", topology_file.c_str(),
              "-deffnm", (CHECKPOINT_DIR + "/" + checkpoint_path.stem().string() + "/" + checkpoint_path.stem().string()).c_str(),
              "-nsteps", "5000",
                static_cast<char *>(nullptr));
        cerr << "[Child] Failed to execute 'gmx'. Please ensure GROMACS is installed and in PATH." << endl;
        BOOST_LOG_TRIVIAL(error) << "[Child] [" << now_ts() << "] Failed to execute 'gmx'.";
        return 1;
    } else if (gromacs_pid < 0) {
        cerr << "Failed to fork." << endl;
        BOOST_LOG_TRIVIAL(error) << "[Main] [" << now_ts() << "] Failed to fork for GROMACS: " << errno;
        return gromacs_pid;
    }

    cout << "[Main] GROMACS running with PID: " << gromacs_pid << endl;
    BOOST_LOG_TRIVIAL(info) << "[Main] [" << now_ts() << "] GROMACS running with PID: " << gromacs_pid;
    return gromacs_pid;
}


void signal_handler(const int signal) {
    // cout << "[Main] Signal received: " << signal << "" << endl;
    gSignalStatus = signal;
}

string get_topology(boost::asio::io_context &io_context, const std::shared_ptr<QueueProducer>& broker, const string &host, const string &service) {

    boost::asio::ip::tcp::resolver resolver(io_context);
    boost::beast::tcp_stream stream(io_context);
    unsigned long messageCount;
    try {
        const boost::asio::ip::tcp::resolver::results_type artemisJolokiaEndpoints =
         resolver.resolve(host, *dotenv::get<string>("ARTEMIS_PORT"));
        stream.connect(artemisJolokiaEndpoints);
        std::string const target = *dotenv::get<string>("ARTEMIS_GETCOUNT_TARGET");
        boost::beast::http::request<boost::beast::http::string_body> countReq{boost::beast::http::verb::get, target, 11};
        countReq.set(boost::beast::http::field::host, host);
        countReq.set(boost::beast::http::field::user_agent, BOOST_BEAST_VERSION_STRING);

        std::string auth = *dotenv::get<string>("ARTEMIS_AUTH");
        std::string encoded_auth;
        encoded_auth.resize(boost::beast::detail::base64::encoded_size(auth.size()));
        boost::beast::detail::base64::encode(&encoded_auth[0], auth.data(), auth.size());
        countReq.set(boost::beast::http::field::authorization, "Basic " + encoded_auth);

        boost::beast::flat_buffer countBuffer;
        boost::beast::http::response<boost::beast::http::string_body> countRes;

        boost::beast::http::write(stream, countReq);
        boost::beast::http::read(stream, countBuffer, countRes);

        cout << countRes.body() << endl;

        auto data = boost::json::parse(countRes.body());
        cout << "JSON Body: " << data << endl;
        messageCount = data.at("value").as_int64();

        if(boost::beast::error_code error_code; error_code && error_code != boost::beast::errc::not_connected)
            throw boost::beast::system_error{error_code};
    } catch (std::exception const& e) {
        std::cerr << "Error: " << e.what() << std::endl;
        BOOST_LOG_TRIVIAL(error) << "[Topology] [" << now_ts() << "] Error: " << e.what();
    }

    const boost::asio::ip::tcp::resolver::results_type endpoints =
      resolver.resolve(host, service);
    stream.connect(endpoints);


    cout << "Server has work? " << messageCount << endl;

    try {
        text_message.reset(broker->dequeue());
    } catch (const std::exception& e) {
        BOOST_LOG_TRIVIAL(error) << "[Get Topology] [" << now_ts() << "] Failed to dequeue message: " << e.what();
        throw;
    }

    if (text_message->getStringProperty("type") == "finished_snapshot") {
        BOOST_LOG_TRIVIAL(warning) << "[" << "Main" << "] [" << now_ts() << "] got finished snapshot";
        cout << "Got finished snapshot" << endl;
        text_message->acknowledge();
        return text_message->getStringProperty("type");
    }

    boost::beast::http::request<boost::beast::http::string_body> req{boost::beast::http::verb::post, "/get_work", 11};
    req.set(boost::beast::http::field::host, host);
    req.set(boost::beast::http::field::user_agent, BOOST_BEAST_VERSION_STRING);

    char hostname[HOST_NAME_MAX];
    if (gethostname(hostname, HOST_NAME_MAX) == 0) {
        std::cout << "Hostname: " << hostname << std::endl;
    } else {
        perror("gethostname");
    }

    req.set("X-Device-ID",hostname);
    req.set("X-CPU-Logical-Cores",to_string(hwinfo::getAllCPUs()[0].numLogicalCores()) );
    req.set("X-OS-Version",hwinfo::OS().version() );

    req.set(boost::beast::http::field::content_type, "application/json");

    boost::json::object mesBody;
    mesBody["Name"]         = text_message->getText();
    mesBody["Path"]         = text_message->getStringProperty("path");
    mesBody["Timestamp"]    = text_message->getStringProperty("created_at");
    mesBody["Description"]  = text_message->getStringProperty("description");
    mesBody["Type"]         = text_message->getStringProperty("type");

    req.body() = boost::json::serialize(mesBody);
    req.prepare_payload();
    boost::beast::flat_buffer buffer;
    boost::beast::http::response_parser<boost::beast::http::empty_body> header_parser;
    header_parser.body_limit(boost::none);

    auto t0 = boost::chrono::steady_clock::now();
    boost::beast::http::write(stream, req);
    boost::beast::http::read_header(stream, buffer, header_parser);

    if(header_parser.get().result() ==  boost::beast::http::status::ok) {
        auto const& res = header_parser.get();
        auto disp = res[boost::beast::http::field::content_disposition];
        auto pos = disp.find("filename=");
        std::string_view filename = disp.substr(pos + string("filename=").size());
        topology_file = "/var/tmp/" + std::string(filename);

        boost::beast::http::response_parser<boost::beast::http::file_body> res_parser(std::move(header_parser));
        boost::beast::error_code ec;
        res_parser.get().body().open(topology_file.c_str(), boost::beast::file_mode::write, ec);
        if(ec) {
            BOOST_LOG_TRIVIAL(error) << "[" << "Main" << "] [" << now_ts() << "] " << "Failed to create file: " + topology_file;
            throw std::system_error(ec, "Failed to create file: " + topology_file);
        }

        boost::beast::http::read(stream, buffer, res_parser);
        auto t1 = boost::chrono::steady_clock::now();
        auto elapsed_ms = boost::chrono::duration_cast<boost::chrono::milliseconds>(t1 - t0).count();

        BOOST_LOG_TRIVIAL(info) << "[" << "Main" << "] [" << now_ts() << "] File ( " << filename << " ) saved successfully! (took " << elapsed_ms << " ms)";
        std::cout << filename << " File saved successfully!" << std::endl;

        auto final_res = res_parser.release();
        stream.socket().shutdown(boost::asio::ip::tcp::socket::shutdown_both, ec);
        stream.socket().close(ec);
        return std::string(final_res["X-File-Type"]);
    } else {
        auto res = header_parser.release();
        std::string error_msg = "Download failed: " + std::to_string(res.result_int()) + " " + std::string(res.reason());
        BOOST_LOG_TRIVIAL(error) << "[" << "Main" << "] [" << now_ts() << "] " << error_msg;
        throw std::runtime_error(error_msg);
    }
}

void setup() {

    struct sigaction sa{};
    sa.sa_handler = signal_handler;
    sigemptyset(&sa.sa_mask); // Don't block any other signals
    sa.sa_flags = SA_SIGINFO;

    sigaction(SIGINT, &sa, nullptr);
    sigaction(SIGTERM, &sa, nullptr);

    try {
        if (std::filesystem::create_directories(CHECKPOINT_DIR)) {
            std::cout << "Directory created: " << CHECKPOINT_DIR << '\n';
        } else {
            std::cout << "Directory already exists: " << CHECKPOINT_DIR << '\n';
        }
    } catch (const std::filesystem::filesystem_error& e) {
        std::cerr << "Failed to create directory: " << e.what() << '\n';
    }
}

int create_tarball(boost::asio::io_context &io_context, const string& name, const string& checkpoint_dir, const string& checkpoint_dir_name, const string& topology_dir , const string& topology) {
    cout << "Tarball compressing with pigz" << endl;

    try {
        const std::string exe = "/usr/bin/tar";
        std::vector<std::string> args = {"-I", "pigz -3", "-cf", name , "-C",  checkpoint_dir, checkpoint_dir_name};
        auto t0 = boost::chrono::steady_clock::now();
        boost::process::process proc(io_context.get_executor(), exe, args);
        proc.wait();
        auto t1 = boost::chrono::steady_clock::now();
        auto elapsed_ms = boost::chrono::duration_cast<boost::chrono::milliseconds>(t1 - t0).count();

        BOOST_LOG_TRIVIAL(info) << "[" << "Tarball" << "] [" << now_ts() << "] Compression successful" << "(took " << elapsed_ms << " ms)";
        std::cout << "[Tarball] Compression successful." << std::endl;
    }
    catch (const std::exception &e) {
        BOOST_LOG_TRIVIAL(error) << "[Tarball] Error launching process:" << e.what();
        std::cerr << "Error launching process: " << e.what() << "\n";
        return 1;
    }
    return 0;
}

int extract_tarball(boost::asio::io_context &io_context,const string& archive_path, const string& checkpoint_dir) {
    cout << "[Tarball] Extracting " << archive_path << endl;

    try {
        const std::string exe = "/usr/bin/tar";
        std::vector<std::string> args = {"-I", "pigz", "-xf" , archive_path, "-C", checkpoint_dir};

        auto t0 = boost::chrono::steady_clock::now();
        boost::process::process proc(io_context.get_executor(), exe, args);
        proc.wait();
        auto t1 = boost::chrono::steady_clock::now();
        auto elapsed_ms = boost::chrono::duration_cast<boost::chrono::milliseconds>(t1 - t0).count();

        filesystem::path p = archive_path;
        topology_file = p.stem().stem().string() + string(".tpr");

        BOOST_LOG_TRIVIAL(info) << "[Tarball] [" << now_ts() << "] Extract tarball successful " << "(took " << elapsed_ms << " ms)";
        std::cout << "[Tarball] Extraction successful." << std::endl;
    }
    catch (const std::exception &e) {
        BOOST_LOG_TRIVIAL(error) << "[Tarball] Error launching process:" << e.what();
        std::cerr << "[Tarball] Error launching process: " << e.what() << "\n";
        return 1;
    }
    return 0;
}

void send_checkpoint(boost::asio::io_context& io_context ,const std::filesystem::path& file_path, const string &host , const string &service , const bool autoCheckpoint = false) {
    boost::asio::ip::tcp::resolver resolver(io_context);
    const boost::asio::ip::tcp::resolver::results_type endpoints = resolver.resolve(host, service);

    boost::beast::tcp_stream stream(io_context);

    stream.connect(endpoints);

    boost::beast::http::request<boost::beast::http::file_body> req{boost::beast::http::verb::post, "/send_snapshot", 11};
    req.set(boost::beast::http::field::host, host);
    req.set(boost::beast::http::field::user_agent, BOOST_BEAST_VERSION_STRING);
    req.set(boost::beast::http::field::content_type, "application/octet-stream");

    char hostname[HOST_NAME_MAX];
    if (gethostname(hostname, HOST_NAME_MAX) == 0) {
        std::cout << "Hostname: " << hostname << std::endl;
    } else {
        perror("gethostname");
    }

    req.set("X-Device-ID",hostname);

    req.set("X-CPU-Logical-Cores",to_string(hwinfo::getAllCPUs()[0].numLogicalCores()) );
    req.set("X-OS-Version",hwinfo::OS().version() );

    if (autoCheckpoint) {
        req.set("X-AutoCheckpoint", "true");
    }

    req.set("X-Filename", file_path.filename().string());

    boost::beast::error_code ec;
    req.body().open(file_path.c_str(), boost::beast::file_mode::read, ec);
    if(ec) {
        throw std::runtime_error("Failed to open tarball for upload: " + ec.message());
    }
    req.prepare_payload();

    auto t0 = boost::chrono::steady_clock::now();
    boost::beast::http::write(stream, req);
    auto t1 = boost::chrono::steady_clock::now();
    auto elapsed_ms = boost::chrono::duration_cast<boost::chrono::milliseconds>(t1 - t0).count();

    boost::beast::flat_buffer buffer;
    boost::beast::http::response<boost::beast::http::string_body> res;
    boost::beast::http::read(stream, buffer, res);

    if (res.result() == boost::beast::http::status::ok) {
        std::cout << "Upload success" << res << std::endl;
        BOOST_LOG_TRIVIAL(info) << "[Upload] [" << now_ts() << "] Upload checkpoint success for " << file_path.filename().string() << " (took " << elapsed_ms << " ms)";
    } else {
        std::cerr << "Upload Failed: " << res.result_int() << " " << res.reason() << std::endl;
        BOOST_LOG_TRIVIAL(error) << "[Upload] [" << now_ts() << "] Upload checkpoint failed: " << res.result_int() << " " << res.reason();
    }

    stream.socket().shutdown(boost::asio::ip::tcp::socket::shutdown_both, ec);
    stream.socket().close(ec);
    if (ec) {
        throw std::runtime_error("Shutdown info: " + ec.message());
    }
};

int send_result(boost::asio::io_context& io_context ,const std::filesystem::path& file_path, const string &host , const string &service ) {
    boost::asio::ip::tcp::resolver resolver(io_context);
    const boost::asio::ip::tcp::resolver::results_type endpoints = resolver.resolve(host, service);

    boost::beast::tcp_stream stream(io_context);

    stream.connect(endpoints);

    boost::beast::http::request<boost::beast::http::file_body> req{boost::beast::http::verb::post, "/send_result", 11};
    req.set(boost::beast::http::field::host, host);
    req.set(boost::beast::http::field::user_agent, BOOST_BEAST_VERSION_STRING);
    req.set(boost::beast::http::field::content_type, "application/octet-stream");

    char hostname[HOST_NAME_MAX];
    if (gethostname(hostname, HOST_NAME_MAX) == 0) {
        std::cout << "Hostname: " << hostname << std::endl;
    } else {
        perror("gethostname");
    }

    req.set("X-Device-ID",hostname);

    req.set("X-CPU-Logical-Cores",to_string(hwinfo::getAllCPUs()[0].numLogicalCores()) );
    req.set("X-OS-Version",hwinfo::OS().version() );

    req.set("X-Filename", file_path.filename().string());

    boost::beast::error_code ec;
    req.body().open(file_path.c_str(), boost::beast::file_mode::read, ec);
    if(ec) {
        throw std::runtime_error("Failed to open tarball for upload: " + ec.message());
    }
    req.prepare_payload();
    boost::beast::flat_buffer buffer;
    boost::beast::http::response<boost::beast::http::string_body> res;

    auto t0 = boost::chrono::steady_clock::now();
    boost::beast::http::write(stream, req);
    auto t1 = boost::chrono::steady_clock::now();
    auto elapsed_ms = boost::chrono::duration_cast<boost::chrono::milliseconds>(t1 - t0).count();

    boost::beast::http::read(stream, buffer, res);

    stream.socket().shutdown(boost::asio::ip::tcp::socket::shutdown_both, ec);
    stream.socket().close(ec);
    if (ec) {
        throw std::runtime_error("Shutdown info: " + ec.message());
    }

    if (res.result() == boost::beast::http::status::ok) {
        std::cout << "Upload success" << res << std::endl;
        BOOST_LOG_TRIVIAL(info) << "[Upload] [" << now_ts() << "] Result upload success for " << file_path.filename().string() << "(took " << elapsed_ms << " ms)" ;
        return 0;
    } else {
        std::cerr << "Upload Failed: " << res.result_int() << " " << res.reason() << std::endl;
        BOOST_LOG_TRIVIAL(error) << "[Upload] [" << now_ts() << "] Result upload failed: " << res.result_int() << " " << res.reason();
        return -1;
    }

}

int main() {

    dotenv::load();
    boost::log::add_file_log(
        boost::log::keywords::file_name = "project.log",
        boost::log::keywords::open_mode = std::ios::app,
        boost::log::keywords::auto_flush = true
    );
    boost::log::add_common_attributes();

    cout << "Starting Checkpoint/Restore with CRIU C++ Demo for GROMACS..." << endl;
    BOOST_LOG_TRIVIAL(info) << "[Main] [" << now_ts() << "] Starting CRIU C++ Demo for GROMACS...";

    const string brokerURI = *dotenv::get<string>("WORKQUEUE_URL");
    const string destURI = "WorkQueue";

    boost::asio::io_context io_context;
    setup();

    try {
        auto t0 = boost::chrono::steady_clock::now();
        activemq::library::ActiveMQCPP::initializeLibrary();
        auto t1 = boost::chrono::steady_clock::now();

        auto elapsed_ms = boost::chrono::duration_cast<boost::chrono::milliseconds>(t1 - t0).count();
        BOOST_LOG_TRIVIAL(info) << "[" << "ActiveMQ" << "] [" << now_ts() << "] initializeLibrary() success (took " << elapsed_ms << " ms)";
    } catch (const std::exception& e) {
        BOOST_LOG_TRIVIAL(error) << "[" << "ActiveMQ" << "] [" << now_ts() << "] initializeLibrary() error: " << e.what();
    }


    cout << "Broker URI: " << brokerURI << endl;
    BOOST_LOG_TRIVIAL(info) << "[Main] [" << now_ts() << "] Broker URI: " << brokerURI;
    std::shared_ptr<QueueProducer> broker;
    try {
        broker = std::make_shared<QueueProducer>(brokerURI, destURI, false, true);
    } catch (const cms::CMSException &e) {
        std::cerr << "Failed to create QueueProducer: " << e.what() << std::endl;
        activemq::library::ActiveMQCPP::shutdownLibrary();
        return 1;
    }

    cout << "[CTRL+D to end simulation]" << endl;

    string work_type;
    int fetch_fail_count = 0;
    for (;;) {
        bool isAcked = false;
        try {
            work_type = get_topology(io_context, broker, *dotenv::get<string>("COORDINATIONSERVER_URL"),*dotenv::get<string>("COORDINATIONSERVER_PORT"));
            fetch_fail_count = 0;
        } catch (const std::exception& e) {
            fetch_fail_count++;
            cerr << "Task fetch failed: " << e.what() << ". Retrying in 5s... (" << fetch_fail_count << "/5)" << endl;
            BOOST_LOG_TRIVIAL(error) << "[Main] [" << now_ts() << "] Task fetch failed: " << e.what() << ". Retrying in 5s... (" << fetch_fail_count << "/5)";
            if (fetch_fail_count >= 5) {
                cerr << "Task fetch failed 5 times. Aborting." << endl;
                BOOST_LOG_TRIVIAL(error) << "[Main] [" << now_ts() << "] Task fetch failed 5 times. Aborting.";
                return 1;
            }
            sleep(5);
            continue;
        }
        if (work_type == "finished_snapshot") {
            cout << "Finished snapshot, skipped..." << endl;
            continue;
        }
        if (work_type == "snapshot") {
            cout << "Work received type: " << work_type << endl;
            if (extract_tarball(io_context,topology_file, CHECKPOINT_DIR + "/") > 0) {
                cerr << "Failed to extract snapshot." << endl;
                continue;
            }
            const filesystem::path tf = topology_file;

            const string cp_dir = CHECKPOINT_DIR + "/" + tf.stem().string();
            std::string cp_log  = cp_dir + string("/dump.log");
            std::ifstream file(cp_log);

            if (!file.is_open()) {
                std::cerr << "[Main] Error: Could not open checkpoint dump file " << cp_log << std::endl;
                return 1;
            }

            std::regex pattern(R"(pid:\s*(\d+)\s*comm:\s*(\S+))");
            std::string line;
            std::smatch match;

            while (std::getline(file, line)) {
                if (std::regex_search(line, match, pattern)) {
                    gromacs_pid = std::stoi(match[1].str());
                    break;
                }
            }
            cout << "Checkpoint dir: " << cp_dir << endl;
            std::cout << "Extracted PID: " << gromacs_pid << std::endl;
            auto restore_stat = criu_restore("restore.log", cp_dir);
            cout << "Restore stat: " << restore_stat << endl;
            if (restore_stat < 0) {
                cerr << "CRIU Restore failed." << endl;
                continue;
            }
        } else {
            gromacs_pid = run_gromacs(); // Update the global variable
        }

        auto last_checkpoint_time = boost::chrono::steady_clock::now();
        constexpr auto checkpoint_interval = boost::chrono::minutes(2);

        while (true) {
            sleep(2);

            int status;
            pid_t result = waitpid(gromacs_pid, &status, WNOHANG);

            auto now = boost::chrono::steady_clock::now();
            bool time_to_checkpoint = (now - last_checkpoint_time >= checkpoint_interval);

            if (result == 0) {
                //Process is alive, and you have permission to see it
                cout << "Last checkpoint time: " << last_checkpoint_time << endl;
                cout << "Time elapsed: " << boost::chrono::steady_clock::now() - last_checkpoint_time << endl;
                cout << "Should it checkpoint? " << (time_to_checkpoint ? "yes" : "no") << endl;
            }
            else if (result == gromacs_pid) {
                cout << "[Main] GROMACS finished naturally." << endl;

                const std::filesystem::path p = std::string(topology_file.substr(topology_file.find_last_of("/\\") + 1));
                const std::string stem_name = p.stem().string(); // e.g., "91f06980-14ec-412d-8ea9-4cd05b289aa6_benchMEM"
                broker->enqueue("", std::format("{:%F %T}", std::chrono::system_clock::now()).c_str(), topology_file.c_str(), "", "finished_snapshot", 4, true);

                const std::string tar_name = stem_name + "_results.tar.gz";
                cout << "Tarname: " << tar_name << endl;
                std::string path = CHECKPOINT_DIR + "/" + stem_name; // Current directory

                try {
                    const std::string exe = "/usr/bin/tar";
                    std::vector<std::string> args = {"-I", "pigz -3", "-cf", tar_name , "-C", path};

                    for (const std::string t : {".cpt", ".edr", ".gro", ".log"}) {
                        args.push_back(stem_name + t);
                    }
                    if (std::filesystem::exists(path + "/dump.log")) args.emplace_back("dump.log");
                    if (std::filesystem::exists(path + "/restore.log")) args.emplace_back("restore.log");

                    boost::process::process proc(io_context.get_executor(), exe, args);

                    proc.wait();

                    const int exit_code = proc.exit_code();
                    std::cout << "Process exited with code: " << exit_code << "\n";
                }
                catch (const std::exception &e) {
                    std::cerr << "Error launching process: " << e.what() << "\n";
                    exit(EXIT_FAILURE);
                }
                send_result(io_context, tar_name, *dotenv::get<string>("COORDINATIONSERVER_URL"), *dotenv::get<string>("COORDINATIONSERVER_PORT"));
                std::remove(tar_name.c_str());
                std::filesystem::remove_all(path);
                if (!isAcked) {
                    text_message->acknowledge();
                    isAcked = true;
                }

                break;
            }
            else {
                if (errno != EINTR) {
                    perror("[Main] waitpid error");
                    break;
                }
            }

            if (gSignalStatus != 0) {
                // kill(gromacs_pid, SIGSTOP);
                BOOST_LOG_TRIVIAL(info) << "[Main] [" << now_ts() << "] Signal detected safely in main loop. Initiating checkpoint...";
                cout << "[Main] Signal detected safely in main loop. Initiating checkpoint..." << endl;
                const std::filesystem::path p = string(topology_file.substr(topology_file.find_last_of("/\\") + 1));
                const string cp_dir = CHECKPOINT_DIR + "/" + p.stem().string();
                if (criu_checkpoint("dump.log", cp_dir) < 0) {
                    cerr << "[Main] Checkpoint failed! Killing GROMACS." << endl;
                    kill(gromacs_pid, SIGKILL);
                    return 0;
                }

                create_tarball(io_context, p.stem().string() + ".tar.gz", CHECKPOINT_DIR + "/" , p.stem().string(), "/var/tmp" , topology_file);
                send_checkpoint(io_context, p.stem().string() + ".tar.gz", *dotenv::get<string>("COORDINATIONSERVER_URL"), *dotenv::get<string>("COORDINATIONSERVER_PORT"));
                std::filesystem::remove_all(CHECKPOINT_DIR + "/" + p.stem().string());
                std::remove(string(p.stem().string() + ".tar.gz").c_str());
                if (!isAcked) {
                    text_message->acknowledge();
                    isAcked = true;
                }
                cout << "[Main] Exiting gracefully." << endl;
                return 0; // Or break to fetch a new task, depending on your logic
            }
            if (time_to_checkpoint) {
                BOOST_LOG_TRIVIAL(info) << "[Main] [" << now_ts() << "] Time to checkpoint. Initiating...";
                cout << "[Main] Time to checkpoint. Initiating..." << endl;
                const std::filesystem::path p = string(topology_file.substr(topology_file.find_last_of("/\\") + 1));
                const string cp_dir = CHECKPOINT_DIR + "/" + p.stem().string();
                if (criu_checkpoint("dump.log", cp_dir, true) < 0) {
                    cerr << "[Main] Checkpoint failed! Killing GROMACS." << endl;
                    kill(gromacs_pid, SIGKILL);
                    return 0;
                }
                create_tarball(io_context, p.stem().string() + ".tar.gz", CHECKPOINT_DIR + "/" , p.stem().string(), "/var/tmp" , topology_file);
                send_checkpoint(io_context, p.stem().string() + ".tar.gz", *dotenv::get<string>("COORDINATIONSERVER_URL"), *dotenv::get<string>("COORDINATIONSERVER_PORT"), true);
                // std::filesystem::remove_all(CHECKPOINT_DIR + "/" + p.stem().string());
                std::remove(string(p.stem().string() + ".tar.gz").c_str());

                if (!isAcked) {
                    text_message->acknowledge();
                    isAcked = true;
                }

                time_to_checkpoint = false;
                last_checkpoint_time = boost::chrono::steady_clock::now();
            }

            // Sleep briefly to prevent 100% CPU usage on the Main loop
            usleep(100000); // 100ms
        }
    }

}
