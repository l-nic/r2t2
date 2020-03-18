#include "lambda-master.hh"

#include <getopt.h>
#include <glog/logging.h>
#include <pbrt/core/sampler.h>

#include <algorithm>
#include <chrono>
#include <cmath>
#include <ctime>
#include <deque>
#include <fstream>
#include <iomanip>
#include <iostream>
#include <memory>
#include <numeric>
#include <random>
#include <sstream>
#include <string>
#include <thread>
#include <vector>

#include "execution/loop.hh"
#include "execution/meow/message.hh"
#include "messages/utils.hh"
#include "net/lambda.hh"
#include "net/requests.hh"
#include "net/socket.hh"
#include "net/transfer_mcd.hh"
#include "net/util.hh"
#include "schedulers/dynamic.hh"
#include "schedulers/null.hh"
#include "schedulers/rootonly.hh"
#include "schedulers/static.hh"
#include "schedulers/uniform.hh"
#include "util/exception.hh"
#include "util/path.hh"
#include "util/random.hh"
#include "util/status_bar.hh"
#include "util/temp_file.hh"
#include "util/tokenize.hh"
#include "util/uri.hh"
#include "util/util.hh"

using namespace std;
using namespace chrono;
using namespace meow;
using namespace r2t2;
using namespace pbrt;

using namespace PollerShortNames;

using OpCode = Message::OpCode;
using PollerResult = Poller::Result::Type;

map<LambdaMaster::Worker::Role, size_t> LambdaMaster::Worker::activeCount = {};
WorkerId LambdaMaster::Worker::nextId = 0;

LambdaMaster::~LambdaMaster() {
    try {
        roost::empty_directory(sceneDir.name());
    } catch (exception &ex) {
    }
}

LambdaMaster::LambdaMaster(const uint16_t listenPort, const uint16_t clientPort,
                           const uint32_t maxWorkers,
                           const uint32_t rayGenerators,
                           const string &publicAddress,
                           const string &storageBackendUri,
                           const string &awsRegion,
                           unique_ptr<Scheduler> &&scheduler,
                           const MasterConfiguration &config)
    : config(config),
      jobId([] {
          ostringstream oss;
          oss << hex << setfill('0') << setw(8) << time(nullptr);
          return oss.str();
      }()),
      maxWorkers(maxWorkers),
      rayGenerators(rayGenerators),
      scheduler(move(scheduler)),
      publicAddress(publicAddress),
      storageBackendUri(storageBackendUri),
      storageBackend(StorageBackend::create_backend(storageBackendUri)),
      awsRegion(awsRegion),
      awsAddress(LambdaInvocationRequest::endpoint(awsRegion), "https"),
      workerStatsWriteTimer(seconds{config.workerStatsWriteInterval},
                            milliseconds{1}) {
    const string scenePath = sceneDir.name();
    roost::create_directories(scenePath);

    protobuf::InvocationPayload invocationProto;
    invocationProto.set_storage_backend(storageBackendUri);
    invocationProto.set_coordinator(publicAddress);
    invocationProto.set_samples_per_pixel(config.samplesPerPixel);
    invocationProto.set_max_path_depth(config.maxPathDepth);
    invocationProto.set_ray_log_rate(config.rayLogRate);
    invocationProto.set_bag_log_rate(config.bagLogRate);
    invocationProto.set_directional_treelets(PbrtOptions.directionalTreelets);

    for (const auto &memcachedServer : config.memcachedServers) {
        *invocationProto.add_memcached_servers() = memcachedServer;
    }

    invocationPayload = protoutil::to_json(invocationProto);

    /* download required scene objects from the bucket */
    auto getSceneObjectRequest = [&scenePath](const ObjectType type) {
        return storage::GetRequest{
            scene::GetObjectName(type, 0),
            roost::path(scenePath) / scene::GetObjectName(type, 0)};
    };

    vector<storage::GetRequest> sceneObjReqs{
        getSceneObjectRequest(ObjectType::Manifest),
        getSceneObjectRequest(ObjectType::Camera),
        getSceneObjectRequest(ObjectType::Sampler),
        getSceneObjectRequest(ObjectType::Lights),
        getSceneObjectRequest(ObjectType::Scene),
    };

    cerr << "Downloading scene data... ";
    storageBackend->get(sceneObjReqs);
    cerr << "done." << endl;

    /* now we can initialize the scene */
    scene = {scenePath, config.samplesPerPixel, config.cropWindow};

    /* initializing the treelets array */
    const size_t treeletCount = scene.base.GetTreeletCount();
    treelets.reserve(treeletCount);
    treeletStats.reserve(treeletCount);

    for (size_t i = 0; i < treeletCount; i++) {
        treelets.emplace_back(i);
        treeletStats.emplace_back();
        unassignedTreelets.insert(i);
    }

    queuedRayBags.resize(treeletCount);
    pendingRayBags.resize(treeletCount);

    tiles = Tiles{config.tileSize, scene.sampleBounds,
                  scene.base.sampler->samplesPerPixel,
                  rayGenerators ? rayGenerators : maxWorkers};

    /* are we logging anything? */
    if (config.collectDebugLogs || config.workerStatsWriteInterval > 0 ||
        config.rayLogRate > 0 || config.bagLogRate > 0) {
        roost::create_directories(config.logsDirectory);
    }

    if (config.workerStatsWriteInterval > 0) {
        wsStream.open(config.logsDirectory + "/" + "workers.csv", ios::trunc);
        tlStream.open(config.logsDirectory + "/" + "treelets.csv", ios::trunc);

        wsStream << "timestamp,workerId,pathsFinished,"
                    "raysEnqueued,raysAssigned,raysDequeued,"
                    "bytesEnqueued,bytesAssigned,bytesDequeued,"
                    "bagsEnqueued,bagsAssigned,bagsDequeued,"
                    "numSamples,bytesSamples,bagsSamples,cpuUsage\n";

        tlStream << "timestamp,treeletId,raysEnqueued,raysDequeued,"
                    "bytesEnqueued,bytesDequeued,bagsEnqueued,bagsDequeued\n";
    }

    auto printInfo = [](char const *key, auto value) {
        cerr << "  " << key << "    \e[1m" << value << "\e[0m" << endl;
    };

    cerr << endl << "Job info:" << endl;
    printInfo("Job ID           ", jobId);
    printInfo("Working directory", scenePath);
    printInfo("Public address   ", publicAddress);
    printInfo("Maxium workers   ", maxWorkers);
    printInfo("Ray generators   ", rayGenerators);
    printInfo("Treelet count    ", treeletCount);
    printInfo("Tile size        ",
              to_string(tiles.tileSize) + "\u00d7" + to_string(tiles.tileSize));
    printInfo("Output dimensions", to_string(scene.sampleExtent.x) + "\u00d7" +
                                       to_string(scene.sampleExtent.y));
    printInfo("Samples per pixel", config.samplesPerPixel);
    printInfo("Total paths      ", scene.sampleExtent.x * scene.sampleExtent.y *
                                       config.samplesPerPixel);
    cerr << endl;

    loop.poller().add_action(Poller::Action(
        rescheduleTimer, Direction::In,
        bind(&LambdaMaster::handleReschedule, this),
        [this]() { return finishedRayGenerators == this->rayGenerators; },
        []() { throw runtime_error("rescheduler failed"); }));

    loop.poller().add_action(Poller::Action(
        alwaysOnFd, Direction::Out,
        [&]() {
            jobExitTimer = make_unique<TimerFD>(14min + 50s);

            loop.poller().add_action(Poller::Action(
                *jobExitTimer, Direction::In,
                [&]() { return ResultType::Exit; }, []() { return true; },
                []() { throw runtime_error("job exit failed"); }));

            return ResultType::Cancel;
        },
        [this]() { return finishedRayGenerators == this->rayGenerators; },
        []() { throw runtime_error("job exit failed"); }));

    loop.poller().add_action(Poller::Action(
        alwaysOnFd, Direction::Out, bind(&LambdaMaster::handleMessages, this),
        [this]() { return !incomingMessages.empty(); },
        []() { throw runtime_error("messages failed"); }));

    loop.poller().add_action(Poller::Action(
        alwaysOnFd, Direction::Out,
        bind(&LambdaMaster::handleQueuedRayBags, this),
        [this]() {
            return initializedWorkers >= this->maxWorkers &&
                   !freeWorkers.empty() &&
                   (tiles.cameraRaysRemaining() || queuedRayBagsCount > 0);
        },
        []() { throw runtime_error("queued ray bags failed"); }));

    if (config.workerStatsWriteInterval > 0) {
        loop.poller().add_action(Poller::Action(
            workerStatsWriteTimer, Direction::In,
            bind(&LambdaMaster::handleWorkerStats, this),
            [this]() { return true; },
            []() { throw runtime_error("worker stats failed"); }));
    }

    loop.poller().add_action(Poller::Action(
        workerInvocationTimer, Direction::In,
        bind(&LambdaMaster::handleWorkerInvocation, this),
        [this]() {
            return !treeletsToSpawn.empty() &&
                   Worker::activeCount[Worker::Role::Tracer] < this->maxWorkers;
        },
        []() { throw runtime_error("worker invocation failed"); }));

    loop.poller().add_action(Poller::Action(
        statusPrintTimer, Direction::In,
        bind(&LambdaMaster::handleStatusMessage, this),
        [this]() { return true; },
        []() { throw runtime_error("status print failed"); }));

    loop.make_listener({"0.0.0.0", listenPort}, [this, maxWorkers](
                                                    ExecutionLoop &loop,
                                                    TCPSocket &&socket) {
        ScopeTimer<TimeLog::Category::AcceptWorker> timer_;
        /* do we want this worker? */
        if (Worker::nextId >= this->rayGenerators && treeletsToSpawn.empty()) {
            socket.close();
            return true;
        }

        const WorkerId workerId = Worker::nextId++;

        auto connectionCloseHandler = [this, workerId]() {
            ScopeTimer<TimeLog::Category::CloseWorker> timer_;

            auto &worker = workers[workerId];

            if (worker.state == Worker::State::Terminating) {
                if (worker.role == Worker::Role::Generator) {
                    lastGeneratorDone = steady_clock::now();
                    finishedRayGenerators++;
                }

                /* it's okay for this worker to go away,
                   let's not panic! */
                worker.state = Worker::State::Terminated;
                Worker::activeCount[worker.role]--;

                if (!worker.outstandingRayBags.empty()) {
                    throw runtime_error(
                        "worker died without finishing its work: " +
                        to_string(workerId));
                }

                return;
            }

            cerr << "Worker info: " << worker.toString() << endl;

            throw runtime_error(
                "worker died unexpectedly: " + to_string(workerId) +
                (worker.awsLogStream.empty()
                     ? ""s
                     : (" ("s + worker.awsLogStream + ")"s)));
        };

        auto parser = make_shared<MessageParser>();
        auto connection = loop.add_connection<TCPSocket>(
            move(socket),
            [this, workerId, parser](auto, string &&data) {
                ScopeTimer<TimeLog::Category::TCPReceive> timer_;
                parser->parse(data);

                while (!parser->empty()) {
                    incomingMessages.emplace_back(workerId,
                                                  move(parser->front()));
                    parser->pop();
                }

                return true;
            },
            [this, workerId]() {
                auto &worker = workers[workerId];

                throw runtime_error(
                    "worker died unexpected: " + to_string(workerId) +
                    (worker.awsLogStream.empty()
                         ? ""s
                         : (" ("s + worker.awsLogStream + ")"s)));
            },
            connectionCloseHandler);

        if (workerId < this->rayGenerators) {
            /* This worker is a ray generator
               Let's (1) say hi, (2) tell the worker to fetch the scene,
               (3) generate rays for its tile */

            /* (0) create the entry for the worker */
            workers.emplace_back(workerId, Worker::Role::Generator,
                                 move(connection));
            auto &worker = workers.back();

            assignBaseObjects(worker);

            /* (1) saying hi, assigning id to the worker */
            protobuf::Hey heyProto;
            heyProto.set_worker_id(workerId);
            heyProto.set_job_id(jobId);
            worker.connection->enqueue_write(
                Message::str(0, OpCode::Hey, protoutil::to_string(heyProto)));

            /* (2) tell the worker to get the scene objects necessary */
            protobuf::GetObjects objsProto;
            for (const ObjectKey &id : worker.objects) {
                *objsProto.add_object_ids() = to_protobuf(id);
            }

            worker.connection->enqueue_write(Message::str(
                0, OpCode::GetObjects, protoutil::to_string(objsProto)));

            /* (3) Tell the worker to generate rays */
            if (tiles.cameraRaysRemaining()) {
                tiles.sendWorkerTile(worker);
            } else {
                /* Too many ray launchers for tile size,
                 * so just finish immediately */
                worker.connection->enqueue_write(
                    Message::str(0, OpCode::FinishUp, ""));
                worker.state = Worker::State::FinishingUp;
            }
        } else {
            /* this is a normal worker */
            if (!treeletsToSpawn.empty()) {
                const TreeletId treeletId = treeletsToSpawn.front();
                treeletsToSpawn.pop_front();

                auto &treelet = treelets[treeletId];
                treelet.pendingWorkers--;

                /* (0) create the entry for the worker */
                workers.emplace_back(workerId, Worker::Role::Tracer,
                                     move(connection));
                auto &worker = workers.back();

                assignBaseObjects(worker);
                assignTreelet(worker, treelet);

                /* (1) saying hi, assigning id to the worker */
                protobuf::Hey heyProto;
                heyProto.set_worker_id(workerId);
                heyProto.set_job_id(jobId);
                worker.connection->enqueue_write(Message::str(
                    0, OpCode::Hey, protoutil::to_string(heyProto)));

                /* (2) tell the worker to get the scene objects necessary */
                protobuf::GetObjects objsProto;
                for (const ObjectKey &id : worker.objects) {
                    *objsProto.add_object_ids() = to_protobuf(id);
                }

                worker.connection->enqueue_write(Message::str(
                    0, OpCode::GetObjects, protoutil::to_string(objsProto)));

                freeWorkers.push_back(worker.id);
            } else {
                throw runtime_error("we accepted a useless worker");
            }
        }

        return true;
    });

    if (clientPort != 0) {
        throw runtime_error("client support is disabled");
    }
}

string LambdaMaster::Worker::toString() const {
    ostringstream oss;

    size_t oustandingSize = 0;
    for (const auto &bag : outstandingRayBags) oustandingSize += bag.bagSize;

    oss << "id=" << id << ",state=" << static_cast<int>(state)
        << ",role=" << static_cast<int>(role) << ",awslog=" << awsLogStream
        << ",treelets=";

    for (auto it = treelets.begin(); it != treelets.end(); it++) {
        if (it != treelets.begin()) oss << ",";
        oss << *it;
    }

    oss << ",outstanding=" << outstandingRayBags.size()
        << ",outstanding-bytes=" << oustandingSize
        << ",enqueued=" << stats.enqueued.bytes
        << ",assigned=" << stats.assigned.bytes
        << ",dequeued=" << stats.dequeued.bytes
        << ",samples=" << stats.samples.bytes;

    return oss.str();
}

void LambdaMaster::run() {
    StatusBar::get();

    if (!config.memcachedServers.empty()) {
        cerr << "Flushing " << config.memcachedServers.size() << " memcached "
             << pluralize("server", config.memcachedServers.size()) << "... ";

        vector<Address> servers;

        for (auto &server : config.memcachedServers) {
            string host;
            uint16_t port = 11211;
            tie(host, port) = Address::decompose(server);
            servers.emplace_back(host, port);
        }

        Poller poller;
        unique_ptr<memcached::TransferAgent> agent =
            make_unique<memcached::TransferAgent>(servers, servers.size());

        size_t flushedCount = 0;

        poller.add_action(Poller::Action(
            agent->eventfd(), Direction::In,
            [&]() {
                if (!agent->eventfd().read_event()) return ResultType::Continue;

                vector<pair<uint64_t, string>> actions;
                agent->tryPopBulk(back_inserter(actions));

                flushedCount += actions.size();

                if (flushedCount == config.memcachedServers.size()) {
                    return ResultType::Exit;
                } else {
                    return ResultType::Continue;
                }
            },
            []() { return true; }));

        agent->flushAll();

        while (poller.poll(-1).result == PollerResult::Success)
            ;

        cerr << "done." << endl;
    }

    if (rayGenerators > 0) {
        /* let's invoke the ray generators */
        cerr << "Launching " << rayGenerators << " ray "
             << pluralize("generator", rayGenerators) << "... ";

        invokeWorkers(rayGenerators + static_cast<size_t>(0.1 * rayGenerators));
        cerr << "done." << endl;
    }

    while (true) {
        auto res = loop.loop_once().result;
        if (res != PollerResult::Success && res != PollerResult::Timeout) break;
    }

    vector<storage::GetRequest> getRequests;
    const string logPrefix = "logs/" + jobId + "/";

    wsStream.close();
    tlStream.close();

    for (auto &worker : workers) {
        if (worker.state != Worker::State::Terminated) {
            worker.connection->socket().close();
        }

        if (config.collectDebugLogs || config.rayLogRate || config.bagLogRate) {
            getRequests.emplace_back(
                logPrefix + to_string(worker.id) + ".INFO",
                config.logsDirectory + "/" + to_string(worker.id) + ".INFO");
        }
    }

    cerr << endl;

    printJobSummary();

    if (!config.jobSummaryPath.empty()) {
        dumpJobSummary();
    }

    if (!getRequests.empty()) {
        cerr << "\nDownloading " << getRequests.size() << " log file(s)... ";
        this_thread::sleep_for(10s);
        storageBackend->get(getRequests);
        cerr << "done." << endl;
    }
}

void usage(const char *argv0, int exitCode) {
    cerr << "Usage: " << argv0 << " [OPTION]..." << endl
         << endl
         << "Options:" << endl
         << "  -p --port PORT             port to use" << endl
         << "  -P --client-port PORT      port for clients to connect" << endl
         << "  -i --ip IPSTRING           public ip of this machine" << endl
         << "  -r --aws-region REGION     region to run lambdas in" << endl
         << "  -b --storage-backend NAME  storage backend URI" << endl
         << "  -m --max-workers N         maximum number of workers" << endl
         << "  -G --ray-generators N      number of ray generators" << endl
         << "  -g --debug-logs            collect worker debug logs" << endl
         << "  -w --worker-stats N        log worker stats every N seconds"
         << endl
         << "  -L --log-rays RATE         log ray actions" << endl
         << "  -B --log-bags RATE         log bag actions" << endl
         << "  -D --logs-dir DIR          set logs directory (default: logs/)"
         << endl
         << "  -S --samples N             number of samples per pixel" << endl
         << "  -M --max-depth N           maximum path depth" << endl
         << "  -a --scheduler TYPE        indicate scheduler type:" << endl
         << "                               - uniform (default)" << endl
         << "                               - static" << endl
         << "                               - all" << endl
         << "                               - none" << endl
         << "  -c --crop-window X,Y,Z,T   set render bounds to [(X,Y), (Z,T))"
         << endl
         << "  -T --pix-per-tile N        pixels per tile (default=44)" << endl
         << "  -n --new-tile-send N       threshold for sending new tiles"
         << endl
         << "  -t --timeout T             exit after T seconds of inactivity"
         << endl
         << "  -j --job-summary FILE      output the job summary in JSON format"
         << endl
         << "  -d --memcached-server      address for memcached" << endl
         << "                             (can be repeated)" << endl
         << "  -h --help                  show help information" << endl;

    exit(exitCode);
}

Optional<Bounds2i> parseCropWindowOptarg(const string &optarg) {
    vector<string> args = split(optarg, ",");
    if (args.size() != 4) return {};

    Point2i pMin, pMax;
    pMin.x = stoi(args[0]);
    pMin.y = stoi(args[1]);
    pMax.x = stoi(args[2]);
    pMax.y = stoi(args[3]);

    return {true, Bounds2i{pMin, pMax}};
}

int main(int argc, char *argv[]) {
    if (argc <= 0) {
        abort();
    }

    timer();

    google::InitGoogleLogging(argv[0]);

    uint16_t listenPort = 50000;
    uint16_t clientPort = 0;
    int32_t maxWorkers = -1;
    int32_t rayGenerators = 0;
    string publicIp;
    string storageBackendUri;
    string region{"us-west-2"};
    uint64_t workerStatsWriteInterval = 0;
    bool collectDebugLogs = false;
    float rayLogRate = 0.0;
    float bagLogRate = 0.0;
    string logsDirectory = "logs/";
    Optional<Bounds2i> cropWindow;
    uint32_t timeout = 0;
    uint32_t pixelsPerTile = 0;
    uint64_t newTileThreshold = 10000;
    string jobSummaryPath;

    unique_ptr<Scheduler> scheduler = nullptr;
    string schedulerName;

    int samplesPerPixel = 0;
    int maxPathDepth = 5;
    int tileSize = 0;

    uint32_t maxJobsOnEngine = 1;
    vector<string> memcachedServers;
    vector<pair<string, uint32_t>> engines;

    struct option long_options[] = {
        {"port", required_argument, nullptr, 'p'},
        {"client-port", required_argument, nullptr, 'P'},
        {"ip", required_argument, nullptr, 'i'},
        {"aws-region", required_argument, nullptr, 'r'},
        {"storage-backend", required_argument, nullptr, 'b'},
        {"max-workers", required_argument, nullptr, 'm'},
        {"ray-generators", required_argument, nullptr, 'G'},
        {"scheduler", required_argument, nullptr, 'a'},
        {"debug-logs", no_argument, nullptr, 'g'},
        {"worker-stats", required_argument, nullptr, 'w'},
        {"log-rays", required_argument, nullptr, 'L'},
        {"log-bags", required_argument, nullptr, 'B'},
        {"logs-dir", required_argument, nullptr, 'D'},
        {"samples", required_argument, nullptr, 'S'},
        {"max-depth", required_argument, nullptr, 'M'},
        {"crop-window", required_argument, nullptr, 'c'},
        {"timeout", required_argument, nullptr, 't'},
        {"job-summary", required_argument, nullptr, 'j'},
        {"pix-per-tile", required_argument, nullptr, 'T'},
        {"new-tile-send", required_argument, nullptr, 'n'},
        {"directional", no_argument, nullptr, 'I'},
        {"jobs", required_argument, nullptr, 'J'},
        {"memcached-server", required_argument, nullptr, 'd'},
        {"engine", required_argument, nullptr, 'E'},
        {"help", no_argument, nullptr, 'h'},
        {nullptr, 0, nullptr, 0},
    };

    while (true) {
        const int opt = getopt_long(
            argc, argv, "p:P:i:r:b:m:G:w:D:a:S:M:L:c:t:j:T:n:J:d:E:B:gh",
            long_options, nullptr);

        if (opt == -1) {
            break;
        }

        switch (opt) {
        // clang-format off
        case 'p': listenPort = stoi(optarg); break;
        case 'P': clientPort = stoi(optarg); break;
        case 'i': publicIp = optarg; break;
        case 'r': region = optarg; break;
        case 'b': storageBackendUri = optarg; break;
        case 'm': maxWorkers = stoi(optarg); break;
        case 'G': rayGenerators = stoi(optarg); break;
        case 'a': schedulerName = optarg; break;
        case 'g': collectDebugLogs = true; break;
        case 'w': workerStatsWriteInterval = stoul(optarg); break;
        case 'D': logsDirectory = optarg; break;
        case 'S': samplesPerPixel = stoi(optarg); break;
        case 'M': maxPathDepth = stoi(optarg); break;
        case 'L': rayLogRate = stof(optarg); break;
        case 'B': bagLogRate = stof(optarg); break;
        case 't': timeout = stoul(optarg); break;
        case 'j': jobSummaryPath = optarg; break;
        case 'n': newTileThreshold = stoull(optarg); break;
        case 'I': PbrtOptions.directionalTreelets = true; break;
        case 'J': maxJobsOnEngine = stoul(optarg); break;
        case 'd': memcachedServers.emplace_back(optarg); break;
        case 'E': engines.emplace_back(optarg, maxJobsOnEngine); break;
        case 'h': usage(argv[0], EXIT_SUCCESS); break;

            // clang-format on

        case 'T': {
            if (strcmp(optarg, "auto") == 0) {
                tileSize = numeric_limits<typeof(tileSize)>::max();
                pixelsPerTile = numeric_limits<typeof(pixelsPerTile)>::max();
            } else {
                pixelsPerTile = stoul(optarg);
                tileSize = ceil(sqrt(pixelsPerTile));
            }
            break;
        }

        case 'c':
            cropWindow = parseCropWindowOptarg(optarg);

            if (!cropWindow.initialized()) {
                cerr << "Error: bad crop window (" << optarg << ")." << endl;
                usage(argv[0], EXIT_FAILURE);
            }

            break;

        default:
            usage(argv[0], EXIT_FAILURE);
            break;
        }
    }

    if (schedulerName == "uniform") {
        scheduler = make_unique<UniformScheduler>();
    } else if (schedulerName == "static") {
        auto storage = StorageBackend::create_backend(storageBackendUri);
        TempFile staticFile{"/tmp/r2t2-lambda-master.STATIC0"};

        cerr << "Downloading static assignment file... ";
        storage->get({{scene::GetObjectName(ObjectType::StaticAssignment, 0),
                       staticFile.name()}});
        cerr << "done." << endl;

        scheduler = make_unique<StaticScheduler>(staticFile.name());
    } else if (schedulerName == "dynamic") {
        scheduler = make_unique<DynamicScheduler>();
    } else if (schedulerName == "rootonly") {
        scheduler = make_unique<RootOnlyScheduler>();
    } else if (schedulerName == "null") {
        scheduler = make_unique<NullScheduler>();
    } else {
        usage(argv[0], EXIT_FAILURE);
    }

    if (scheduler == nullptr || listenPort == 0 || maxWorkers <= 0 ||
        rayGenerators < 0 || samplesPerPixel < 0 || maxPathDepth < 0 ||
        rayLogRate < 0 || rayLogRate > 1.0 || bagLogRate < 0 ||
        bagLogRate > 1.0 || publicIp.empty() || storageBackendUri.empty() ||
        region.empty() || newTileThreshold == 0 ||
        (cropWindow.initialized() && pixelsPerTile != 0 &&
         pixelsPerTile != numeric_limits<typeof(pixelsPerTile)>::max() &&
         pixelsPerTile > cropWindow->Area())) {
        usage(argv[0], 2);
    }

    ostringstream publicAddress;
    publicAddress << publicIp << ":" << listenPort;

    unique_ptr<LambdaMaster> master;

    // TODO clean this up
    MasterConfiguration config = {samplesPerPixel,
                                  maxPathDepth,
                                  collectDebugLogs,
                                  workerStatsWriteInterval,
                                  rayLogRate,
                                  bagLogRate,
                                  logsDirectory,
                                  cropWindow,
                                  tileSize,
                                  seconds{timeout},
                                  jobSummaryPath,
                                  newTileThreshold,
                                  move(memcachedServers),
                                  move(engines)};

    try {
        master = make_unique<LambdaMaster>(listenPort, clientPort, maxWorkers,
                                           rayGenerators, publicAddress.str(),
                                           storageBackendUri, region,
                                           move(scheduler), config);

        master->run();

        cerr << endl << timer().summary() << endl;
    } catch (const exception &e) {
        print_exception(argv[0], e);
        return EXIT_FAILURE;
    }

    return EXIT_SUCCESS;
}
