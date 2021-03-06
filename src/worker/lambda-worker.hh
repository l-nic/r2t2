#pragma once

#include <pbrt/accelerators/cloudbvh.h>
#include <pbrt/core/geometry.h>
#include <pbrt/main.h>
#include <pbrt/raystate.h>

#include <atomic>
#include <cstring>
#include <fstream>
#include <future>
#include <iostream>
#include <optional>
#include <queue>
#include <random>
#include <string>
#include <thread>
#include <tuple>

#include "common/lambda.hh"
#include "common/stats.hh"
#include "master/lambda-master.hh"
#include "messages/message.hh"
#include "net/address.hh"
#include "net/s3.hh"
#include "net/transfer.hh"
#include "storage/backend_s3.hh"
#include "util/cpu.hh"
#include "util/eventfd.hh"
#include "util/eventloop.hh"
#include "util/histogram.hh"
#include "util/temp_dir.hh"
#include "util/timerfd.hh"
#include "util/units.hh"

#include "readerwriterqueue/readerwriterqueue.h"

#define TLOG( tag ) LOG( INFO ) << "[" #tag "] "

namespace r2t2 {

constexpr std::chrono::milliseconds SAMPLE_BAGS_INTERVAL { 1'000 };
constexpr std::chrono::milliseconds WORKER_STATS_INTERVAL { 1'000 };

constexpr size_t MAX_BAG_SIZE { 4 * 1024 * 1024 }; // 4 MiB

struct WorkerConfiguration
{
  int samples_per_pixel;
  int max_path_depth;
  std::chrono::milliseconds bagging_delay;

  float ray_log_rate;
  float bag_log_rate;

  std::vector<Address> memcached_servers;
};

/* Relationship between different queues in LambdaWorker:

                                  +------------+
                    +------------->   SAMPLE   +------------+
                    |             +------------+            |
                    |                                       |
                    |                                       |
               +---------+   rays   +-------+   rays   +----v---+
            +-->  TRACE  +---------->  OUT  +---------->  SEND  +--+
            |  +---------+          +-------+          +--------+  |
            |                                                      |
            |                                                      |
            |                                                      |
            |  ray bags                                  ray bags  |
            +---------------------+  network  <--------------------+
*/

class LambdaWorker
{
public:
  LambdaWorker( const std::string& coordinator_ip,
                const uint16_t coordinator_port,
                const std::string& storage_backend_uri,
                const WorkerConfiguration& config );

  void run();
  void terminate();
  void upload_logs();

private:
  using steady_clock = std::chrono::steady_clock;
  using rays_clock = std::chrono::system_clock;

  ////////////////////////////////////////////////////////////////////////////
  // Job Information                                                        //
  ////////////////////////////////////////////////////////////////////////////

  const WorkerConfiguration config;
  const UniqueDirectory working_directory;
  std::optional<WorkerId> worker_id {};
  std::optional<std::string> job_id {};
  bool terminated { false };

  ////////////////////////////////////////////////////////////////////////////
  // Graphics                                                               //
  ////////////////////////////////////////////////////////////////////////////

  /*** Scene Information ****************************************************/

  bool scene_loaded { false };
  std::vector<std::pair<TreeletId, std::string>> downloaded_treelets {};
  std::map<uint64_t, SceneObject> pending_scene_objects {};

  struct SceneData
  {
  public:
    pbrt::scene::Base base {};

    int samples_per_pixel { 1 };
    uint8_t max_depth { 5 };

    SceneData() {}
  } scene {};

  /*** Ray Tracing **********************************************************/

  /* ray-tracing thread runs this function, reads from trace_queue and writes
     to processed_queue */
  void handle_trace_queue();

  void handle_processed_queue();

  void generate_rays( const pbrt::Bounds2i& crop_window );

  std::thread raytracing_thread {};
  EventFD rays_ready_fd {};

  moodycamel::BlockingReaderWriterQueue<pbrt::RayStatePtr> trace_queue { 8192 };
  moodycamel::ReaderWriterQueue<pbrt::RayStatePtr> processed_queue { 8192 };

  std::atomic<bool> trace_queue_empty { true };
  std::atomic<bool> processed_queue_empty { true };

  std::map<TreeletId, std::shared_ptr<pbrt::CloudBVH>> treelets {};
  std::map<TreeletId, std::queue<pbrt::RayStatePtr>> out_queue {};
  std::queue<pbrt::Sample> samples {};
  size_t out_queue_size { 0 };

  ////////////////////////////////////////////////////////////////////////////
  // Communication                                                          //
  ////////////////////////////////////////////////////////////////////////////

  /* the coordinator and storage backend */

  const Address coordinator_addr;
  meow::Client<TCPSession> master_connection;
  Storage storage_backend_info;
  S3StorageBackend scene_storage_backend;
  S3StorageBackend job_storage_backend;

  /* messages that can be processed only when the scene is fully loaded */
  std::queue<meow::Message> pending_messages {};

  /* processes incoming messages; called by handle_messages */
  void process_message( const meow::Message& message );

  /* downloads the necessary scene objects */
  void get_and_setup_scene( const protobuf::GetObjects& objects );

  /* handle messages that are queued for when the scene is loaded */
  void handle_pending_messages();

  /* process rays supposed to be sent out */
  void handle_out_queue();

  /* sending the rays out */
  void handle_open_bags();

  /* sending the rays out */
  void handle_sealed_bags();

  /* opening up received ray bags */
  void handle_receive_queue();

  /* turning samples into sample bags */
  void handle_samples();

  /* sending sample bags out */
  void handle_sample_bags();

  void handle_transfer_results( const bool sample_bags );

  void handle_scene_object_results();

  /* queues */

  /* current bag for each treelet */
  std::map<TreeletId, RayBag> open_bags {};

  /* bags that are sealed and ready to be sent out */
  std::queue<RayBag> sealed_bags {};

  /* sample bags ready to be sent out */
  std::queue<RayBag> sample_bags {};

  /* ray bags that are received, but not yet unpacked */
  std::queue<RayBag> receive_queue {};

  /* id of the paths that are finished (for bookkeeping) */
  std::queue<uint64_t> finished_path_ids {};

  /*** Ray Bags *************************************************************/

  enum class Task
  {
    Download,
    Upload
  };

  std::string ray_bags_key_prefix {};
  std::map<TreeletId, BagId> current_bag_id {};
  std::map<uint64_t, std::pair<Task, RayBagInfo>> pending_ray_bags {};
  std::map<uint64_t, std::pair<Task, RayBagInfo>> pending_sample_bags {};
  BagId current_sample_bag_id { 0 };

  /*** Transfer Agent *******************************************************/

  std::unique_ptr<TransferAgent> transfer_agent;
  std::unique_ptr<TransferAgent> samples_transfer_agent;
  std::unique_ptr<TransferAgent> scene_transfer_agent;

  ////////////////////////////////////////////////////////////////////////////
  // Stats                                                                  //
  ////////////////////////////////////////////////////////////////////////////

  void handle_worker_stats();
  void send_worker_stats();

  struct
  {
    uint64_t generated { 0 };
    std::atomic<uint64_t> terminated { 0 };
  } rays {};

  ////////////////////////////////////////////////////////////////////////////
  // Logging                                                                //
  ////////////////////////////////////////////////////////////////////////////

  enum class RayAction
  {
    Generated,
    Traced,
    Queued,
    Bagged,
    Unbagged,
    Finished
  };

  enum class BagAction
  {
    Created,
    Sealed,
    Submitted,
    Enqueued,
    Requested,
    Dequeued,
    Opened
  };

  void log_ray( const RayAction action,
                const pbrt::RayState& state,
                const RayBagInfo& info = RayBagInfo::EmptyBag() );

  void log_bag( const BagAction action, const RayBagInfo& info );

  const std::string log_base { "r2t2-worker" };
  const std::string info_log_name { log_base + ".INFO" };
  std::string log_prefix { "logs/" };
  const bool track_rays { config.ray_log_rate > 0 };
  const bool track_bags { config.bag_log_rate > 0 };

  std::bernoulli_distribution coin { 0.5 };
  std::mt19937 rand_engine { std::random_device {}() };

  const steady_clock::time_point work_start { steady_clock::now() };

  ////////////////////////////////////////////////////////////////////////////
  // Other ℭ𝔯𝔞𝔭
  ////////////////////////////////////////////////////////////////////////////

  EventLoop loop {};
  std::optional<EventLoop::RuleHandle> finish_up_rule {};
  meow::Client<TCPSession>::RuleCategories worker_rule_categories;

  /* Timers */
  TimerFD seal_bags_timer {};
  TimerFD sample_bags_timer { SAMPLE_BAGS_INTERVAL };
  TimerFD worker_stats_timer { WORKER_STATS_INTERVAL };

  ////////////////////////////////////////////////////////////////////////////
  // Local Stats                                                            //
  ////////////////////////////////////////////////////////////////////////////

  CPUStats cpu_stats {};
};

} // namespace r2t2
