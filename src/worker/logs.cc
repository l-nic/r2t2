#include <sys/resource.h>
#include <sys/time.h>
#include <iomanip>

#include "lambda-worker.hh"
#include "messages/utils.hh"
#include "util/exception.hh"

using namespace std;
using namespace chrono;
using namespace r2t2;
using namespace pbrt;
using namespace meow;

using OpCode = Message::OpCode;

void LambdaWorker::send_worker_stats()
{
  CPUStats new_cpu_stats {};
  const auto diff = new_cpu_stats - cpu_stats;
  cpu_stats = new_cpu_stats;

  auto work_jiffies = diff.user + diff.nice + diff.system;
  auto total_jiffies = work_jiffies + diff.idle + diff.iowait + diff.irq
                       + diff.soft_irq + diff.steal + diff.guest
                       + diff.guest_nice;

  WorkerStats stats;
  stats.finishedPaths = finished_path_ids.size();
  stats.cpuUsage = 1.0 * work_jiffies / total_jiffies;

  protobuf::WorkerStats proto = to_protobuf( stats );
  master_connection.push_request(
    { *worker_id, OpCode::WorkerStats, protoutil::to_string( proto ) } );

  finished_path_ids = {};
}

void LambdaWorker::handle_worker_stats()
{
  worker_stats_timer.read_event();

  if ( !worker_id )
    return;

  send_worker_stats();
}

void LambdaWorker::upload_logs()
{
  if ( !worker_id )
    return;

  TLOG( RAYHOPS ) << local_stats.ray_hops.str();
  TLOG( SHADOWHOPS ) << local_stats.shadow_ray_hops.str();
  TLOG( PATHHOPS ) << local_stats.path_hops.str();

  google::FlushLogFiles( google::INFO );

  vector<storage::PutRequest> put_logs_request
    = { { info_log_name, log_prefix + to_string( *worker_id ) + ".INFO" } };

  storage_backend->put( put_logs_request );
}

string toCSVString(const Vector3f &ray) {
    ostringstream oss;

     oss << fixed << setprecision(3) << "[" << ray.x << ' '
        << ray.y << ' ' << ray.z << "]";
     return oss.str();
}
string toCSVString(const Point3f &point) {
    ostringstream oss;

     oss << fixed << setprecision(3) << "[" << point.x << ' '
        << point.y << ' ' << point.z << "]";
     return oss.str();
}
void LambdaWorker::log_ray( const RayAction action,
                            const RayState& state,
                            const RayBagInfo& info )
{
  if ( !track_rays || !state.trackRay || action == RayAction::Traced )
    return;

  ostringstream oss;
  /* timestamp,pathId,hop,shadowRay,remainingBounces,workerId,treeletId,
      ray origin,ray direction,action,bag */
  if (action ==  RayAction::Generated){
    oss << duration_cast<milliseconds>( system_clock::now().time_since_epoch() )
             .count()
        << ',' << state.sample.id << ',' << state.hop << ',' << state.isShadowRay
        << ',' << static_cast<int>( state.remainingBounces ) << ',' << *worker_id
        << ',' << state.CurrentTreelet() << ',' << toCSVString(state.ray.o) << ',' 
        << toCSVString(state.ray.d * state.ray.time) << ',';
  }
  else{
  /* timestamp,pathId,hop,shadowRay,remainingBounces,workerId,treeletId,
      action,bag */
      oss << duration_cast<milliseconds>( system_clock::now().time_since_epoch() )
               .count()
          << ',' << state.sample.id << ',' << state.hop << ',' << state.isShadowRay
          << ',' << static_cast<int>( state.remainingBounces ) << ',' << *worker_id
          << ',' << state.CurrentTreelet() << ',';
    }

  // clang-format off
    switch(action) {
    case RayAction::Generated: oss << "Generated,";                break;
    case RayAction::Traced:    oss << "Traced,";                   break;
    case RayAction::Queued:    oss << "Queued,";                   break;
    case RayAction::Bagged:    oss << "Bagged," << info.str("");   break;
    case RayAction::Unbagged:  oss << "Unbagged," << info.str(""); break;
    case RayAction::Finished:  oss << "Finished,";                 break;
    }
  // clang-format on

  TLOG( RAY ) << oss.str();
}

void LambdaWorker::log_bag( const BagAction action, const RayBagInfo& info )
{
  if ( !track_bags || !info.tracked )
    return;

  ostringstream oss;

  /* timestamp,bagTreeletId,bagWorkerId,bagId,thisWorkerId,count,size,action */
  oss << duration_cast<milliseconds>( system_clock::now().time_since_epoch() )
           .count()
      << ',' << info.treelet_id << ',' << info.worker_id << ',' << info.bag_id
      << ',' << *worker_id << ',' << info.ray_count << ',' << info.bag_size
      << ',';

  // clang-format off
    switch(action) {
    case BagAction::Created:   oss << "Created"; break;
    case BagAction::Sealed:    oss << "Sealed"; break;
    case BagAction::Submitted: oss << "Submitted"; break;
    case BagAction::Enqueued:  oss << "Enqueued"; break;
    case BagAction::Requested: oss << "Requested"; break;
    case BagAction::Dequeued:  oss << "Dequeued"; break;
    case BagAction::Opened:    oss << "Opened"; break;
    }
  // clang-format on

  TLOG( BAG ) << oss.str();
}
