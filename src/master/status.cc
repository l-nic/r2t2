#include <iomanip>

#include "lambda-master.hh"
#include "messages/utils.hh"
#include "util/status_bar.hh"

using namespace std;
using namespace std::chrono;
using namespace r2t2;

constexpr milliseconds EXIT_GRACE_PERIOD { 5'000 };

void LambdaMaster::handle_status_message()
{
  status_print_timer.read_event();

  const auto now = steady_clock::now();

  if ( config.timeout.count() && now - last_action_time >= config.timeout ) {
    cerr << "Job terminated due to inactivity." << endl;
    return;
  } else if ( not job_timeout_timer.armed()
              and scene.total_paths == aggregated_stats.finishedPaths ) {
    cerr << "Done! Terminating the job in "
         << duration_cast<seconds>( EXIT_GRACE_PERIOD ).count() << "s..."
         << endl;

    job_timeout_timer.set( 0s, EXIT_GRACE_PERIOD );

    loop.add_rule(
      "terminate job",
      job_timeout_timer,
      Direction::In,
      [this] {
        job_timeout_timer.read_event();
        terminate();
      },
      [] { return true; } );
  }

  const auto lagging_workers
    = count_if( workers.begin(), workers.end(), [&now]( const auto& worker ) {
        return ( worker.state != Worker::State::Terminated )
               && ( now - worker.last_seen >= seconds { 4 } );
      } );

  const auto elapsed_seconds
    = duration_cast<seconds>( now - start_time ).count();

  auto percent = []( const uint64_t n, const uint64_t total ) -> double {
    return total ? ( ( ( uint64_t )( 100 * ( 100.0 * n / total ) ) ) / 100.0 )
                 : 0.0;
  };

  auto BG = []( const bool reset = false ) -> char const* {
    constexpr char const* BG_A = "\033[48;5;022m";
    constexpr char const* BG_B = "\033[48;5;028m";

    static bool alternate = true;
    alternate = reset ? false : !alternate;

    return alternate ? BG_B : BG_A;
  };

  auto& s = aggregated_stats;

  // clang-format off
  ostringstream oss;
  oss << "\033[0m" << fixed << setprecision(2)

      // finished paths
      << BG(true) << " \u21af " << s.finishedPaths
      << " (" << percent(s.finishedPaths, scene.total_paths) << "%) "

      << BG() << " \u21a6 " << Worker::active_count[Worker::Role::Generator]
              << "/" << ray_generators << " "

      << BG() << " \u03bb " << Worker::active_count[Worker::Role::Tracer]
              << "/" << max_workers << " "

      << BG() << " \u29d6 " << treelets_to_spawn.size() << " "

      // lagging workers
      << BG() << " \u203c " << lagging_workers << " "

      // enqueued bytes
      << BG() << " \u2191 " << format_bytes(s.enqueued.bytes) << " "

      // assigned bytes
      << BG() << " \u21ba " << percent(s.assigned.bytes - s.dequeued.bytes,
                                        s.enqueued.bytes) << "% "

      // dequeued bytes
      << BG() << " \u2193 " << percent(s.dequeued.bytes, s.enqueued.bytes)
              << "% "

      // elapsed time
      << BG() << " " << setfill('0')
              << setw(2) << (elapsed_seconds / 60) << ":" << setw(2)
              << (elapsed_seconds % 60) << " "

      << BG();
  // clang-format on

  StatusBar::set_text( oss.str() );
}
