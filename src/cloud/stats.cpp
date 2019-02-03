#include "stats.h"

#include <math.h>


using namespace::std::chrono;

namespace pbrt {
namespace global {
WorkerStats workerStats;
}

void RayStats::reset() {
    sentRays = 0;
    receivedRays = 0;
    waitingRays = 0;
    processedRays = 0;
    for (double& d : traceDurationPercentiles) {
        d = 0;
    }

#ifdef PER_RAY_STATS
    rayDurations.clear();
#endif  // PER_RAY_STATS
}

void RayStats::merge(const RayStats& other) {
    sentRays += other.sentRays;
    receivedRays += other.receivedRays;
    waitingRays += other.waitingRays;
    processedRays += other.processedRays;

    for (int i = 0; i < NUM_PERCENTILES; ++i) {
        traceDurationPercentiles[i] += other.traceDurationPercentiles[i];
    }
#ifdef PER_RAY_STATS
    rayDurations.insert(rayDurations.end(), other.rayDurations.begin(),
                        other.rayDurations.end());
#endif  // PER_RAY_STATS
}

#define INCREMENT_FIELD(name__)          \
    do {                                 \
        aggregateStats.name__ += 1;    \
        objectStats[type].name__ += 1; \
    } while (false)

void WorkerStats::recordFinishedPath() { _finishedPaths += 1; }

void WorkerStats::recordSentRay(const SceneManager::ObjectKey& type) {
    INCREMENT_FIELD(sentRays);
}

void WorkerStats::recordReceivedRay(const SceneManager::ObjectKey& type) {
    INCREMENT_FIELD(receivedRays);
}
void WorkerStats::recordWaitingRay(const SceneManager::ObjectKey& type) {
    INCREMENT_FIELD(waitingRays);
}
void WorkerStats::recordProcessedRay(const SceneManager::ObjectKey& type) {
    INCREMENT_FIELD(processedRays);
}

#undef INCREMENT_FIELD

void WorkerStats::recordRayInterval(const SceneManager::ObjectKey& type,
                                    timepoint_t start, timepoint_t end) {
    auto total_time =
        std::chrono::duration_cast<std::chrono::nanoseconds>((end - start))
            .count();
    aggregateStats.rayDurations.push_back(total_time);
#ifdef PER_RAY_STATS
    objectStats[type].rayDurations.push_back(total_time);
#endif
}

void WorkerStats::reset() {
    _finishedPaths = 0;
    aggregateStats.reset();
    objectStats.clear();
    timePerAction.clear();
    intervalStart = now();
    intervalsPerAction.clear();
    intervalStart = now();
}

void WorkerStats::merge(const WorkerStats& other) {
    _finishedPaths += other._finishedPaths;
    aggregateStats.merge(other.aggregateStats);
    queueStats = other.queueStats;
    for (const auto& kv : other.objectStats) {
        objectStats[kv.first].merge(kv.second);
    }
    for (const auto& kv : other.timePerAction) {
        timePerAction[kv.first] += kv.second;
    }
    for (const auto& kv : other.intervalsPerAction) {
        intervalsPerAction[kv.first].insert(intervalsPerAction[kv.first].end(),
                                            kv.second.begin(), kv.second.end());
    }
}

WorkerStats::Recorder::~Recorder() {
    auto end = now();
    stats.timePerAction[name] +=
        std::chrono::duration_cast<std::chrono::nanoseconds>((end - start))
            .count();
#ifdef PER_INTERVAL_STATS
    stats.intervalsPerAction[name].push_back(std::make_tuple(
        std::chrono::duration_cast<std::chrono::nanoseconds>(
            (start - stats.start)).count(),
        std::chrono::duration_cast<std::chrono::nanoseconds>(
            (end - stats.start)).count()));
#endif
}

WorkerStats::Recorder::Recorder(WorkerStats& stats_, const std::string& name_)
    : stats(stats_), name(name_) {
    start = now();
}

ExponentialMovingAverage::ExponentialMovingAverage(
    high_resolution_clock::duration period)
    : period(period), empty(true) {
    // lastValue, lastTime, and average will all be ignored since `empty` is
    // true.
}

double ExponentialMovingAverage::updateNow( double value ) {
  return update( value, high_resolution_clock::now() );
}

double ExponentialMovingAverage::update( double value, high_resolution_clock::time_point time ) {
  if ( empty ) {
    empty = false;
    average = value;
  } else {
    high_resolution_clock::duration deltaT = time - lastTime;
    double w1 = exp( - deltaT / period );
    double w2 = ( 1.0 - w1 ) * period / deltaT;
    average = w1 * average + ( 1.0 - w2 ) * value + ( w2 - w1 ) * lastValue;
  }
  lastTime = time;
  lastValue = value;
  return average;
}

}  // namespace pbrt
