#include <getopt.h>
#include <iostream>
#include <memory>
#include <queue>
#include <string>
#include <unistd.h>
#include <sys/socket.h>
#include <netinet/ip.h>
#include <arpa/inet.h>
#include <sys/types.h>
#include <map>
#include <thread>
#include <mutex>

#include <pbrt/accelerators/cloudbvh.h>
#include <pbrt/core/geometry.h>
#include <pbrt/main.h>
#include <pbrt/raystate.h>

#define UNUSED(expr) do { (void)(expr); } while (0)

#define ERROR_MSG_ID 0 // Error condition, should never occur. Set to zero since this is likely in an improperly configured message.
#define RAY_MSG_LOAD_ID 1 // Switches to initiators
#define RAY_MSG_ID 2 // Regular rays in transit
#define SAMPLE_MSG_ID 3 // Samples returning back to the root

#define SERVER_PORT 5000
const int CONN_BACKLOG = 32;


using namespace std;

int _server_fd = -1;
map<uint32_t, int> _treelet_handles;
uint32_t _num_treelets = 0;
map<uint32_t, mutex> _send_mutex;
bool _hold_switch_threads = true; // TODO: This should really be a condition variable

/* This is a simple ray tracer, built using the api provided by r2t2's fork of
pbrt. */

void usage( char* argv0 )
{
  cerr << argv0 << " SCENE-PATH [SAMPLES-PER-PIXEL]" << endl;
}

int start_server_socket(uint16_t port) {
  int sockfd = socket(AF_INET, SOCK_STREAM, 0);
  if (sockfd < 0) {
    cerr << "Server socket creation failed." << endl;
    return -1;
  }
  int sockopt_enable = 1;
  int setopt_retval = setsockopt(sockfd, SOL_SOCKET, SO_REUSEADDR, &sockopt_enable, sizeof(int));
  if (setopt_retval < 0) {
    cerr << "Server socket bind bind failure." << endl;
    return -1;
  }
  struct sockaddr_in addr;
  addr.sin_family = AF_INET;
  addr.sin_port = htons(port);
  addr.sin_addr.s_addr = htonl(INADDR_ANY);
  int bind_retval = bind(sockfd, (struct sockaddr*)&addr, sizeof(addr));
  if (bind_retval < 0) {
    cerr << "Server socket bind failure." << endl;
    return -1;
  }
  int listen_retval = listen(sockfd, CONN_BACKLOG);
  if (listen_retval < 0) {
    cerr << "Server socket listen failure." << endl;
    return -1;
  }
  return sockfd;
}

int accept_client_connections(uint32_t num_connections) {
  for (uint32_t i = 0; i < num_connections; i++) {
    struct sockaddr_in addr;
    socklen_t addr_len = sizeof(addr);
    printf("Waiting for client connection %d of %d...\n", i, num_connections);
    int connectionfd = accept(_server_fd, (struct sockaddr*)&addr, &addr_len);
    if (connectionfd < 0) {
      cerr << "Server connection accept attempt " << i << " failed." << endl;
      return -1;
    }
    if (addr.sin_family != AF_INET) {
      cerr << "Server accepted non-internet connection on iteration " << i << endl;
      return -1;
    }
    uint32_t treelet = 0;
    ssize_t actual_len = read(connectionfd, &treelet, sizeof(uint32_t));
    if (actual_len <= 0) {
      cerr << "Unable to read treelet id from new connection " << i << endl;
      return -1;
    }
    if (treelet >= num_connections) {
      fprintf(stderr, "Treelet id %d received on iteration %d is >= max number of connections %d\n", treelet, i, num_connections);
    }
    printf("Got treelet id %d\n", treelet);
    _treelet_handles[treelet] = connectionfd;
  }
  return 0;
}

void send_to_client(uint32_t current_treelet, char* ray_buffer, uint64_t actual_len) {
  _send_mutex.at(current_treelet).lock();
  int write_fd = _treelet_handles[current_treelet];
  ssize_t written_len = write(write_fd, ray_buffer, actual_len);
  if (written_len <= 0) {
    cerr << "Error writing to treelet " << current_treelet << endl;
  }
  _send_mutex.at(current_treelet).unlock();
}

void handle_client_reads(uint32_t treelet) {
  int treelet_fd = _treelet_handles[treelet];
  while (true) {
    // Wait for messages to arrive from a client
    uint32_t header_buf[3];
    ssize_t total_len = 0;
    ssize_t actual_len = 0;
    do {
        actual_len = read(treelet_fd, (char*)&header_buf + total_len, 3*sizeof(uint32_t) - total_len);
        total_len += actual_len;
        if (actual_len <= 0) {
            fprintf(stderr, "Read error for treelet %d\n", treelet);
            return;
        }
    } while (total_len < 3*sizeof(uint32_t));
    if (header_buf[0] > _num_treelets) {
      fprintf(stderr, "Invalid treelet id %d\n", header_buf[0]);
      return;
    }
    char* buffer = new char[header_buf[2] + 2*sizeof(uint32_t)];
    memcpy(buffer, &header_buf[1], sizeof(uint32_t));
    memcpy(buffer + sizeof(uint32_t), &header_buf[2], sizeof(uint32_t));
    total_len = 0;
    do {
        actual_len = read(treelet_fd, buffer + 2*sizeof(uint32_t) + total_len, header_buf[2] - total_len);
        total_len += actual_len;
        if (actual_len <= 0) {
            fprintf(stderr, "Read error for treelet %d\n", treelet);
            return;
        }
    } while (total_len < header_buf[2]);
    uint32_t data_len = total_len;

    if (header_buf[0] < _num_treelets) {
      // Forward to a treelet
      send_to_client(header_buf[0], buffer, header_buf[2] + 2*sizeof(uint32_t));
    } else if (header_buf[0] == _num_treelets) {
      // Switch should process this one. Right now the switch only understands samples.
      if (header_buf[1] == SAMPLE_MSG_ID) {
        printf("Switch received sample message\n");
      } else {
        fprintf(stderr, "Switch received unknown message %d\n", header_buf[1]);
        return;
      }
    } else {
      fprintf(stderr, "Invalid destination selected.\n");
      return;
    }
    delete [] buffer;
  }
}

int main( int argc, char* argv[] )
{
  if ( argc <= 0 ) {
    abort();
  }

  if ( argc < 2 ) {
    usage( argv[0] );
    return EXIT_FAILURE;
  }

  const size_t max_depth = 5;
  const string scene_path { argv[1] };
  const size_t samples_per_pixel = ( argc > 2 ) ? stoull( argv[2] ) : 0;

  /* (1) loading the scene */
  auto scene_base = pbrt::scene::LoadBase( scene_path, samples_per_pixel );

  /* (2) loading all the treelets */
  vector<shared_ptr<pbrt::CloudBVH>> treelets;

  for ( size_t i = 0; i < scene_base.GetTreeletCount(); i++ ) {
    treelets.push_back( pbrt::scene::LoadTreelet( scene_path, i ) );
  }

  _server_fd = start_server_socket(SERVER_PORT);
  if (_server_fd < 0) {
    return -1;
  }
  int client_connections_retval = accept_client_connections(scene_base.GetTreeletCount());
  if (client_connections_retval < 0) {
    return -1;
  }

  /* (3) generating all the initial rays */
  queue<pbrt::RayStatePtr> ray_queue;

    _num_treelets = scene_base.GetTreeletCount();

    vector<thread> all_threads;
    for ( size_t i = 0; i < scene_base.GetTreeletCount(); i++ ) {
      _send_mutex[i].lock();
      _send_mutex[i].unlock();
      all_threads.emplace_back(move(thread(handle_client_reads, i)));
    }
  int total_ray_count = 0;
  for (const auto pixel : scene_base.sampleBounds) {
    for (int sample = 0; sample < scene_base.samplesPerPixel; sample++) {
      total_ray_count++;
    }
  }
  printf("Total ray count is %d\n", total_ray_count);

  for ( const auto pixel : scene_base.sampleBounds ) {
    for ( int sample = 0; sample < scene_base.samplesPerPixel; sample++ ) {
      pbrt::RayStatePtr current_ray = pbrt::graphics::GenerateCameraRay( scene_base.camera,
                                           pixel,
                                           sample,
                                           max_depth,
                                           scene_base.sampleExtent,
                                           scene_base.sampler );
      uint64_t packed_ray_size = pbrt::RayState::MaxPackedSize + 2*sizeof(uint32_t);
      char* ray_buffer = new char[packed_ray_size];
      uint32_t ray_msg_load_id = RAY_MSG_LOAD_ID;
      memcpy(ray_buffer, &ray_msg_load_id, sizeof(uint32_t));
      uint64_t actual_len = current_ray->Serialize(ray_buffer + sizeof(uint32_t));
      usleep(1000);
      send_to_client(current_ray->CurrentTreelet(), ray_buffer, actual_len + sizeof(uint32_t));
      delete [] ray_buffer;
    }
  }
  printf("sent all samples\n");
  _hold_switch_threads = false;

    for (auto& t : all_threads) {
      t.join();
    }
  


  return 0;

  /* (4) tracing rays to completing */
  vector<pbrt::Sample> samples;

  pbrt::MemoryArena arena;

  while ( not ray_queue.empty() ) {
    pbrt::RayStatePtr ray = move( ray_queue.front() );
    ray_queue.pop();
    for (int i = 0; i < ray->toVisitHead; i++) {
      if (i == 0 && ray->toVisit[i].treelet == 0 && ray->toVisit[i].node == 0 && ray->toVisit[i].primitive == 0) {
        continue;
      }
      if (ray->toVisit[i].node == 0) {
        continue;
      }
      printf("index %d has treelet id %d node id %d primitive %d\n", i, ray->toVisit[i].treelet, ray->toVisit[i].node, ray->toVisit[i].primitive);
    }

    auto& treelet = treelets[ray->CurrentTreelet()];

    /* This is the ray tracing core logic; calling one of TraceRay or ShadeRay
    functions, based on the state of the `ray`, and putting the result back
    into the ray_queue for further processing, or into samples when they are
    done. */
    if ( not ray->toVisitEmpty() ) {
      auto new_ray = pbrt::graphics::TraceRay( move( ray ), *treelet );

      const bool hit = new_ray->HasHit();
      const bool empty_visit = new_ray->toVisitEmpty();

      if ( new_ray->IsShadowRay() ) {
        if ( hit or empty_visit ) {
          new_ray->Ld = hit ? 0.f : new_ray->Ld;
          samples.emplace_back( *new_ray ); // this ray is done
        } else {
          ray_queue.push( move( new_ray ) ); // back to the ray queue
        }
      } else if ( not empty_visit or hit ) {
        ray_queue.push( move( new_ray ) ); // back to the ray queue
      } else if ( empty_visit ) {
        new_ray->Ld = 0.f;
        samples.emplace_back( *new_ray ); // this ray is done
      }
    } else if ( ray->HasHit() ) {
      auto [bounce_ray, shadow_ray]
        = pbrt::graphics::ShadeRay( move( ray ),
                                    *treelet,
                                    scene_base.lights,
                                    scene_base.sampleExtent,
                                    scene_base.sampler,
                                    max_depth,
                                    arena );

      if ( bounce_ray != nullptr ) {
        ray_queue.push( move( bounce_ray ) ); // back to the ray queue
      }

      if ( shadow_ray != nullptr ) {
        ray_queue.push( move( shadow_ray ) ); // back to the ray queue
      }
    }
  }

  /* (5) accumulating the samples and producing the final output */
  pbrt::graphics::AccumulateImage( scene_base.camera, samples );
  pbrt::graphics::WriteImage( scene_base.camera );

  return EXIT_SUCCESS;
}
