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

#include <pbrt/accelerators/cloudbvh.h>
#include <pbrt/core/geometry.h>
#include <pbrt/main.h>
#include <pbrt/raystate.h>

#define UNUSED(expr) do { (void)(expr); } while (0)

#define ERROR_MSG_ID 0 // Error condition, should never occur. Set to zero since this is likely in an improperly configured message.
#define RAY_MSG_LOAD_ID 1 // Switches to initiators
#define RAY_MSG_ID 2 // Regular rays in transit

#define SERVER_IP_ADDR "127.0.0.1"
#define SERVER_PORT 5000


using namespace std;

int _ray_generator_fd = -1;
uint32_t _treelet_id = -1;

/* This is a simple ray tracer, built using the api provided by r2t2's fork of
pbrt. */

void usage( char* argv0 )
{
  cerr << argv0 << " SCENE-PATH [SAMPLES-PER-PIXEL] TREELET_ID" << endl;
}

int start_client_socket(const char* ip_address, uint16_t port) {
    int sockfd = socket(AF_INET, SOCK_STREAM, 0);
    if (sockfd < 0) {
        return -1;
    }
    struct in_addr network_order_address;
    int ip_conversion_retval = inet_aton(ip_address, &network_order_address);
    if (ip_conversion_retval < 0) {
        return -1;
    }
    struct sockaddr_in addr;
    addr.sin_family = AF_INET;
    addr.sin_port = htons(port);
    addr.sin_addr = network_order_address;
    int connect_retval = connect(sockfd, (struct sockaddr*)&addr, sizeof(addr));
    if (connect_retval < 0) {
        return -1;
    }
    ssize_t written_len = write(sockfd, &_treelet_id, sizeof(uint32_t));
    if (written_len <= 0) {
        return -1;
    }
    return sockfd;
}

int read_from_generator(char* buffer, uint32_t* data_len) {
    printf("Reading from generator\n");
    uint32_t header_buf[2];
    ssize_t total_len = 0;
    ssize_t actual_len = 0;
    do {
        actual_len = read(_ray_generator_fd, (char*)&header_buf + total_len, 2*sizeof(uint32_t) - total_len);
        total_len += actual_len;
        if (actual_len <= 0) {
            return -1;
        }
    } while (total_len < 2*sizeof(uint32_t));
    printf("Got id %d and message size %d\n", header_buf[0], header_buf[1]);
    if (header_buf[0] != RAY_MSG_LOAD_ID) {
        return -1;
    }

    total_len = 0;
    do {
        actual_len = read(_ray_generator_fd, buffer + total_len, header_buf[1] - total_len);
        total_len += actual_len;
        printf("Read %d bytes\n", actual_len);
        if (actual_len <= 0) {
            return -1;
        }
    } while (total_len < header_buf[1]);
    *data_len = total_len;
    return 0;
}

int main( int argc, char* argv[] )
{
  if ( argc <= 0 ) {
    abort();
  }

  if ( argc < 3 ) {
    usage( argv[0] );
    return EXIT_FAILURE;
  }

  const size_t max_depth = 5;
  const string scene_path { argv[1] };
  const size_t samples_per_pixel = ( argc > 2 ) ? stoull( argv[2] ) : 0;
  _treelet_id = stoull(argv[3]);

  /* (1) loading the scene */
  auto scene_base = pbrt::scene::LoadBase( scene_path, samples_per_pixel );

  /* (2) loading all the treelets */
  vector<shared_ptr<pbrt::CloudBVH>> treelets;

  for ( size_t i = 0; i < scene_base.GetTreeletCount(); i++ ) {
    treelets.push_back( pbrt::scene::LoadTreelet( scene_path, i ) );
  }

  _ray_generator_fd = start_client_socket(SERVER_IP_ADDR, SERVER_PORT);
  if (_ray_generator_fd < 0) {
      return -1;
  }

  /* (3) generating all the initial rays */
  queue<pbrt::RayStatePtr> ray_queue;

  for ( const auto pixel : scene_base.sampleBounds ) {
    for ( int sample = 0; sample < scene_base.samplesPerPixel; sample++ ) {
      char* buffer = new char[pbrt::RayState::MaxPackedSize];
      uint32_t data_len;
      int read_retval = read_from_generator(buffer, &data_len);
      if (read_retval < 0) {
          return -1;
      }
      pbrt::RayStatePtr current_ray(new pbrt::RayState());
      current_ray->Deserialize(buffer, data_len);
      delete [] buffer;
      ray_queue.push(move(current_ray));
    }
  }

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
