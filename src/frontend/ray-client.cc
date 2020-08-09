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
#define SAMPLE_MSG_ID 3 // Samples returning back to the root

// Message format should be: Treelet dest (or root), message type, message size, message
// Messages that are read don't need to read out the dest field.
// Initial bringup messages will be different, and will just consist of the treelet source.
// All writes from treelets go to the root
// All reads from treelets come from the root, and are at the beginning of the loop instead of the ray_queue dequeue
// The root will start off by writing out initial rays, and will then enter an event loop where it receives all incoming messages,
// processes them if they're intended for it, or forwards them back out to the actual destination if they're intended for another treelet.


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
    if (header_buf[0] != RAY_MSG_LOAD_ID && header_buf[0] != RAY_MSG_ID) {
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

void write_to_generator(char* buffer, uint32_t data_len) {
    ssize_t written_len = write(_ray_generator_fd, buffer, data_len);
    if (written_len <= 0) {
        cerr << "Error writing to treelet" << endl;
    }
}

void send_ray(pbrt::RayStatePtr ray) {
    uint64_t packed_ray_size = pbrt::RayState::MaxPackedSize + 3*sizeof(uint32_t);
    char* ray_buffer = new char[packed_ray_size];
    uint32_t ray_treelet_dest = ray->CurrentTreelet();
    printf("sending to treelet %d\n", ray_treelet_dest);
    uint32_t ray_msg_load_id = RAY_MSG_ID;
    memcpy(ray_buffer, &ray_treelet_dest, sizeof(uint32_t));
    memcpy(ray_buffer + sizeof(uint32_t), &ray_msg_load_id, sizeof(uint32_t));
    uint64_t actual_len = ray->Serialize(ray_buffer + 2*sizeof(uint32_t));
    write_to_generator(ray_buffer, actual_len + 2*sizeof(uint32_t));
    delete [] ray_buffer;
    printf("Sent to treelet\n");
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
//  queue<pbrt::RayStatePtr> ray_queue;

//     if (_treelet_id == 0) {
//   for ( const auto pixel : scene_base.sampleBounds ) {
//     for ( int sample = 0; sample < scene_base.samplesPerPixel; sample++ ) {
//       char* buffer = new char[pbrt::RayState::MaxPackedSize];
//       uint32_t data_len;
//       int read_retval = read_from_generator(buffer, &data_len);
//       if (read_retval < 0) {
//           return -1;
//       }
//       pbrt::RayStatePtr current_ray(new pbrt::RayState());
//       current_ray->Deserialize(buffer, data_len);
//       delete [] buffer;
//       ray_queue.push(move(current_ray));
//     }
//   }
// }

  /* (4) tracing rays to completing */
  vector<pbrt::Sample> samples;

  pbrt::MemoryArena arena;

  while (true) {
    pbrt::RayStatePtr ray(new pbrt::RayState());
    // if (!ray_queue.empty()) {
    //     ray = move(ray_queue.front());
    //     ray_queue.pop();
    // } else {
        char* buffer = new char[pbrt::RayState::MaxPackedSize];
        uint32_t data_len;
        printf("Reading from generator\n");
        int read_retval = read_from_generator(buffer, &data_len);
        if (read_retval < 0) {
        return -1;
        }
        ray->Deserialize(buffer, data_len);
        delete [] buffer;
   // }

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
          send_ray(move(new_ray)); // back to the ray queue
        }
      } else if ( not empty_visit or hit ) {
          send_ray(move(new_ray)); // back to the ray queue
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
        send_ray( move( bounce_ray ) ); // back to the ray queue
      }

      if ( shadow_ray != nullptr ) {
        send_ray( move( shadow_ray ) ); // back to the ray queue
      }
    }
  }

  /* (5) accumulating the samples and producing the final output */
  pbrt::graphics::AccumulateImage( scene_base.camera, samples );
  pbrt::graphics::WriteImage( scene_base.camera );

  return EXIT_SUCCESS;
}
