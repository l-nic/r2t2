#include <getopt.h>
#include <iostream>
#include <memory>
#include <queue>
#include <string>
#include <unistd.h>
//#include <sys/socket.h>
//#include <netinet/ip.h>
//#include <arpa/inet.h>
//#include <sys/types.h>
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
#define TREELET_MSG_LOAD_ID 4 // Treelets being loaded from the switch.
#define BASE_MSG_LOAD_ID 5 // Base data being loaded from the switch.

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
uint32_t _num_treelets = -1;

// CSR modification macros
#define csr_read(csr)           \
({                \
  unsigned long __v;       \
  __asm__ __volatile__ ("csrr %0, " #csr      \
            : "=r" (__v) :      \
            : "memory");      \
  __v;              \
})

#define csr_write(csr, val)         \
({                \
  unsigned long __v = (unsigned long)(val);   \
  __asm__ __volatile__ ("csrw " #csr ", %0"   \
            : : "rK" (__v)      \
            : "memory");      \
})


/* This is a simple ray tracer, built using the api provided by r2t2's fork of
pbrt. */

void usage( char* argv0 )
{
  cerr << argv0 << " SCENE-PATH [SAMPLES-PER-PIXEL] TREELET_ID NUM_TREELETS" << endl;
}

int start_client_socket(const char* ip_address, uint16_t port) {
    // int sockfd = socket(AF_INET, SOCK_STREAM, 0);
    // if (sockfd < 0) {
    //     return -1;
    // }
    // struct in_addr network_order_address;
    // int ip_conversion_retval = inet_aton(ip_address, &network_order_address);
    // if (ip_conversion_retval < 0) {
    //     return -1;
    // }
    // struct sockaddr_in addr;
    // addr.sin_family = AF_INET;
    // addr.sin_port = htons(port);
    // addr.sin_addr = network_order_address;
    // int connect_retval = connect(sockfd, (struct sockaddr*)&addr, sizeof(addr));
    // if (connect_retval < 0) {
    //     return -1;
    // }
    // ssize_t written_len = write(sockfd, &_treelet_id, sizeof(uint32_t));
    // if (written_len <= 0) {
    //     return -1;
    // }
    // return sockfd;
    return -1;
}

int read_from_generator(char* buffer, uint32_t* data_len) {
    // uint32_t header_buf[2];
    // ssize_t total_len = 0;
    // ssize_t actual_len = 0;
    // do {
    //     actual_len = read(_ray_generator_fd, (char*)&header_buf + total_len, 2*sizeof(uint32_t) - total_len);
    //     total_len += actual_len;
    //     if (actual_len <= 0) {
    //         return -1;
    //     }
    // } while (total_len < 2*sizeof(uint32_t));
    // if (header_buf[0] != RAY_MSG_LOAD_ID && header_buf[0] != RAY_MSG_ID) {
    //     return -1;
    // }

    // total_len = 0;
    // do {
    //     actual_len = read(_ray_generator_fd, buffer + total_len, header_buf[1] - total_len);
    //     total_len += actual_len;
    //     if (actual_len <= 0) {
    //         return -1;
    //     }
    // } while (total_len < header_buf[1]);
    // *data_len = total_len;
    // return 0;
}

void write_to_generator(char* buffer, uint32_t data_len) {
    // ssize_t written_len = write(_ray_generator_fd, buffer, data_len);
    // if (written_len <= 0) {
    //     cerr << "Error writing to treelet" << endl;
    // }
}

void send_ray(pbrt::RayStatePtr ray) {
    uint64_t packed_ray_size = pbrt::RayState::MaxPackedSize + 3*sizeof(uint32_t);
    char* ray_buffer = new char[packed_ray_size];
    uint32_t ray_treelet_dest = ray->CurrentTreelet();
    uint32_t ray_msg_load_id = RAY_MSG_ID;
    memcpy(ray_buffer, &ray_treelet_dest, sizeof(uint32_t));
    memcpy(ray_buffer + sizeof(uint32_t), &ray_msg_load_id, sizeof(uint32_t));
    uint64_t actual_len = ray->Serialize(ray_buffer + 2*sizeof(uint32_t));
    write_to_generator(ray_buffer, actual_len + 2*sizeof(uint32_t));
    delete [] ray_buffer;
}

void send_sample(pbrt::Sample sample) {
    uint64_t packed_sample_size = sample.Size() + 3*sizeof(uint32_t);
    char* sample_buffer = new char[packed_sample_size];
    uint32_t msg_dest = _num_treelets;
    uint32_t msg_id = SAMPLE_MSG_ID;
    uint32_t msg_size = sample.Size();
    memcpy(sample_buffer, &msg_dest, sizeof(uint32_t));
    memcpy(sample_buffer + sizeof(uint32_t), &msg_id, sizeof(uint32_t));
    uint64_t actual_len = sample.Serialize(sample_buffer + 2*sizeof(uint32_t));
    write_to_generator(sample_buffer, actual_len + 2*sizeof(uint32_t));
    delete [] sample_buffer;
}

// TODO: This should be unified with read_from_generator. It's basically the same thing, but it allocates the buffer as well,
// and it checks for a different message type id. We'll probably want to figure out some sort of partial loop unrolling for the nanopu.
// If we could use the preprocessor to define several loops and then choose between those, that would actually be super helpful.
// Or we could just make up a fixed number for now.
int read_treelet(char** buffer, uint64_t* size) {
    // printf("Reading treelet data\n");
    // uint32_t header_buf[2];
    // ssize_t total_len = 0;
    // ssize_t actual_len = 0;
    // do {
    //     actual_len = read(_ray_generator_fd, (char*)&header_buf + total_len, 2*sizeof(uint32_t) - total_len);
    //     total_len += actual_len;
    //     if (actual_len <= 0) {
    //         return -1;
    //     }
    // } while (total_len < 2*sizeof(uint32_t));
    // if (header_buf[0] != TREELET_MSG_LOAD_ID) {
    //     return -1;
    // }
    // *buffer = new char[header_buf[1]];

    // total_len = 0;
    // do {
    //     actual_len = read(_ray_generator_fd, *buffer + total_len, header_buf[1] - total_len);
    //     total_len += actual_len;
    //     if (actual_len <= 0) {
    //         return -1;
    //     }
    // } while (total_len < header_buf[1]);
    // *size = total_len;
    // printf("Read all treelet data with size %d\n", *size);
    // return 0;
}

// TODO: Same here. These should all really be combined.
int read_buffer(char** buffer, uint64_t* size, uint32_t msg_type) {
    uint64_t header = csr_read(0x50);
    uint32_t* header_buf = (uint32_t*)&header;

    // printf("Reading base data\n");
    // uint32_t header_buf[2];
    // ssize_t total_len = 0;
    // ssize_t actual_len = 0;
    // do {
    //     actual_len = read(_ray_generator_fd, (char*)&header_buf + total_len, 2*sizeof(uint32_t) - total_len);
    //     total_len += actual_len;
    //     if (actual_len <= 0) {
    //         return -1;
    //     }
    // } while (total_len < 2*sizeof(uint32_t));
    printf("message id is %d and size is %d\n", header_buf[0], header_buf[1]);
    if (header_buf[0] != msg_type) {
        return -1;
    }
    uint32_t buf_size = header_buf[1];
    buf_size += sizeof(uint64_t) - (buf_size % sizeof(uint64_t)); // Round up to 64-bit words
    *buffer = new char[buf_size];

    for(uint32_t offset = 0; offset < buf_size; offset += sizeof(uint64_t)) {
        // printf("Loading offset %d\n", offset);
        uint64_t data = csr_read(0x50);
        // printf("%#lx\n", data);
        memcpy(*buffer + offset, &data, sizeof(uint64_t));
    }
    *size = header_buf[1];

    // total_len = 0;
    // do {
    //     actual_len = read(_ray_generator_fd, *buffer + total_len, header_buf[1] - total_len);
    //     total_len += actual_len;
    //     if (actual_len <= 0) {
    //         return -1;
    //     }
    // } while (total_len < header_buf[1]);
    // *size = total_len;
    printf("Read all treelet data with size %d\n", *size);
    // return 0;
    return 0;
}

int main( int argc, char* argv[] )
{
  //printf("%ld\n", csr_read(0x50));
//   uint64_t intial_data = csr_read(0x50);
//   uint32_t* data_ref = (uint32_t*)&intial_data;
//   printf("%d, %d\n", data_ref[0], data_ref[1]);
  if ( argc <= 0 ) {
    abort();
  }

  if ( argc < 4 ) {
    usage( argv[0] );
    return EXIT_FAILURE;
  }

  const size_t max_depth = 5;
  const string scene_path { argv[1] };
  const size_t samples_per_pixel = ( argc > 2 ) ? stoull( argv[2] ) : 0;
  _treelet_id = stoull(argv[3]);
  _num_treelets = stoull(argv[4]);

  /* (1) loading the scene */
  // auto scene_base = pbrt::scene::LoadBase( scene_path, samples_per_pixel );

  /* (2) loading all the treelets */
  //vector<shared_ptr<pbrt::CloudBVH>> treelets;

//   _ray_generator_fd = start_client_socket(SERVER_IP_ADDR, SERVER_PORT);
//   if (_ray_generator_fd < 0) {
//       printf("exiting\n");
//       return -1;
//   }

  char* base_buffer = nullptr;
  uint64_t base_size = 0;
  int base_retval = read_buffer(&base_buffer, &base_size, BASE_MSG_LOAD_ID);
  if (base_retval < 0) {
      fprintf(stderr, "Unable to read base\n");
      return -1;
  }
  printf("Loading network base\n");
  //char* base_buffer = new char[1000];
  //uint64_t base_size = 1000;
  pbrt::scene::Base scene_base = pbrt::scene::LoadNetworkBase(base_buffer, base_size, samples_per_pixel);
  printf("Loaded network base\n");
  delete [] base_buffer;

  //for ( size_t i = 0; i < scene_base.GetTreeletCount(); i++ ) {
  //treelets.push_back( pbrt::scene::LoadNetworkTreelet( scene_path, i ) );
  char* buffer = nullptr;
  uint64_t size = 0;
  printf("Reading treelet\n");
  int treelet_retval = read_buffer(&buffer, &size, TREELET_MSG_LOAD_ID);
  if (treelet_retval < 0) {
      fprintf(stderr, "Unable to read treelet\n");
      return -1;
  }
    for (int j = 0; j < 10; j++) {
      char* current_offset = buffer + j*sizeof(uint32_t);
      printf("%#x\n", *(uint32_t*)current_offset);
    }
  printf("Loading network treelet\n");
  shared_ptr<pbrt::CloudBVH> treelet = pbrt::scene::LoadNetworkTreelet(_treelet_id, buffer, size);
  printf("Loaded network treelet\n");
  delete [] buffer;
  while (1);
  //}

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
//  vector<pbrt::Sample> samples;

  pbrt::MemoryArena arena;
  int ray_count = 0;

  while (true) {
    pbrt::RayStatePtr ray(new pbrt::RayState());
    // if (!ray_queue.empty()) {
    //     ray = move(ray_queue.front());
    //     ray_queue.pop();
    // } else {
        char* buffer = new char[pbrt::RayState::MaxPackedSize];
        uint32_t data_len;
        int read_retval = read_from_generator(buffer, &data_len);
        if (read_retval < 0) {
        return -1;
        }
        ray->Deserialize(buffer, data_len);
        delete [] buffer;
        if (ray_count % 1000 == 0) {
            printf("Received ray %d\n", ray_count);
        }
        ray_count++;
   // }

    // for (int i = 0; i < ray->toVisitHead; i++) {
    //   if (i == 0 && ray->toVisit[i].treelet == 0 && ray->toVisit[i].node == 0 && ray->toVisit[i].primitive == 0) {
    //     continue;
    //   }
    //   if (ray->toVisit[i].node == 0) {
    //     continue;
    //   }
    //   printf("index %d has treelet id %d node id %d primitive %d\n", i, ray->toVisit[i].treelet, ray->toVisit[i].node, ray->toVisit[i].primitive);
    // }

    if (ray->CurrentTreelet() != _treelet_id) {
        fprintf(stderr, "Treelet id %d not the same as own treelet id %d\n", ray->CurrentTreelet(), _treelet_id);
        return -1;
    }

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
          send_sample( *new_ray ); // this ray is done
        } else {
          send_ray(move(new_ray)); // back to the ray queue
        }
      } else if ( not empty_visit or hit ) {
          send_ray(move(new_ray)); // back to the ray queue
      } else if ( empty_visit ) {
        new_ray->Ld = 0.f;
        send_sample( *new_ray ); // this ray is done
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
  //pbrt::graphics::AccumulateImage( scene_base.camera, samples );
  //pbrt::graphics::WriteImage( scene_base.camera );

  return EXIT_SUCCESS;
}
