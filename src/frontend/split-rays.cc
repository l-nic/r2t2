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
#include <fstream>
#include <dirent.h>
#include <filesystem>
namespace fs = std::filesystem;

#include <pbrt/accelerators/cloudbvh.h>
#include <pbrt/core/geometry.h>
#include <pbrt/main.h>
#include <pbrt/raystate.h>
#include "messages/serialization.hh"

#define UNUSED(expr) do { (void)(expr); } while (0)

#define ERROR_MSG_ID 0 // Error condition, should never occur. Set to zero since this is likely in an improperly configured message.
#define RAY_MSG_LOAD_ID 1 // Switches to initiators
#define RAY_MSG_ID 2 // Regular rays in transit
#define SAMPLE_MSG_ID 3 // Samples returning back to the root
#define TREELET_MSG_LOAD_ID 4 // Treelets being loaded from the switch.
#define BASE_MSG_LOAD_ID 5 // Base data being loaded from the switch.

#define SERVER_PORT 5000
const int CONN_BACKLOG = 32;


using namespace std;

int _server_fd = -1;
map<uint32_t, int> _treelet_handles;
uint32_t _num_treelets = 0;
map<uint32_t, mutex> _send_mutex;
vector<pbrt::Sample> _all_samples;
mutex _sample_lock;
bool _hold_switch_threads = true; // TODO: This should really be a condition variable
uint32_t _sample_count = 0;
pbrt::scene::Base scene_base;

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
  ssize_t total_len = 0;
  do {
    ssize_t written_len = write(write_fd, ray_buffer + total_len, actual_len - total_len);
    total_len += written_len;
    if (written_len <= 0) {
      cerr << "Error writing to treelet " << current_treelet << endl;
      return;
    }
  } while (total_len < actual_len);
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
        pbrt::Sample sample;
        sample.Deserialize(buffer + 2*sizeof(uint32_t), header_buf[2]);
        _sample_lock.lock();
        _all_samples.emplace_back(move(sample));
        // if (total_sample_count % 100 == 0) {
        //   pbrt::graphics::AccumulateImage( scene_base.camera, _all_samples );
        //   pbrt::graphics::WriteImage( scene_base.camera );
        // }
        printf("Sample count %d\n", _sample_count);
        if (_sample_count % 10000 == 0) {
          pbrt::graphics::AccumulateImage( scene_base.camera, _all_samples );
          pbrt::graphics::WriteImage( scene_base.camera );   
        }
        _sample_count++;
        _sample_lock.unlock();
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

int get_treelet_mats(const string& scene_path, vector<string>& mat_filenames) {
  // for (const auto& entry : fs::directory_iterator(scene_path)) {
  //   all_mat_names.push_back(entry.path());
  // }
  DIR *dir;
  struct dirent *ent;
  if ((dir = opendir(scene_path.c_str())) != NULL) {
    while ((ent = readdir(dir)) != NULL) {
      string entry(ent->d_name);
      if (entry.find("MAT") == 0) { // TODO: This is probably not quite right
        mat_filenames.push_back(scene_path + string("/") + entry);
      }
    }
    closedir(dir);
  } else {
    printf("Unable to open treelet directory\n");
    return -1;
  }
  return 0;
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
  printf("Loading old base\n");
  scene_base = pbrt::scene::LoadBase( scene_path, samples_per_pixel );
  printf("Loaded base\n");

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

  vector<string> mat_filenames;
  int mat_send_retval = get_treelet_mats(scene_path, mat_filenames);
  if (mat_send_retval < 0) {
    return -1;
  }

  char* base_buffer = nullptr;
  uint64_t base_size = 0;
  pbrt::scene::SerializeBaseToBuffer(scene_path + "/CAMERA", scene_path + "/LIGHTS", scene_path + "/SAMPLER",
                                     scene_path + "/SCENE", scene_path + "/MANIFEST", &base_buffer, &base_size);
  // TODO: This is a giant copy to add what really ammounts to just a few bytes to the front of the buffer.
  char* base_to_send = new char[base_size + 2*sizeof(uint32_t)];
  uint32_t msg_type = BASE_MSG_LOAD_ID;
  uint32_t msg_size = base_size;
  memcpy(base_to_send, &msg_type, sizeof(uint32_t));
  memcpy(base_to_send + sizeof(uint32_t), &msg_size, sizeof(uint32_t));
  memcpy(base_to_send + 2*sizeof(uint32_t), base_buffer, base_size);
  delete [] base_buffer;

  // Send the raw saved protobuf data to the clients over the network.
  for ( size_t i = 0; i < scene_base.GetTreeletCount(); i++ ) {
    printf("Sending treelet data to treelet %d\n", i);
    _send_mutex[i].lock(); // This needs to be here so that the lock map is initialized before we do anything.
    _send_mutex[i].unlock();

    send_to_client(i, base_to_send, base_size + 2*sizeof(uint32_t));

    // TODO: This is definitely way too many copies.
    string treelet_file_name = scene_path + "/T" + to_string(i);
    printf("continuing\n");
    char* buffer = nullptr;
    uint64_t size = 0;
    pbrt::scene::SerializeTreeletToBuffer(treelet_file_name, mat_filenames, &buffer, &size);
    //ifstream treelet_file_data(treelet_file_name, ios::binary);
    // r2t2::protobuf::RecordReader treelet_reader(treelet_file_name);
    // uint32_t num_triangle_meshes = 0;
    // treelet_reader.read(&num_triangle_meshes);


    // uint32_t next_size = 0;
    // treelet_file_data.read((char*)&next_size, sizeof(uint32_t));
    // uint32_t num_triangle_meshes = 0;
    // buffer_stream.read((char*)&num_triangle_meshes, sizeof(uint32_t));

    // treelet_ostream << treelet_file_data.rdbuf();
    // string treelet_string = treelet_ostream.str();
    char* to_send = new char[size + 2*sizeof(uint32_t)];
    uint32_t msg_type = TREELET_MSG_LOAD_ID;
    uint32_t msg_size = size;
    memcpy(to_send, &msg_type, sizeof(uint32_t));
    memcpy(to_send + sizeof(uint32_t), &msg_size, sizeof(uint32_t));
    memcpy(to_send + 2*sizeof(uint32_t), buffer, size); // We don't copy the terminating null character at the end of c_str's return string.
    delete [] buffer;
    //int num_4byte_words = treelet_string.length() / 4;
    // Looks like the data record stores the size of each object immediately beforehand.
    for (int j = 0; j < 10; j++) {
      char* current_offset = to_send + 2*sizeof(uint32_t) + j*sizeof(uint32_t);
      printf("%#x\n", *(uint32_t*)current_offset);
    }
    send_to_client(i, to_send, size + 2*sizeof(uint32_t));
    delete [] to_send;
  }

  delete [] base_to_send;


  /* (3) generating all the initial rays */
  queue<pbrt::RayStatePtr> ray_queue;

    _num_treelets = scene_base.GetTreeletCount();

    vector<thread> all_threads;
    for ( size_t i = 0; i < scene_base.GetTreeletCount(); i++ ) {
      all_threads.emplace_back(move(thread(handle_client_reads, i)));
    }
  int total_ray_count = 0;
  for (const auto pixel : scene_base.sampleBounds) {
    for (int sample = 0; sample < scene_base.samplesPerPixel; sample++) {
      total_ray_count++;
    }
  }
  printf("Total ray count is %d\n", total_ray_count);
  total_ray_count = 0;

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
      total_ray_count++;
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