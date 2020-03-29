#pragma once

#include <atomic>

#include "net/s3.hh"
#include "net/secure_socket.hh"
#include "net/socket.hh"
#include "storage/backend_gs.hh"
#include "storage/backend_s3.hh"
#include "transfer.hh"

constexpr std::chrono::seconds ADDR_UPDATE_INTERVAL { 25 };

class S3TransferAgent : public TransferAgent
{
protected:
  struct S3Config
  {
    AWSCredentials credentials {};
    std::string region {};
    std::string bucket {};
    std::string prefix {};

    std::string endpoint {};
    std::atomic<Address> address { Address { "0", 0 } };

    S3Config( const std::unique_ptr<StorageBackend>& backend );
  } clientConfig;

  static constexpr size_t MAX_REQUESTS_ON_CONNECTION { 1 };
  std::chrono::steady_clock::time_point lastAddrUpdate {};
  const bool uploadAsPublic;

  HTTPRequest getRequest( const Action& action );

  void doAction( Action&& action ) override;
  void workerThread( const size_t threadId ) override;

public:
  S3TransferAgent( const std::unique_ptr<StorageBackend>& backend,
                   const size_t threadCount = MAX_THREADS,
                   const bool uploadAsPublic = false );

  ~S3TransferAgent();
};
