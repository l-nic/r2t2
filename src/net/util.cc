#include "net/util.hh"

#include <endian.h>

using namespace std;

string put_field( const bool n )
{
  return n ? string( 1, '\x01' ) : string( 1, '\x00' );
}

string put_field( const uint64_t n )
{
  const uint64_t network_order = htobe64( n );
  return string( reinterpret_cast<const char*>( &network_order ),
                 sizeof( network_order ) );
}

string put_field( const uint32_t n )
{
  const uint32_t network_order = htobe32( n );
  return string( reinterpret_cast<const char*>( &network_order ),
                 sizeof( network_order ) );
}

string put_field( const uint16_t n )
{
  const uint16_t network_order = htobe16( n );
  return string( reinterpret_cast<const char*>( &network_order ),
                 sizeof( network_order ) );
}

void put_field( char* message, const bool n, size_t loc )
{
  message[loc] = n ? '\x01' : '\x00';
}

void put_field( char* message, const uint64_t n, size_t loc )
{
  const uint64_t network_order = htobe64( n );
  memcpy( message + loc,
          reinterpret_cast<const char*>( &network_order ),
          sizeof( network_order ) );
}

void put_field( char* message, const uint32_t n, size_t loc )
{
  const uint32_t network_order = htobe32( n );
  memcpy( message + loc,
          reinterpret_cast<const char*>( &network_order ),
          sizeof( network_order ) );
}

void put_field( char* message, const uint16_t n, size_t loc )
{
  const uint32_t network_order = htobe16( n );
  memcpy( message + loc,
          reinterpret_cast<const char*>( &network_order ),
          sizeof( network_order ) );
}