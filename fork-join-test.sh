#!/usr/bin/env bash
echo
echo
echo "Calling STATUS 0"
curl -H "Accept:application/json" -v "http://localhost:3000/v1/status?barrier-id=a~b"
echo
echo "Calling FORK"
curl -v -H "Content-Type:application/json" -X POST "http://localhost:3000/v1/fork" -d '{"count": 3,"destination": "http://localhost:3000/v1/publish","join-id": "a","transaction-id": "b", "timeout-destination": "http://localhost:3000/v1/publish/timeout", "max-wait": 10}'
echo
echo
echo "Calling STATUS 1"
curl -H "Accept:application/json" -v "http://localhost:3000/v1/status?barrier-id=a~b"
echo
echo
echo "JOIN 0"
curl -v -H "Content-Type:application/json" -X POST "http://localhost:3000/v1/join" -d '{"barrier-id": "a~b","position": 0,"message": "fffffff"}'
echo
echo
echo "JOIN 1"
curl -v -H "Content-Type:application/json" -X POST "http://localhost:3000/v1/join" -d '{"barrier-id": "a~b","position": 1,"message": "fffffff"}'
echo
echo
echo "Calling STATUS 2"
curl -H "Accept:application/json" -v "http://localhost:3000/v1/status?barrier-id=a~b"
echo
echo
echo "JOIN 2"
curl -v -H "Content-Type:application/json" -X POST "http://localhost:3000/v1/join" -d '{"barrier-id": "a~b","position": 2,"message": "fffffff"}'
echo
echo
echo "Calling STATUS 3"
curl -H "Accept:application/json" -v "http://localhost:3000/v1/status?barrier-id=a~b"
sleep 2 
echo
echo
echo "Calling STATUS 4"
curl -H "Accept:application/json" -v "http://localhost:3000/v1/status?barrier-id=a~b"
