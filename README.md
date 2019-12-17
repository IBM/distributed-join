# distributed-join
Demonstration of using Redis for distributed fork-join semantics with a continuation and at least once processing.

The user needs to setup a barrier with barrier id:

```
curl -v -H "Content-Type:application/json" 
    -X POST "http://localhost:3000/v1/fork" 
    -d '{
            "count": 3,
            "destination": "http://localhost:3000/v1/publish",
            "join-id": "a",
            "transaction-id": "b", 
            "timeout-destination": "http://localhost:3000/v1/publish/timeout", 
            "max-wait": 10
        }'
```

In the above example, the user creates a fork (barrier) that will accept three parts and will reply back to the destination 
address (continuation). 

Once the fork is created, parts can be joined to it:

```
curl -v -H "Content-Type:application/json" 
    -X POST "http://localhost:3000/v1/join" 
    -d '{
            "barrier-id": "a~b",
            "position": 0,
            "message": "fffffff"
        }'
```

In the above example, the user submits a join to barrier-id consisting of join-id and transaction-id in position 0

The user can get status of the join in the following example: 

```
curl -H "Accept:application/json" 
    -v "http://localhost:3000/v1/status?barrier-id=a~b"
```

Once all positions are collected, the continuation will fire and submit the full payload to the destination. 
If the timeout passes, the continuation will fire what's available to the timeout-destination. 

The semantics for firing continuation are "at least once". It's achieved by using a "double-lock" mechanism with two Redis queues. The details are in the core.clj file. 
 
