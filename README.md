## Unique resources tracking app

### Distributed sharded resources that support Compare-And-Set style of operation
1) Distributed sharded resources that can be acquired by users.
2) Resources control transfer between users.

### I-offender
Operations that may break app level invariants when executed concurrently.

## Problem statement

This app needs to support the following operations:

`Assign(user, resource)`

`Unassign(user, resource_location)`

`Reassign(user, prev_resource_location, new_resource)`

across many client and users. App level invariant we want to obtain: Each unique resource belongs to exactly one user at any point in time.
Resources are not permanently assigned; even after assignment to a user, they can be reassigned to another user.
External clients are outside our control; therefore, concurrent requests must be handled defensively.
For example, the following sequence of operations breaks our application level invariant.

```

Client1 `Create(user=1, resource=x)` <> Client2 `Create(user=1, resource=y)`

and

Client1 `Update(user=2, from=x, to=y)` <> Client2 `Update(user=2, from=x, to=z)`

```


### Assign
`UserId(A)` has no resources associated to it and wants to acquire `Resource(x)` only if `Resource(x)` doesn’t belong to another user.

```
   lock(UserId(1))       
      acquire_if_available(Resource(y))               
   unlock(UserId(1))
```

### Reassign
`UserId(A)` owns `Resource(x)` and wants to acquire `Resource(y)` only if `Resource(y)` doesn’t belong to another user

```
   lock(UserId(1)) 
      acquire_if_available(Resource(y))
      release(Resource(x))
   unlock(UserId(1))
```


## Requirements
 
✅ Application correctness

✅ Clients are able to order their own operations and provide a globally unique ID (`user_id`).

It's a relative order invariant. Resource acquisition requires a `precondition check`.  Releasing resource doesn't require any checks and should succeed eventually. Causal ordering if enough to guarantee we never violate anything in this flow.


# How to run 

```

docker-compose -f docker-compose.yml up

sbt a

sbt b

```

### Available api 

```

grpcurl -plaintext 127.0.0.1:8080 list

http GET 127.0.0.1:8079/resources/cluster/members
http GET 127.0.0.2:8079/resources/cluster/shards/usr-rs
http GET 127.0.0.2:8079/resources/cluster/shards/rs

```

### Assign conflicts

Create conflict1: `User 1` attempt to obtain `resource=ff645a` and `resource=ff645b` at the same time from different clients (contention on `OwnerId(1)`)

```
 
grpcurl -d '{"resource":{"name":"ff645a","version":1},"user_id":"111367c3-9ad3-47ef-a6b0-784d52c96481"}' -plaintext 127.0.0.1:8080 com.resource.api.ResourceService/Assign
grpcurl -d '{"resource":{"name":"ff645b","version":2},"user_id":"111367c3-9ad3-47ef-a6b0-784d52c96481"}' -plaintext 127.0.0.1:8080 com.resource.api.ResourceService/Assign

grpcurl -d '{"location":{"bucketId":"-1546872207885992589","seqNum":0},"user_id":"111367c3-9ad3-47ef-a6b0-784d52c96481"}' -plaintext 127.0.0.2:8080 com.resource.api.ResourceService/Release


```

Create conflict2: `OwnerId(1)` and `OwnerId(2)` attempt to obtain `definition=cff645` at the same time from different clients (Contention on `definition=cff645`)

```
  
grpcurl -d '{"resource":{"name":"cff645","version":1},"user_id":"111367c3-9ad3-47ef-a6b0-784d52c96483"}' -plaintext 127.0.0.1:8080 com.resource.api.ResourceService/Assign
grpcurl -d '{"resource":{"name":"cff645","version":1},"user_id":"111367c3-9ad3-47ef-a6b0-784d52c96484"}' -plaintext 127.0.0.1:8080 com.resource.api.ResourceService/Assign

```


### Reassign conflicts

Conflict #1: `OwnerId(1)` attempt to update definition=`aa` to definition=`bb` from different clients at the same time

```
grpcurl -d '{"resource":{"name":"aa","version":1},"user_id":"211367c3-9ad3-47ef-a6b0-784d52c96486"}' -plaintext 127.0.0.1:8080 com.resource.api.ResourceService/Assign

grpcurl -d '{"resource":{"name":"bb","version":1},"userId":"211367c3-9ad3-47ef-a6b0-784d52c96486","location":{"bucketId":"7589125186702523474","seqNum":"0"}}' -plaintext 127.0.0.1:8080 com.resource.api.ResourceService/Reassign
grpcurl -d '{"resource":{"name":"bb","version":1},"userId":"211367c3-9ad3-47ef-a6b0-784d52c96486","location":{"bucketId":"7589125186702523474","seqNum":"0"}}' -plaintext 127.0.0.1:8080 com.resource.api.ResourceService/Reassign
 
```


Conflict #2: 
`OwnerId(1)` attempts to update resource=`ccf64567868` to definition=`ccf64567868a` and
`OwnerId(1)` attempts to update resource=`ccf64567868` to definition=`ccf64567868b` at the same time

``` 

grpcurl -d '{"resource":{"name":"ccf64567868","version":1},"user_id":"211367c3-9ad3-47ef-a6b0-784d52c96489"}' -plaintext 127.0.0.1:8080 com.resource.api.ResourceService/Assign

grpcurl -d '{"resource":{"name":"ccf64567868a","version":1},"userId":"211367c3-9ad3-47ef-a6b0-784d52c96489","location":{"bucketId":"2611229345306089543","seqNum":"0"}}' -plaintext 127.0.0.1:8080 com.resource.api.ResourceService/Reassign
grpcurl -d '{"resource":{"name":"ccf64567868b","version":1},"userId":"211367c3-9ad3-47ef-a6b0-784d52c96489","location":{"bucketId":"2611229345306089543","seqNum":"0"}}' -plaintext 127.0.0.1:8080 com.resource.api.ResourceService/Reassign

`OwnerId(1)` attempt to update resource to `resource=a` and `resource=b` at the same time from different clients


```

### Assign/Reassign conflicts

Conflict #1: 
   `OwnerId(1)` attempts to update its resource to `ccf64567868b` while `OwnerId(2)` attempts to update its definition to the same value 
   and at the same time causing contention on definition(`cc645697`)


```

grpcurl -d '{"resource":{"name":"aaf645699","version":1},"user_id":"211367c3-9ad3-47ef-a6b0-784d52c96471"}' -plaintext 127.0.0.1:8080 com.resource.api.ResourceService/Assign
grpcurl -d '{"resource":{"name":"bbf645698","version":1},"user_id":"211367c3-9ad3-47ef-a6b0-784d52c96472"}' -plaintext 127.0.0.1:8080 com.resource.api.ResourceService/Assign


grpcurl -d '{"resource":{"name":"cc645697","version":1},"userId":"211367c3-9ad3-47ef-a6b0-784d52c96471","location":{"bucketId":"-6385963432772962948","seqNum":"0"}}' -plaintext 127.0.0.1:8080 com.resource.api.ResourceService/Reassign
grpcurl -d '{"resource":{"name":"cc645697","version":1},"userId":"211367c3-9ad3-47ef-a6b0-784d52c96472","location":{"bucketId":"43923305614222974","seqNum":"0"}}' -plaintext 127.0.0.1:8080 com.resource.api.ResourceService/Reassign


`OwnerId(1)` attempt to update resource to `resource=a` and `resource=b` at the same time from different clients

```

        

### Assign/Reassign conflicts

```

grpcurl -d '{"resource":{"name":"a","version":1},"user_id":"211367c3-9ad3-47ef-a6b0-784d52c96482" }' -plaintext 127.0.0.1:8080 com.resource.api.ResourceService/Assign


grpcurl -d '{"resource":{"name":"b","version":1},"userId":"211367c3-9ad3-47ef-a6b0-784d52c96482","location":{"bucketId":"-294803572505835665","seqNum":"0"}}' -plaintext 127.0.0.1:8080 com.resource.api.ResourceService/Reassign

grpcurl -d '{"resource":{"name":"b","version":1},"userId":"211367c3-9ad3-47ef-a6b0-784d52c96485"}' -plaintext 127.0.0.1:8080 com.resource.api.ResourceService/Assign


```

### Other examples

```

grpcurl -d '{"resource":{"name":"x1","version":2},"user_id":"211367c3-9ad3-47ef-a6b0-784d52c96492"}' -plaintext 127.0.0.1:8080 com.resource.api.ResourceService/Assign
grpcurl -d '{"resource":{"name":"b1","version":2},"userId":"211367c3-9ad3-47ef-a6b0-784d52c96492","location":{"bucketId":"-8081860899035392800","seqNum":"0"}}' -plaintext 127.0.0.1:8080 com.resource.api.ResourceService/Reassign
grpcurl -d '{"resource":{"name":"c1","version":2},"userId":"211367c3-9ad3-47ef-a6b0-784d52c96492","location":{"bucketId":"5395919907386521286","seqNum":"0"}}' -plaintext 127.0.0.1:8080 com.resource.api.ResourceService/Reassign
grpcurl -d '{"resource":{"name":"d1","version":2},"userId":"211367c3-9ad3-47ef-a6b0-784d52c96492","location":{"bucketId":"591900785108084723","seqNum":"0"}}' -plaintext 127.0.0.1:8080 com.resource.api.ResourceService/Reassign
grpcurl -d '{"resource":{"name":"e1","version":2},"userId":"211367c3-9ad3-47ef-a6b0-784d52c96492","location":{"bucketId":"-4373920888234706083","seqNum":"0"}}' -plaintext 127.0.0.1:8080 com.resource.api.ResourceService/Reassign

grpcurl -d '{"location":{"bucketId":"5800016335066854875","seqNum":0},"user_id":"211367c3-9ad3-47ef-a6b0-784d52c96492"}' -plaintext 127.0.0.2:8080 com.resource.api.ResourceService/Release


grpcurl -d '{"userId":"211367c3-9ad3-47ef-a6b0-784d52c96489"}' -plaintext 127.0.0.1:8080 com.resource.api.ResourceService/GetResource
grpcurl -d '{"location":{"bucketId":"-4373920888234706083","seqNum": "1"},"user_id":"111367c3-9ad3-47ef-a6b0-784d52c96481"}' -plaintext 127.0.0.2:8080 com.resource.api.ResourceService/Release


grpcurl -d '{"userId":"211367c3-9ad3-47ef-a6b0-784d52c96489"}' -plaintext 127.0.0.1:8080 com.resource.api.ResourceService/GetResource

```

Another similar domain and use cases:   
   * Accounts with unique usernames
   * users buying/exchanging available seats/tickets/parking lots
   * library: users / books
   * vehicles renting
   * students and course subscriptions



grpcurl -d '{"resource":{"name":"a","version":1},"user_id":"211367c3-9ad3-47ef-a6b0-784d52c96482" }' -plaintext 174.138.113.57:8080 com.resource.api.ResourceService/Assign

```

                     
kubectl --kubeconfig=./kubernetes/k8s-1-31-1-do-3-tor1-1729544104597-kubeconfig.yaml delete namespaces resources-ns

kubectl --kubeconfig=./kubernetes/k8s-1-31-1-do-3-tor1-1729544104597-kubeconfig.yaml apply -f kubernetes/namespace.json
kubectl --kubeconfig=./kubernetes/k8s-1-31-1-do-3-tor1-1729544104597-kubeconfig.yaml config set-context --current --namespace=resources-ns

kubectl --kubeconfig=./kubernetes/k8s-1-31-1-do-3-tor1-1729544104597-kubeconfig.yaml create -f kubernetes/sbr-lease.yml -n resources-ns
kubectl --kubeconfig=./kubernetes/k8s-1-31-1-do-3-tor1-1729544104597-kubeconfig.yaml apply -f kubernetes/service-lb-do.yml

kubectl --kubeconfig=./kubernetes/k8s-1-31-1-do-3-tor1-1729544104597-kubeconfig.yaml create secret generic \
    db-secret \
    --from-literal=DB_HOST=... \
    --from-literal=DB_NAME=... \
    --from-literal=DB_USER=... \
    --from-literal=DB_PASSWORD=... \
    --from-literal=DB_PORT=...

kubectl --kubeconfig=./kubernetes/k8s-1-31-1-do-3-tor1-1729544104597-kubeconfig.yaml apply -f kubernetes/deployment.yml
kubectl --kubeconfig=./kubernetes/k8s-1-31-1-do-3-tor1-1729544104597-kubeconfig.yaml scale deployment/resources --replicas=2


kubectl --kubeconfig=./kubernetes/k8s-1-31-1-do-3-tor1-1729544104597-kubeconfig.yaml get deployments
kubectl --kubeconfig=./kubernetes/k8s-1-31-1-do-3-tor1-1729544104597-kubeconfig.yaml get pods
kubectl --kubeconfig=./kubernetes/k8s-1-31-1-do-3-tor1-1729544104597-kubeconfig.yaml get services

kubectl --kubeconfig=./kubernetes/k8s-1-31-1-do-3-tor1-1729544104597-kubeconfig.yaml logs -f -c resources resources-94cdcfbfb-c2vjr

kubectl delete pod <pod-name>

```


### Drop all tcp traffic to simulate split brain
```
kubectl --kubeconfig=./kubernetes/k8s-1-31-1-do-3-tor1-1729544104597-kubeconfig.yaml exec -it resources-64bb48b97d-r99bz -- /bin/sh

iptables -A INPUT -p tcp -j DROP
  
iptables -D INPUT -p tcp -j DROP`

```

### Links  
  https://doc.akka.io/libraries/akka-projection/current/durable-state.html
  https://doc.akka.io/libraries/akka-core/current/typed/index-persistence-durable-state.html
