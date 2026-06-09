
## 1. Assign a resource to a user (happy case scenario)

Assign resource(a) to user(1)

```mermaid
sequenceDiagram
    participant GrpcClient
    participant GrpcService
    participant UsrRes(urs=1)
    participant UsrResProj
    participant TakenRes(a)
    participant TakenResProj

    GrpcClient ->> GrpcService: 1. Assign(usr=1,rs=a)
    
    GrpcService ->> UsrRes(urs=1): 2. Assign(usr=1,rs=a)
    
    UsrRes(urs=1) ->> UsrRes(urs=1): 3. if(no_active_lock && linkedRs=None) persist(LockState(pending=Assign(usr=1,rs=a))) 

    UsrResProj -->> UsrResProj: 4. DurableStateChange(lockState) 
    
    UsrResProj ->> TakenRes(a): 5. Assign(usr=1,rs=a) 
    
    TakenRes(a) ->> TakenRes(a): 5. if_available(persist(Assigned(usr=1)))
    
    TakenResProj -->> TakenResProj: 6. Assigned(usr=1,rs=a)
    
    TakenResProj ->> UsrRes(urs=1): 7. Confirm(usr=1,rs=a, Assigned)

    UsrRes(urs=1) ->> UsrRes(urs=1): 8. persist(LockState(penging=None), linkedRs=a)) 

    UsrRes(urs=1) ->> GrpcClient: 8. Reply(OK, location(a))

    
```

## 2.  Release a resource (happy case scenario)

user(1) releases resource(a)

```mermaid
sequenceDiagram
    participant GrpcClient
    participant GrpcService
    participant UsrRes(urs=1)
    participant UsrResProj
    participant TakenRes(a)
    participant TakenResProj

    GrpcClient ->> GrpcService: 1. Release(usr=1,rs=a)
    
    GrpcService ->> UsrRes(urs=1): 2. Unassign(usr=1,rs=a)
    
    UsrRes(urs=1) ->> UsrRes(urs=1): 3. if(no_active_lock && linkedRs=a) persist(LockState(pending=Release(usr=1,rs=a))) 

    UsrResProj -->> UsrResProj: 4. DurableStateChange(lockState) 
    
    UsrResProj ->> TakenRes(a): 5. Release(usr=1,rs=a) 
    
    TakenRes(a) ->> TakenRes(a): 6. persist(Unassigned(usr=1))
    
    TakenResProj -->> TakenResProj: 7. Unassigned(usr=1,rs=a)
    
    TakenResProj ->> UsrRes(urs=1): 8. Confirm(usr=1,rs=a, Unassigned)

    UsrRes(urs=1) ->> UsrRes(urs=1): 9. persist(LockState(penging=None), linkedRs=None))

    UsrRes(urs=1) ->> GrpcClient: 10. Reply(OK)

    
```



### 2. Reassign a resource to a user (happy case scenario)
2-step operation:

a) Assign resource(b) to user(1) 

b) Unassign resource(a) from user(1) 

```mermaid
sequenceDiagram

participant GrpcClient
participant GrpcService
participant UsrRes_1
participant UsrResProj
participant TakenRes(b)
participant TakenResProj
participant TakenRes(a)


GrpcClient ->> GrpcService: 1. Reassign(usr=1,from=a,to=b)

GrpcService ->> UsrRes_1: 2. Reassign(usr=1,from=a,to=b)

UsrRes_1 ->> UsrRes_1: 3. if (no_active_lock && linkedRs=a) persist(LockState(pending=Reassign(usr=1,from=a,to=b)

UsrResProj -->> UsrResProj: 4. DurableStateChange(lockState)

UsrResProj ->> TakenRes(b): 5. Reassign(usr=1,from=a,to=b)  

TakenRes(b) ->> TakenRes(b): 6. if_available persist(Assigned(user=1))

TakenResProj -->> TakenResProj: 7. Assigned(user=1,rs=b)
  
TakenResProj ->> TakenRes(a):8. Unassign(usr=1,rs=a))

TakenRes(a) ->> TakenRes(a):9. persist(Unassigned(usr=1))

TakenResProj -->> TakenResProj:10. Unassign(usr=1,rs=a))

TakenResProj ->> UsrRes_1:11. Confirm(usr=1,from=a, to=b,Reassigned)
 
UsrRes_1 ->> UsrRes_1:12. persist(LockState(penging=None), linkedRs=b))

UsrRes_1 ->> GrpcClient:13. Reply(OK, location(b))

```
