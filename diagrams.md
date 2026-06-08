

### 1. Assigns a resource to a user

Assign resource(a) to user(1)


```plantuml
@startuml

entity GrpcClient

entity Service

entity UsrRes_1

entity UsrResProj

entity TakenRes_a

entity TakenResProj

GrpcClient -> Service:1. Assign(user=1,rs=a)
Service -> UsrRes_1:2. Assign(user=1,rs=a)
UsrRes_1 -> UsrRes_1:3. if_no_active_lock persist(LockState(pending=Assign(user=1,rs=a))) 

UsrResProj -> UsrResProj:4. Pulls DurableStateChange(lockState) 
UsrResProj -> TakenRes_a:5. Assign(user=1,rs=a) 

TakenRes_a -> TakenRes_a:6. if_available(persist(Assigned(user=1)))  


TakenResProj -> TakenResProj:7. Pulls Assigned(user=1,rs=a)
TakenResProj -> UsrRes_1:8. Confirm(user=1,rs=a, Assigned)
UsrRes_1 -> UsrRes_1:9. persist(Remove(LockState()),LinkedResource(a))
UsrRes_1 -> GrpcClient:10. Reply(OK,location(a))  

@enduml
```


## 2.  Release a resource (happy case scenario)

user(1) releases resource(a)

```plantuml
@startuml

entity GrpcClient

entity GrpcService

entity UsrRes_1

entity UsrResProj

entity TakenRes_a

entity TakenResProj


GrpcClient -> GrpcService: 1. Release(usr=1,rs=a)

GrpcService -> UsrRes_1: 2. Unassign(usr=1,rs=a)

UsrRes_1 -> UsrRes_1: 3. if(no_active_lock && rs=a) persist(LockState(pending=Release(usr=1,rs=a))) 

UsrResProj -> UsrResProj: 4. Pulls DurableStateChange(lockState) 

UsrResProj -> TakenRes_a: 5. Release(usr=1,rs=a) 

TakenRes_a -> TakenRes_a: 6. persist(Unassigned(usr=1))

TakenResProj -> TakenResProj: 7. Pulls Unassigned(usr=1,rs=a)

TakenResProj -> UsrRes_1: 7. Confirm(usr=1,rs=a, Unassigned)

UsrRes_1 -> UsrRes_1: 8. persist(LockState(pendingCmd=None), linkedRs=None))

UsrRes_1 -> GrpcClient: 9. Reply(OK)  

@enduml
```

### 2. Reassign a resource to a user (happy case scenario)
2-step operation:

a) Assign resource(b) to user(1)

b) Unassign resource(a) from user(1)

```plantuml
@startuml

entity GrpcClient

entity Service

entity UsrRes_1

entity UsrResProj

entity TakenRes_b

entity TakenResProj

entity TakenRes_a


GrpcClient --> Service:1. Reassign(usr=1,from=a,to=b)
Service --> UsrRes_1:2. Reassign(usr=1,from=a,to=b)
UsrRes_1 --> UsrRes_1:3. if_no_active_lock persist(LockState(pending=Reassign(usr=1,from=a,to=b)

UsrResProj --> UsrResProj:4. Pulls DurableStateChange(lockState)

UsrResProj --> TakenRes_b:5. Reassign(usr=1,from=a,to=b)  

TakenRes_b --> TakenRes_b:6. if_available persist(Assigned(user=1,rs=b))

TakenResProj --> TakenResProj:7. Pulls Assigned(user=1,rs=b)  
TakenResProj --> TakenRes_a:8. Unassign(user=1,rs=a))
TakenRes_a --> TakenRes_a:9. persist(Unassigned(usr=1))

TakenResProj --> TakenResProj:10. Pulls Unassign(usr=1,rs=a))
TakenResProj --> UsrRes_1:11. Confirm(user=1,from=a, to=b,Reassigned) 
UsrRes_1 --> UsrRes_1:12. persist(Remove(LockState()), LinkedResource(b))
UsrRes_1 --> GrpcClient:13. Reply(OK)

@enduml
```
     
