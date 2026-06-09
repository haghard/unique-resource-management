
## 1. Assign a resource to a user

Assign `resource(a)` to `user(1)`

```plantuml
@startuml

entity GrpcClient

entity GrpcService

entity UserResourceLink_1

entity TakenUniqueResource_a

entity TakenResProj

GrpcClient -> GrpcService:1. Assign(usr=1,rs=a)
GrpcService -> UserResourceLink_1:2. Assign(user=1,rs=a)
UserResourceLink_1 -> UserResourceLink_1:3. if(no_active_lock && linkedRs=None) persist(LockState(pendingCmd=Assign(user=1,rs=a))) 
UserResourceLink_1 -> TakenUniqueResource_a:4. Assign(usr=1,rs=a)
TakenUniqueResource_a -> TakenUniqueResource_a:5. if_available persist(Assigned(user=1))  
TakenResProj --> TakenResProj:6. Assigned(usr=1,rs=a)
TakenResProj -> UserResourceLink_1:7. Confirm(usr=1,rs=a, Assigned)
UserResourceLink_1 -> UserResourceLink_1:8. persist(LockState(pendingCmd=None),LinkedResource(a))
UserResourceLink_1 -> GrpcClient:9. Reply(resourceLocation(a))  

@enduml
```


## 2. Release a resource
Given `user(1)` is linked to `resource(a)`

`user(1)` should release `resource(a)`


```plantuml
@startuml

entity GrpcClient

entity GrpcService

entity UserResourceLink_1

entity TakenUniqueResource_a

entity TakenResProj

GrpcClient -> GrpcService:1. Release(usr=1,rs=a)
GrpcService -> UserResourceLink_1:2. Unassign(user=1,res=a)
UserResourceLink_1 -> UserResourceLink_1:3. if(no_active_lock && linkedRs=a)(persist(LockState(pendingCmd=Unassign(usr=1,rs=a)))) 
UserResourceLink_1 -> TakenUniqueResource_a:4. Release(usr=1,res=a)
TakenUniqueResource_a -> TakenUniqueResource_a:5. persist(Unassigned(usr=1))  
TakenResProj -> TakenResProj:6. Unassigned(usr=1,rs=a)
TakenResProj -> UserResourceLink_1:7. Confirm(usr=1,rs=a,Assigned)
UserResourceLink_1 -> UserResourceLink_1:8. persist(LockState(pendingCmd=None),linkedRs=None))
UserResourceLink_1 -> GrpcClient:9. Reply(resourceLocation(a))  

@enduml
```



## 3. Reassign a resource to a user

It is a 2-step operation. Given `user(1)` is linked to `resource(b)` we need to do the following:

a) Assign `resource(b)` to `user(1)`

b) Unassign `resource(a)` from `user(1)`


```plantuml
@startuml

entity GrpcClient

entity ResourceService

entity UserResourceLink_1

entity TakenUniqueResource_b

entity TakenResProj

entity TakenUniqueResource_a


GrpcClient -> ResourceService:1. Reassign(usr=1,from=a,to=b)
ResourceService -> UserResourceLink_1:2. Reassign(usr=1,from_rs=a,to_rs=b)
UserResourceLink_1 -> UserResourceLink_1:3. if(no_active_lock && linkedRs=a)(persist(LockState(pendingCmd=Reassign(usr=1,from_rs=a,to_rs=b))
UserResourceLink_1 -> TakenUniqueResource_b:4. Reassign(usr=1,rs=b)
TakenUniqueResource_b -> TakenUniqueResource_b:5. persist(Assigned(usr=1,rs=b))
TakenResProj -> TakenResProj:6. Assigned(usr=1,rs=b)  
TakenResProj -> TakenUniqueResource_a:7. Unassign(usr=1,rs=a))
TakenUniqueResource_a -> TakenUniqueResource_a:8. persist(Unassigned(usr=1))
TakenResProj -> TakenResProj:9. Unassign(usr=1,rs=a))
TakenResProj -> UserResourceLink_1:10. Confirm(usr=1,from_rs=a,to_rs=b,Reassigned) 
UserResourceLink_1 -> UserResourceLink_1:11. persist(LockState(pendingCmd=None),LinkedResource(b))
UserResourceLink_1 -> GrpcClient:12 Reply(resourceLocation(b))

@enduml
```