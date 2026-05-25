

i=0
j=9

while [ $i -ne $j ]
do
  i=$(($i+1))
  grpcurl -d '{"resource":{"name":"'a"${i}"'","version":1},"user_id":"'111367c3-9ad3-47ef-a6b0-784d52c9648"${i}"'" }' -plaintext 127.0.0.1:8080 com.resource.api.ResourceService/Assign;
  grpcurl -d '{"resource":{"name":"'b"${i}"'","version":1},"user_id":"'211367c3-9ad3-47ef-a6b0-784d52c9648"${i}"'" }' -plaintext 127.0.0.1:8080 com.resource.api.ResourceService/Assign;
  grpcurl -d '{"resource":{"name":"'c"${i}"'","version":1},"user_id":"'311367c3-9ad3-47ef-a6b0-784d52c9648"${i}"'" }' -plaintext 127.0.0.1:8080 com.resource.api.ResourceService/Assign;
  grpcurl -d '{"resource":{"name":"'d"${i}"'","version":1},"user_id":"'411367c3-9ad3-47ef-a6b0-784d52c9648"${i}"'" }' -plaintext 127.0.0.1:8080 com.resource.api.ResourceService/Assign;
  grpcurl -d '{"resource":{"name":"'e"${i}"'","version":1},"user_id":"'511367c3-9ad3-47ef-a6b0-784d52c9648"${i}"'" }' -plaintext 127.0.0.1:8080 com.resource.api.ResourceService/Assign;
  #sleep .1
done
