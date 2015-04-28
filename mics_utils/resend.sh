while [ 1 ]; do
for i in 1 2 ; do
python universal_testing_client.py tcp://127.0.0.1:19575 PUB 0 '{"emitter": "peerstatus", "wid": "peerstatus_1", "seq": "0", "task_id": "0", "ip": "127.0.1.1", "arg": "", "pid": 2704, "next": "unset", "ext_ip": "127.0.0.1", "step": "peerstatus_1_consumer", "type": "master", "id": "peerstatus_1", "serialization": "str", "where":"em.ubity.com", "event": "RESEND", "channel": "PeerStatus"}'
	done;
	sleep 1800;
done
