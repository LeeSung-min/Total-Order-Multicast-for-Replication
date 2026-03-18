import uuid

class Message:
    def __init__(self, msg_type, update_id, op, ts, sender_id):
        self.msg_type = msg_type
        self.update_id = update_id
        self.op = op
        self.ts = ts
        self.sender_id = sender_id


class Replica:
    def __init__(self, replica_id, all_replica_ids, network_simulator):
        self.replica_id = replica_id
        self.all_replica_ids = all_replica_ids
        self.network = network_simulator  # Will be implemented in Part B

        # Local Data Structures
        self.clock = 0
        self.holdback_queue = []
        self.max_seen = {r_id: 0 for r_id in all_replica_ids}

        # Application State
        self.kv_store = {}
        self.delivered_history = []

    def _sort_queue(self):
        # Sort by timestamp first. If tied, sort by sender_id.
        self.holdback_queue.sort(key=lambda m: (m.ts, m.sender_id))

    def receive_client_update(self, op):
        # 1. Advance clock
        self.clock += 1
        update_id = str(uuid.uuid4())

        # 2. Create and queue the TOBCAST
        msg = Message('TOBCAST', update_id, op, self.clock, self.replica_id)
        self.holdback_queue.append(msg)
        self._sort_queue()

        # 3. Update our own max_seen
        self.max_seen[self.replica_id] = self.clock

        # 4. Broadcast to everyone else
        self.network.broadcast(self.replica_id, msg)
        self.try_deliver()

    def receive_message(self, msg):
        # 1. Update clock: max(local, received) + 1
        self.clock = max(self.clock, msg.ts) + 1

        # 2. Track the highest timestamp seen from this sender
        self.max_seen[msg.sender_id] = max(self.max_seen[msg.sender_id], msg.ts)

        if msg.msg_type == 'TOBCAST':
            # Queue the update
            self.holdback_queue.append(msg)
            self._sort_queue()

            # Send ACK to all replicas so they know our clock has advanced
            self.clock += 1
            ack_msg = Message('ACK', msg.update_id, None, self.clock, self.replica_id)
            self.network.broadcast(self.replica_id, ack_msg)

            # Update our own progress tracker
            self.max_seen[self.replica_id] = self.clock

        # Try to deliver messages now that our state has changed
        self.try_deliver()

    def try_deliver(self):
        while self.holdback_queue:
            head = self.holdback_queue[0]
            is_safe = True

            # Check if all other replicas have passed this timestamp
            for r_id in self.all_replica_ids:
                if r_id == head.sender_id:
                    continue

                if self.max_seen[r_id] < head.ts:
                    is_safe = False
                    break
                elif self.max_seen[r_id] == head.ts and r_id < head.sender_id:
                    # Tie-breaker check to prevent deadlocks
                    is_safe = False
                    break

            if is_safe:
                # It's safe! Pop and apply.
                msg_to_deliver = self.holdback_queue.pop(0)
                self.apply_operation(msg_to_deliver)
            else:
                # If the head isn't safe, stop checking.
                break

    def apply_operation(self, msg):
        op_type, key, value = msg.op

        if op_type == 'put':
            self.kv_store[key] = value
        elif op_type == 'append':
            self.kv_store[key] = self.kv_store.get(key, "") + value
        elif op_type == 'incr':
            self.kv_store[key] = self.kv_store.get(key, 0) + value

        self.delivered_history.append(msg.update_id)


class NetworkSimulator:
    def __init__(self):
        self.replicas = {}
        self.events = []  # Priority queue: (time, counter, type, dest_id, payload)
        self.time = 0
        self.counter = 0  # Tie-breaker for simultaneous events
        self.last_delivery = {}  # Enforces FIFO per (sender_id, dest_id)

    def add_replica(self, replica):
        self.replicas[replica.replica_id] = replica

    def broadcast(self, sender_id, msg):
        for dest_id in self.replicas:
            # Simulate random network delay (e.g., 10 to 100 ms)
            delay = random.randint(10, 100)

            # ENFORCE FIFO: A message cannot arrive before a previously sent message
            # from the same sender to the same destination.
            earliest_possible = self.last_delivery.get((sender_id, dest_id), self.time) + 1
            deliver_time = max(self.time + delay, earliest_possible)
            self.last_delivery[(sender_id, dest_id)] = deliver_time

            self.counter += 1
            heapq.heappush(self.events, (deliver_time, self.counter, 'msg', dest_id, msg))

    def client_send(self, dest_id, op, delay=0):
        # Simulates a client sending a request to a specific replica
        deliver_time = self.time + delay
        self.counter += 1
        heapq.heappush(self.events, (deliver_time, self.counter, 'client', dest_id, op))

    def run(self):
        # Process all events in time order until the network is quiet
        while self.events:
            t, _, ev_type, dest_id, payload = heapq.heappop(self.events)
            self.time = t
            if ev_type == 'msg':
                self.replicas[dest_id].receive_message(payload)
            elif ev_type == 'client':
                self.replicas[dest_id].receive_client_update(payload)


def verify_and_print_results(replicas, experiment_name):
    print(f"\n--- Results: {experiment_name} ---")

    # Grab the baseline from Replica 1
    base_state = replicas[0].kv_store
    base_history = replicas[0].delivered_history

    is_correct = True
    for r in replicas:
        print(f"Replica {r.replica_id} State: {r.kv_store}")
        # Print first 5 history items just to keep logs clean
        print(f"  -> History (first 5): {r.delivered_history[:5]}")

        if r.kv_store != base_state or r.delivered_history != base_history:
            is_correct = False

    print("\n✅ CORRECTNESS CHECK PASSED: All replicas have identical state and history."
          if is_correct else "\n❌ CORRECTNESS CHECK FAILED: Replicas diverged.")
    print("-" * 50)


def setup_system(num_replicas=3):
    sim = NetworkSimulator()
    replica_ids = [i for i in range(1, num_replicas + 1)]
    replicas = [Replica(r_id, replica_ids, sim) for r_id in replica_ids]
    for r in replicas:
        sim.add_replica(r)
    return sim, replicas


# ==========================================
# B2: Required Experiments
# ==========================================

if __name__ == "__main__":

    # Experiment 1: Concurrent Conflicting Updates
    print("\nStarting Experiment 1: Concurrent Conflicting Updates")
    sim1, replicas1 = setup_system(3)
    # Clients hit different replicas at the exact same simulation time
    sim1.client_send(dest_id=1, op=('put', 'account_A', 100))
    sim1.client_send(dest_id=2, op=('append', 'account_A', "_bonus"))
    sim1.client_send(dest_id=3, op=('put', 'account_A', 50))
    sim1.run()
    verify_and_print_results(replicas1, "Concurrent Conflicting Updates")

    # Experiment 2: High Contention
    print("\nStarting Experiment 2: High Contention")
    sim2, replicas2 = setup_system(5)  # 5 replicas for more chaos
    # 30 concurrent increments to the same counter, sprayed across random replicas
    for _ in range(30):
        target_replica = random.randint(1, 5)
        sim2.client_send(dest_id=target_replica, op=('incr', 'shared_counter', 1))
    sim2.run()
    verify_and_print_results(replicas2, "High Contention (30 increments)")

    # Experiment 3: Non-conflicting Updates
    print("\nStarting Experiment 3: Non-conflicting Updates")
    sim3, replicas3 = setup_system(3)
    sim3.client_send(dest_id=1, op=('put', 'user_1', 'Alice'))
    sim3.client_send(dest_id=2, op=('put', 'user_2', 'Bob'))
    sim3.client_send(dest_id=3, op=('put', 'user_3', 'Charlie'))
    sim3.run()
    verify_and_print_results(replicas3, "Non-conflicting Updates")