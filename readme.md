## System Architecture Diagram
Clients
|         \         (clients can send to any replica)
v          v
+----+ +----+ +----+ +----+
 | R1 |  | R2 |   | R3 |   | R4 |
+----+ +----+ +----+ +----+
   \           |          /          |
     \------|------/--------|
      total-order multicast
(TOBCAST + ACK, holdback queues, deliver only when safe)

### How to Run the Code
This project is entirely self-contained in a single Python script. No external libraries or complicated setups are required.

Ensure you have Python 3 installed on your machine.

Open your terminal and navigate to the folder containing the code.

Run the following command:

python totally_ordered_multicast.py

### What to Expect (Experiments)
When you run the script, the built-in network simulator will automatically execute the three required test cases:

Concurrent Conflicting Updates: 3 clients send updates to the same key at the exact same time to 3 different replicas. The log will show that all replicas resolved the order identically.

High Contention: 30 concurrent increment operations are blasted randomly across 5 replicas.

Non-conflicting Updates: Updates to different keys are sent to different replicas, proving the system still enforces total order even when the data doesn't strictly conflict.

At the end of each experiment block in the terminal, you will see a ✅ CORRECTNESS CHECK PASSED message, which automatically verifies that all replicas have identical final states and identical delivery histories.