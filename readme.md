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
