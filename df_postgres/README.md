### Network Address Functions
https://www.postgresql.org/docs/16/functions-net.html

| Implemented | Function                | Return Type | Description                                               | Example                                              | Result          |
|-------------|-------------------------|-------------|-----------------------------------------------------------|------------------------------------------------------|-----------------|
|      ❓      | abbrev(inet)            | text        | abbreviated display format as text                        | abbrev(inet '10.1.0.0/16')                          | 10.1.0.0/16     |
|      ❓      | abbrev(cidr)            | text        | abbreviated display format as text                        | abbrev(cidr '10.1.0.0/16')                          | 10.1/16         |
|      ✔      | broadcast(inet)         | inet        | broadcast address for network                             | broadcast('192.168.1.5/24')                         | 192.168.1.255/24|
|      ✔      | family(inet)            | int         | extract family of address; 4 for IPv4, 6 for IPv6         | family('::1')                                      | 6               |
|      ✔      | host(inet)              | text        | extract IP address as text                                | host('192.168.1.5/24')                              | 192.168.1.5     |
|      ✔      | hostmask(inet)          | inet        | construct host mask for network                           | hostmask('192.168.23.20/30')                        | 0.0.0.3         |
|      ✔      | masklen(inet)           | int         | extract netmask length                                    | masklen('192.168.1.5/24')                           | 24              |
|      ✔      | netmask(inet)           | inet        | construct netmask for network                             | netmask('192.168.1.5/24')                           | 255.255.255.0   |
|      ✔      | network(inet)           | cidr        | extract network part of address                           | network('192.168.1.5/24')                           | 192.168.1.0/24  |
|      ✔      | set_masklen(inet, int)  | inet        | set netmask length for inet value                         | set_masklen('192.168.1.5/24', 16)                   | 192.168.1.5/16  |
|      ✔      | set_masklen(cidr, int)  | cidr        | set netmask length for cidr value                         | set_masklen('192.168.1.0/24'::cidr, 16)             | 192.168.0.0/16  |
|      ❓      | text(inet)              | text        | extract IP address and netmask length as text             | text(inet '192.168.1.5')                            | 192.168.1.5/32  |
|      ✔      | inet_same_family(inet, inet) | boolean | are the addresses from the same family?                   | inet_same_family('192.168.1.5/24', '::1')            | false           |
|      ✔      | inet_merge(inet, inet)  | cidr        | the smallest network which includes both of the given networks | inet_merge('192.168.1.5/24', '192.168.2.5/24')    |
