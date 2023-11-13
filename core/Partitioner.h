//
// Created by Yi Lu on 8/31/18.
//

#pragma once

#include <glog/logging.h>
#include <memory>
#include <numeric>
#include <string>
#include "core/Defs.h"
#include "core/RouterValue.h"
#include "Table.h"
namespace star {

class Partitioner {
public:
  Partitioner(std::size_t coordinator_id, std::size_t coordinator_num) {
    DCHECK(coordinator_id <= coordinator_num);
    this->coordinator_id = coordinator_id;
    this->coordinator_num = coordinator_num;
  }

  virtual ~Partitioner() = default;

  std::size_t total_coordinators() const { return coordinator_num; }

  virtual std::size_t replica_num() const = 0;

  virtual bool is_replicated() const = 0;

  virtual bool has_master_partition(std::size_t partition_id) const = 0;

  virtual std::size_t master_coordinator(std::size_t partition_id) const = 0;

  virtual std::size_t secondary_coordinator(std::size_t partition_id) const = 0;

  
  virtual bool is_partition_replicated_on(std::size_t partition_id,
                                          std::size_t coordinator_id) const = 0;

  bool is_partition_replicated_on_me(std::size_t partition_id) const {
    return is_partition_replicated_on(partition_id, coordinator_id);
  }

  
  virtual bool has_master_partition(int table_id, int partition_id, const void* key) const = 0;

  virtual bool is_dynamic() const = 0;

  virtual std::size_t master_coordinator(int table_id, int partition_id, const void* key) const = 0;
  virtual std::size_t secondary_coordinator(int table_id, int partition_id, const void* key) const = 0;

  virtual std::size_t master_coordinator(int table_id, int partition_id, const void* key, int replica_id) const = 0;
  virtual std::size_t secondary_coordinator(int table_id, int partition_id, const void* key, int replica_id) const = 0;

  // check if the replica of `key` is on Node `coordinator_id` or not
  virtual bool is_partition_replicated_on(int table_id, int partition_id, const void* key,
                                  std::size_t coordinator_id) const = 0;


  virtual bool is_backup() const = 0;

protected:
  std::size_t coordinator_id;
  std::size_t coordinator_num;
};

/*
 * N is the total number of replicas.
 * N is always larger than 0.
 * The N coordinators from the master coordinator have the replication for a
 * given partition.
 */

template <std::size_t N> class HashReplicatedPartitioner : public Partitioner {
public:
  HashReplicatedPartitioner(std::size_t coordinator_id,
                            std::size_t coordinator_num)
      : Partitioner(coordinator_id, coordinator_num) {
    // CHECK(N > 0 && N <= coordinator_num);
  }

  ~HashReplicatedPartitioner() override = default;

  std::size_t replica_num() const override { return N; }

  bool is_replicated() const override { return N > 1; }

  bool has_master_partition(std::size_t partition_id) const override {
    return master_coordinator(partition_id) == coordinator_id;
  }

  std::size_t master_coordinator(std::size_t partition_id) const override {
    return partition_id % coordinator_num;
  }
  std::size_t secondary_coordinator(std::size_t partition_id) const override {
    return (partition_id + 1) % coordinator_num;
  }
  bool is_partition_replicated_on(std::size_t partition_id,
                                  std::size_t coordinator_id) const override {
    DCHECK(coordinator_id <= coordinator_num);
    if(coordinator_num == 1){
      return true;
    }
    std::size_t first_replica = master_coordinator(partition_id);
    std::size_t last_replica = (first_replica + N - 1) % coordinator_num;

    if (last_replica >= first_replica) {
      return first_replica <= coordinator_id && coordinator_id <= last_replica;
    } else {
      return coordinator_id >= first_replica || coordinator_id <= last_replica;
    }
  }


  
  bool has_master_partition(int table_id, int partition_id, const void* key) const override { // std::size_t partition_id, 
    // DCHECK(false);
    return master_coordinator(partition_id) == coordinator_id;// false;
  }

  
  std::size_t master_coordinator(int table_id, int partition_id, const void* key) const override {
    // DCHECK(false);
    return master_coordinator(partition_id); // false;
  }
  std::size_t secondary_coordinator(int table_id, int partition_id, const void* key) const override {
    return secondary_coordinator(partition_id); // false;
  }
  
  std::size_t master_coordinator(int table_id, int partition_id, const void* key, int replica_id) const override {
    DCHECK(replica_id < 2);
    if(replica_id == 0){
      return master_coordinator(partition_id); // false;
    } else {
      return secondary_coordinator(partition_id); // false;
    }
  }
  std::size_t secondary_coordinator(int table_id, int partition_id, const void* key, int replica_id) const override {
    DCHECK(false);
    return secondary_coordinator(partition_id); // false;
  }

  bool is_partition_replicated_on(int table_id, int partition_id, const void* key,
                                  std::size_t coordinator_id) const override {
    // DCHECK(false);
    return is_partition_replicated_on(partition_id, coordinator_id); //false;
  }

  bool is_backup() const override { return false; }

  bool is_dynamic() const override { return false; };
};

using HashPartitioner = HashReplicatedPartitioner<1>;

class PrimaryBackupPartitioner : public Partitioner {
public:
  PrimaryBackupPartitioner(std::size_t coordinator_id,
                           std::size_t coordinator_num)
      : Partitioner(coordinator_id, coordinator_num) {
    CHECK(coordinator_num == 2);
  }

  ~PrimaryBackupPartitioner() override = default;

  std::size_t replica_num() const override { return 2; }

  bool is_replicated() const override { return true; }

  bool has_master_partition(std::size_t partition_id) const override {
    return coordinator_id == 0;
  }

  std::size_t master_coordinator(std::size_t partition_id) const override {
    return 0;
  }
  std::size_t secondary_coordinator(std::size_t partition_id) const override {
    return (partition_id) % coordinator_num;
  }

  bool is_partition_replicated_on(std::size_t partition_id,
                                  std::size_t coordinator_id) const override {
    DCHECK(coordinator_id <= coordinator_num);
    return true;
  }
  
  bool has_master_partition(int table_id, int partition_id, const void* key) const override { // std::size_t partition_id, 
    DCHECK(false);
    return false;
  }

  
  std::size_t master_coordinator(int table_id, int partition_id, const void* key) const override {
    return master_coordinator(partition_id);

  }
  std::size_t secondary_coordinator(int table_id, int partition_id, const void* key) const override {
    return secondary_coordinator(partition_id); // false;

  }
  
  std::size_t master_coordinator(int table_id, int partition_id, const void* key, int replica_id) const override {
    DCHECK(false);
    return master_coordinator(partition_id); // false;
  }
  std::size_t secondary_coordinator(int table_id, int partition_id, const void* key, int replica_id) const override {
    DCHECK(false);
    return secondary_coordinator(partition_id); // false;
  }
  bool is_partition_replicated_on(int table_id, int partition_id, const void* key,
                                  std::size_t coordinator_id) const override {
    DCHECK(false);
    return false;
  }
  bool is_backup() const override { return coordinator_id == 1; }

  bool is_dynamic() const override { return false; };

};

/*
 * There are 2 replicas in the system with N coordinators.
 * Coordinator 0 has a full replica.
 * The other replica is partitioned across coordinator 1 and coordinator N - 1
 *
 *
 * The master partition is partition id % N.
 *
 * case 1
 * If the master partition is from coordinator 1 to coordinator N - 1,
 * the secondary partition is on coordinator 0.
 *
 * case 2
 * If the master partition is on coordinator 0,
 * the secondary partition is from coordinator 1 to coordinator N - 1.
 *
 */

class StarSPartitioner : public Partitioner {
public:
  StarSPartitioner(std::size_t coordinator_id, std::size_t coordinator_num)
      : Partitioner(coordinator_id, coordinator_num) {
    // CHECK(coordinator_num >= 2);
  }

  ~StarSPartitioner() override = default;

  std::size_t replica_num() const override { return 2; }

  bool is_replicated() const override { return true; }

  bool has_master_partition(std::size_t partition_id) const override {
    return master_coordinator(partition_id) == coordinator_id;
  }

  std::size_t master_coordinator(std::size_t partition_id) const override {
    return partition_id % coordinator_num;
  }
  std::size_t secondary_coordinator(std::size_t partition_id) const override {
    return (partition_id) % coordinator_num;
  }
  bool is_partition_replicated_on(std::size_t partition_id,
                                  std::size_t coordinator_id) const override {
    DCHECK(coordinator_id <= coordinator_num);
    if(coordinator_num == 1){
      return true;
    }

    auto master_id = master_coordinator(partition_id);
    auto secondary_id = 0u; // case 1
    if (master_id == 0) {
      secondary_id = partition_id % (coordinator_num - 1) + 1; // case 2
    }
    return coordinator_id == master_id || coordinator_id == secondary_id;
  }
  
  bool has_master_partition(int table_id, int partition_id, const void* key) const override { // std::size_t partition_id, 
    DCHECK(false);
    return false;
  }

  
  std::size_t master_coordinator(int table_id, int partition_id, const void* key) const override {
    return master_coordinator(partition_id);
  }
  std::size_t secondary_coordinator(int table_id, int partition_id, const void* key) const override {
    return secondary_coordinator(partition_id); // false;

  }
  
    std::size_t master_coordinator(int table_id, int partition_id, const void* key, int replica_id) const override {
    DCHECK(false);
    return master_coordinator(partition_id); // false;
  }
  std::size_t secondary_coordinator(int table_id, int partition_id, const void* key, int replica_id) const override {
    DCHECK(false);
    return secondary_coordinator(partition_id); // false;
  }
  bool is_partition_replicated_on(int table_id, int partition_id, const void* key,
                                  std::size_t coordinator_id) const override {
    DCHECK(false);
    return false;
  }
  bool is_backup() const override { return false; }

  bool is_dynamic() const override { return false; };

};

class StarCPartitioner : public Partitioner {
public:
  StarCPartitioner(std::size_t coordinator_id, std::size_t coordinator_num)
      : Partitioner(coordinator_id, coordinator_num) {
    // CHECK(coordinator_num >= 2);
  }

  ~StarCPartitioner() override = default;

  std::size_t replica_num() const override { return 2; }

  bool is_replicated() const override { return true; }

  bool has_master_partition(std::size_t partition_id) const override {
    return coordinator_id == 0;
  }

  std::size_t master_coordinator(std::size_t partition_id) const override {
    return 0;
  }

  std::size_t secondary_coordinator(std::size_t partition_id) const override {
    return (partition_id) % coordinator_num;
  }
  bool is_partition_replicated_on(std::size_t partition_id,
                                  std::size_t coordinator_id) const override {
    DCHECK(coordinator_id <= coordinator_num);
    if(coordinator_num == 1){
      return true;
    }
    if (coordinator_id == 0)
      return true;
    // 非全副本节点需要判断
    if(partition_id % coordinator_num == 0){
      return coordinator_id == (partition_id / coordinator_num) % (coordinator_num - 1) + 1;
    } else {
      return coordinator_id == partition_id % coordinator_num;
                              //partition_id % (coordinator_num - 1) + 1; 
                              //((partition_id - 1) % (coordinator_num - 1)) + 1;
    }
  }
  
  bool has_master_partition(int table_id, int partition_id, const void* key) const override { // std::size_t partition_id, 
    DCHECK(false);
    return false;
  }

  
  std::size_t master_coordinator(int table_id, int partition_id, const void* key) const override {
    return master_coordinator(partition_id);

  }
  std::size_t secondary_coordinator(int table_id, int partition_id, const void* key) const override {
    return secondary_coordinator(partition_id); // false;

  }
    std::size_t master_coordinator(int table_id, int partition_id, const void* key, int replica_id) const override {
    DCHECK(false);
    return master_coordinator(partition_id); // false;
  }
  std::size_t secondary_coordinator(int table_id, int partition_id, const void* key, int replica_id) const override {
    DCHECK(false);
    return secondary_coordinator(partition_id); // false;
  }
  bool is_partition_replicated_on(int table_id, int partition_id, const void* key,
                                  std::size_t coordinator_id) const override {
    DCHECK(false);
    return false;
  }
  bool is_backup() const override { return coordinator_id != 0; }

  bool is_dynamic() const override { return false; };
};

/***
 * Lion Partitioner
 * Features:
 *      - 2 full replicas
 *      - 2 kinds of replicas: + (stands for dynamic replica | master    ), 
 *                             - (stands for static replica  | secondary )
 *      - replicas will change as time goes by accordding to the locality.
 * Here is an example(inital state):
 * 
 *     | N0     |  N1    |  N2    |
 * P0  | -      |  +     |        |
 * P1  |        |  -     |  +     |
 * P2  | +      |        |  -     |
 * P3  | -      |  +     |        |
 * P4  |        |  -     |  +     |
 * P5  | +      |        |  -     |
 * 
 * ==> only the dynamic replica can be moved 
 * 
 *     | N0     |  N1    |  N2    |
 * P0  | -      |        |  +     |
 * P1  |        |  -     |  +     |
 * P2  | +      |        |  -     |
 * P3  | -      |  +     |        |
 * P4  | +      |  -     |        |
 * P5  | +      |        |  -     |
 * 
*/

class LionInitPartitioner : public Partitioner {
public:
  LionInitPartitioner(std::size_t coordinator_id, std::size_t coordinator_num)
      : Partitioner(coordinator_id, coordinator_num) {
    CHECK(coordinator_num >= 1);
  }

  ~LionInitPartitioner() override = default;

  std::size_t replica_num() const override { return 2; }

  bool is_replicated() const override { return true; }

  bool has_master_partition(std::size_t partition_id) const override {
    return master_coordinator(partition_id) == coordinator_id;
  }

  std::size_t master_coordinator(std::size_t partition_id) const override {
    // the dynamic replica is a unit after static replica at initial state
    return (partition_id + 1) % coordinator_num;
  }

  std::size_t secondary_coordinator(std::size_t partition_id) const override {
    // the static replica is master at initial state
    return (partition_id) % coordinator_num;
  }
  bool is_partition_replicated_on(std::size_t partition_id,
                                  std::size_t coordinator_id) const override {
    // judge if it is replicated partition or not
    DCHECK(coordinator_id <= coordinator_num);
    if(coordinator_num == 1){
      return true;
    }
    auto master_id = master_coordinator(partition_id);
    auto secondary_id = secondary_coordinator(partition_id); // case 1
    
    return coordinator_id == master_id || coordinator_id == secondary_id;
  }
  
  bool has_master_partition(int table_id, int partition_id, const void* key) const override { // std::size_t partition_id, 
    DCHECK(false);
    return false;
  }
  std::size_t secondary_coordinator(int table_id, int partition_id, const void* key) const override {
    return secondary_coordinator(partition_id); // false;
  }
  
  std::size_t master_coordinator(int table_id, int partition_id, const void* key) const override {
    DCHECK(false);
    return false;

  }

    std::size_t master_coordinator(int table_id, int partition_id, const void* key, int replica_id) const override {
    DCHECK(false);
    return master_coordinator(partition_id); // false;
  }
  std::size_t secondary_coordinator(int table_id, int partition_id, const void* key, int replica_id) const override {
    DCHECK(false);
    return secondary_coordinator(partition_id); // false;
  }
  bool is_partition_replicated_on(int table_id, int partition_id, const void* key,
                                  std::size_t coordinator_id) const override {
    DCHECK(false);
    return false;
  }
  bool is_backup() const override { return false; }

  bool is_dynamic() const override { return false; };


};

template <std::size_t N> class LionInitReplicaPartitioner : public Partitioner {
public:
  LionInitReplicaPartitioner(std::size_t coordinator_id, std::size_t coordinator_num)
      : Partitioner(coordinator_id, coordinator_num) {
    CHECK(coordinator_num >= 1);
  }

  ~LionInitReplicaPartitioner() override = default;

  std::size_t replica_num() const override { return N; }

  bool is_replicated() const override { return true; }

  bool has_master_partition(std::size_t partition_id) const override {
    return master_coordinator(partition_id) == coordinator_id;
  }

  std::size_t master_coordinator(std::size_t partition_id) const override {
    // the dynamic replica is a unit after static replica at initial state
    return (partition_id + 1) % coordinator_num;
  }

  std::size_t secondary_coordinator(std::size_t partition_id) const override {
    // the static replica is master at initial state
    DCHECK(false);
    return (partition_id) % coordinator_num;
  }
  bool is_partition_replicated_on(std::size_t partition_id,
                                  std::size_t coordinator_id) const override {
    // judge if it is replicated partition or not
    DCHECK(coordinator_id <= coordinator_num);
    if(coordinator_num == 1){
      return true;
    }
    // 
    auto last_replica = master_coordinator(partition_id); // partition_id + 1
    //  [partition_id - N + 1, partition_id]
    std::size_t first_replica = (last_replica - N + 1 + coordinator_num) % coordinator_num;
    //
    if (last_replica >= first_replica) {
      return first_replica <= coordinator_id && coordinator_id <= last_replica;
    } else {
      return coordinator_id >= first_replica || coordinator_id <= last_replica;
    }
  }
  
  bool has_master_partition(int table_id, int partition_id, const void* key) const override { // std::size_t partition_id, 
    DCHECK(false);
    return false;
  }
  std::size_t secondary_coordinator(int table_id, int partition_id, const void* key) const override {
    return secondary_coordinator(partition_id); // false;
  }
  
  std::size_t master_coordinator(int table_id, int partition_id, const void* key) const override {
    DCHECK(false);
    return false;

  }

    std::size_t master_coordinator(int table_id, int partition_id, const void* key, int replica_id) const override {
    DCHECK(false);
    return master_coordinator(partition_id); // false;
  }
  std::size_t secondary_coordinator(int table_id, int partition_id, const void* key, int replica_id) const override {
    DCHECK(false);
    return secondary_coordinator(partition_id); // false;
  }
  bool is_partition_replicated_on(int table_id, int partition_id, const void* key,
                                  std::size_t coordinator_id) const override {
    DCHECK(false);
    return false;
  }
  bool is_backup() const override { return false; }

  bool is_dynamic() const override { return false; };


};


template <class Workload> 
class LionDynamicPartitioner : public LionInitPartitioner {
public:
  using WorkloadType = Workload;
  using DatabaseType = typename WorkloadType::DatabaseType;

  LionDynamicPartitioner(std::size_t coordinator_id, std::size_t coordinator_num, DatabaseType& db):
    LionInitPartitioner(coordinator_id, coordinator_num), db(db){
    CHECK(coordinator_num >= 1);
  }

  ~LionDynamicPartitioner() override = default;

  std::size_t replica_num() const override { return 2; }

  bool is_replicated() const override { return true; }

  
  bool has_master_partition(int table_id, int partition_id, const void* key) const override { // std::size_t partition_id, 
    return master_coordinator(table_id, partition_id, key) == coordinator_id;
  }

  std::size_t master_coordinator(int table_id, int partition_id, const void* key) const override {
    /**
     * @brief dynamic mastership. master piece will be moved to other coordinators!
     * 
     */
    return db.get_dynamic_coordinator_id(coordinator_num, table_id, key);// *master_coordinator;
  }
  std::size_t secondary_coordinator(int table_id, int partition_id, const void* key) const override {
    auto router_table = db.find_router_table(table_id);// , master_coordinator_id);
    auto router_val = static_cast<RouterValue*>(router_table->search_value(key));
    return router_val->get_secondary_coordinator_id();
  }

  bool is_partition_replicated_on(int table_id, int partition_id, const void* key,
                                  std::size_t coordinator_id) const override {
    DCHECK(coordinator_id <= coordinator_num);
      if(coordinator_num == 1){
        return true;
      }
      // db.get_dynamic_coordinator_id(coordinator_num, table_id, key);
      auto router_table = db.find_router_table(table_id);//, master_coordinator_id);
      auto router_val = (RouterValue*)(router_table->search_value(key));
      uint64_t secondary_coordinator_ids = router_val->get_secondary_coordinator_id(); 
      return ((secondary_coordinator_ids >> coordinator_id) & 1);// all_replicas->second.count(coordinator_id);
  }


  bool has_master_partition(std::size_t partition_id) const override {
    DCHECK(false);
    return false; // master_coordinator(partition_id) == coordinator_id;
  }

  std::size_t master_coordinator(std::size_t partition_id) const override {
    // the static replica is master at initial state
    DCHECK(false);
    return (partition_id) % coordinator_num;
  }

  std::size_t secondary_coordinator(std::size_t partition_id) const override {
    // the static replica is master at initial state
    DCHECK(false);
    return (partition_id) % coordinator_num;
  }
  std::size_t master_coordinator(int table_id, int partition_id, const void* key, int replica_id) const override {
    DCHECK(false);
    return master_coordinator(partition_id); // false;
  }
  std::size_t secondary_coordinator(int table_id, int partition_id, const void* key, int replica_id) const override {
    DCHECK(false);
    return secondary_coordinator(partition_id); // false;
  }
  bool is_partition_replicated_on(std::size_t partition_id,
                                  std::size_t coordinator_id) const override {
    // judge if it is replicated partition or not
    DCHECK(false);    
    return true;// coordinator_id == master_id || coordinator_id == secondary_id;
  }
  
  bool is_backup() const override { return coordinator_id != 0; }

  bool is_dynamic() const override { return true; };


private:
  DatabaseType& db;
};

template <class Workload> 
class LionStaticPartitioner : public LionInitPartitioner {
public:
  using WorkloadType = Workload;
  using DatabaseType = typename WorkloadType::DatabaseType;

  LionStaticPartitioner(std::size_t coordinator_id, std::size_t coordinator_num, DatabaseType& db):
    LionInitPartitioner(coordinator_id, coordinator_num), db(db){
    CHECK(coordinator_num >= 1);
  }

  ~LionStaticPartitioner() override = default;

  std::size_t replica_num() const override { return 2; }

  bool is_replicated() const override { return true; }

  
  bool has_master_partition(int table_id, int partition_id, const void* key) const override { // std::size_t partition_id, 
    return master_coordinator(table_id, partition_id, key) == coordinator_id;
  }

  std::size_t master_coordinator(int table_id, int partition_id, const void* key) const override {
    // the static replica is master at initial state
    return (partition_id) % coordinator_num;
  }
  std::size_t secondary_coordinator(int table_id, int partition_id, const void* key) const override {
    auto router_table = db.find_router_table(table_id);// , master_coordinator_id);
    auto router_val = static_cast<RouterValue*>(router_table->search_value(key));
    return router_val->get_secondary_coordinator_id();
  }
  bool is_partition_replicated_on(int table_id, int partition_id, const void* key,
                                  std::size_t coordinator_id) const override {
    // check if has dynamic replica on coordinator
    DCHECK(coordinator_id <= coordinator_num);
      if(coordinator_num == 1){
        return true;
      }
      // db.get_dynamic_coordinator_id(coordinator_num, table_id, key);
      auto router_table = db.find_router_table(table_id);//, master_coordinator_id);
      auto router_val = (RouterValue*)(router_table->search_value(key));
      uint64_t secondary_coordinator_ids = router_val->get_secondary_coordinator_id(); 
      
      return ((secondary_coordinator_ids >> coordinator_id) & 1);
  }


  bool has_master_partition(std::size_t partition_id) const override {
    DCHECK(false);
    return false; // master_coordinator(partition_id) == coordinator_id;
  }

  std::size_t master_coordinator(std::size_t partition_id) const override {
    // the static replica is master at initial state
    DCHECK(false);
    return 0; // (partition_id + 1) % coordinator_num;
  }

  std::size_t secondary_coordinator(std::size_t partition_id) const override {
    // the static replica is master at initial state
    DCHECK(false);
    return 0; // (partition_id) % coordinator_num;
  }
  std::size_t master_coordinator(int table_id, int partition_id, const void* key, int replica_id) const override {
    DCHECK(false);
    return master_coordinator(partition_id); // false;
  }
  std::size_t secondary_coordinator(int table_id, int partition_id, const void* key, int replica_id) const override {
    DCHECK(false);
    return secondary_coordinator(partition_id); // false;
  }
  bool is_partition_replicated_on(std::size_t partition_id,
                                  std::size_t coordinator_id) const override {
    // judge if it is replicated partition or not
    DCHECK(false);    
    return true;// coordinator_id == master_id || coordinator_id == secondary_id;
  }

  bool is_backup() const override { return coordinator_id != 0; }

  bool is_dynamic() const override { return false; };

private:
  DatabaseType& db;
};

class CalvinPartitioner : public Partitioner {

public:
  CalvinPartitioner(std::size_t coordinator_id, std::size_t coordinator_num,
                    std::vector<std::size_t> replica_group_sizes)
      : Partitioner(coordinator_id, coordinator_num) {

    std::size_t size = 0;
    for (auto i = 0u; i < replica_group_sizes.size(); i++) {
      CHECK(replica_group_sizes[i] > 0);
      size += replica_group_sizes[i];

      if (coordinator_id < size) {
        coordinator_start_id = size - replica_group_sizes[i];
        replica_group_id = i;
        replica_group_size = replica_group_sizes[i];
        break;
      }
    }
    CHECK(std::accumulate(replica_group_sizes.begin(),
                          replica_group_sizes.end(), 0u) == coordinator_num);
  }

  ~CalvinPartitioner() override = default;

  std::size_t replica_num() const override { return replica_group_size; }

  bool is_replicated() const override {
    // replica group in calvin is independent
    return false;
  }

  bool has_master_partition(std::size_t partition_id) const override {
    return master_coordinator(partition_id) == coordinator_id;
  }

  std::size_t master_coordinator(std::size_t partition_id) const override {
    // return partition_id % replica_group_size + coordinator_start_id;
    return partition_id % coordinator_num;
  }
  std::size_t secondary_coordinator(std::size_t partition_id) const override {
    return (partition_id + 1) % coordinator_num;
  }
  bool is_partition_replicated_on(std::size_t partition_id,
                                  std::size_t coordinator_id) const override {
    if(coordinator_num == 1){
      return true;
    }
    // replica group in calvin is independent
    return master_coordinator(partition_id) == coordinator_id || 
           secondary_coordinator(partition_id) == coordinator_id;
  }
  
  bool has_master_partition(int table_id, int partition_id, const void* key) const override { // std::size_t partition_id, 
    DCHECK(false);
    return false;
  }

  
  std::size_t master_coordinator(int table_id, int partition_id, const void* key) const override {
    DCHECK(false);
    return false;

  }
  std::size_t secondary_coordinator(int table_id, int partition_id, const void* key) const override {
    DCHECK(false);
    return secondary_coordinator(partition_id); // false;
  }
  std::size_t master_coordinator(int table_id, int partition_id, const void* key, int replica_id) const override {
    DCHECK(false);
    return master_coordinator(partition_id); // false;
  }
  std::size_t secondary_coordinator(int table_id, int partition_id, const void* key, int replica_id) const override {
    DCHECK(false);
    return secondary_coordinator(partition_id); // false;
  }
  bool is_partition_replicated_on(int table_id, int partition_id, const void* key,
                                  std::size_t coordinator_id) const override {
    DCHECK(false);
    return false;
  }
  bool is_backup() const override { return false; }

  bool is_dynamic() const override { return false; };


public:
  std::size_t replica_group_id;
  std::size_t replica_group_size;

private:
  // the first coordinator in this replica group
  std::size_t coordinator_start_id;
};

template <class Workload> 
class HermesPartitioner : public Partitioner {
  using WorkloadType = Workload;
  using DatabaseType = typename WorkloadType::DatabaseType;

public:
  HermesPartitioner(std::size_t coordinator_id, std::size_t coordinator_num,
                    std::vector<std::size_t> replica_group_sizes, DatabaseType& db)
      : Partitioner(coordinator_id, coordinator_num), db(db) {
    coordinator_start_id = 0;
    replica_group_id = 0;
    replica_group_size = coordinator_num;
  }

  ~HermesPartitioner() override = default;

  std::size_t replica_num() const override { return 2; }

  bool is_replicated() const override {
    // replica group in calvin is independent
    return false;
  }

  bool has_master_partition(std::size_t partition_id) const override {
    DCHECK(false);
    return master_coordinator(partition_id) == coordinator_id;
  }

  std::size_t master_coordinator(std::size_t partition_id) const override {
    DCHECK(false);
    return partition_id % replica_group_size + coordinator_start_id;
  }
  std::size_t secondary_coordinator(std::size_t partition_id) const override {
    return (partition_id + 1) % coordinator_num;
  }
  bool is_partition_replicated_on(std::size_t partition_id,
                                  std::size_t coordinator_id) const override {
    // replica group in calvin is independent
    DCHECK(false);

    return master_coordinator(partition_id) == coordinator_id || 
           secondary_coordinator(partition_id) == coordinator_id;
  }
  
  bool has_master_partition(int table_id, int partition_id, const void* key) const override { // std::size_t partition_id, 
    return master_coordinator(table_id, partition_id, key) == coordinator_id;
  }

  
  std::size_t master_coordinator(int table_id, int partition_id, const void* key) const override {
    DCHECK(false);
    return db.get_dynamic_coordinator_id(coordinator_num, table_id, key);

  }
  std::size_t secondary_coordinator(int table_id, int partition_id, const void* key) const override {
    DCHECK(false);
    auto master_coordinator_id = master_coordinator(table_id, partition_id, key);
    auto router_table = db.find_router_table(table_id, master_coordinator_id);
    size_t secondary_coordinator_id = *(size_t*)router_table->search_value(key);
    return secondary_coordinator_id;
  }
  
  std::size_t master_coordinator(int table_id, int partition_id, const void* key, int replica_id) const override {
    return db.get_dynamic_coordinator_id(coordinator_num, table_id, key, replica_id); // false;
  }
  std::size_t secondary_coordinator(int table_id, int partition_id, const void* key, int replica_id) const override {
    DCHECK(false);
    return secondary_coordinator(partition_id); // false;
  }

  bool is_partition_replicated_on(int table_id, int partition_id, const void* key,
                                  std::size_t coordinator_id) const override {
    DCHECK(coordinator_id <= coordinator_num);
    if(coordinator_num == 1){
      return true;
    }
    // static replica
    auto master_coordinator_id = master_coordinator(table_id, partition_id, key);
    if(master_coordinator_id == coordinator_id){
      return true;
    } else {
      auto router_table = db.find_router_table(table_id, master_coordinator_id);
      size_t secondary_coordinator_id = *(size_t*)router_table->search_value(key);
      VLOG(DEBUG_V12) << *(int*)key << " " << coordinator_id << " " << secondary_coordinator_id; 
      return secondary_coordinator_id == coordinator_id;
    }
  }

  bool is_backup() const override { return false; }

  bool is_dynamic() const override { return false; };


public:
  std::size_t replica_group_id;
  std::size_t replica_group_size;

private:
  // the first coordinator in this replica group
  std::size_t coordinator_start_id;
  DatabaseType& db;
};

class PartitionerFactory {
public:
  static std::unique_ptr<Partitioner>
  create_partitioner(const std::string &part, std::size_t coordinator_id,
                     std::size_t coordinator_num) {
    // number means the number of replica 
    if (part == "hash") {
      return std::make_unique<HashPartitioner>(coordinator_id, coordinator_num);
    } else if (part == "hash2") {
      return std::make_unique<HashReplicatedPartitioner<2>>(coordinator_id,
                                                            coordinator_num);
    } else if (part == "hash3") {
      return std::make_unique<HashReplicatedPartitioner<3>>(coordinator_id,
                                                            coordinator_num);
    } else if (part == "hash4") {
      return std::make_unique<HashReplicatedPartitioner<4>>(coordinator_id,
                                                            coordinator_num);
    } else if (part == "hash5") {
      return std::make_unique<HashReplicatedPartitioner<5>>(coordinator_id,
                                                            coordinator_num);
    } else if (part == "hash6") {
      return std::make_unique<HashReplicatedPartitioner<6>>(coordinator_id,
                                                            coordinator_num);
    } else if (part == "hash7") {
      return std::make_unique<HashReplicatedPartitioner<7>>(coordinator_id,
                                                            coordinator_num);
    } else if (part == "hash8") {
      return std::make_unique<HashReplicatedPartitioner<8>>(coordinator_id,
                                                            coordinator_num);
    } else if (part == "pb") {
      return std::make_unique<PrimaryBackupPartitioner>(coordinator_id,
                                                        coordinator_num);
    } else if (part == "StarS") {
      return std::make_unique<StarSPartitioner>(coordinator_id,
                                                coordinator_num);
    } else if (part == "StarC") {
      return std::make_unique<StarCPartitioner>(coordinator_id,
                                                coordinator_num);
    } else if (part == "Lion") {
      return std::make_unique<LionInitPartitioner>(coordinator_id,
                                                   coordinator_num);
    } else if (part == "Lion1") {
      return std::make_unique<LionInitReplicaPartitioner<1>>(coordinator_id,
                                                   coordinator_num);
    } else if (part == "Lion2") {
      return std::make_unique<LionInitReplicaPartitioner<2>>(coordinator_id,
                                                   coordinator_num);
    } else if (part == "Lion3") {
      return std::make_unique<LionInitReplicaPartitioner<3>>(coordinator_id,
                                                   coordinator_num);
    } else if (part == "Lion4") {
      return std::make_unique<LionInitReplicaPartitioner<4>>(coordinator_id,
                                                   coordinator_num);
    } else {
      CHECK(false);
      return nullptr;
    }
  }
};

} // namespace star