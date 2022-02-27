//
// Created by Yi Lu on 9/6/18.
//

#pragma once

#include "common/Encoder.h"
#include "common/Message.h"
#include "common/MessagePiece.h"
#include "common/MyMove.h"

namespace star {

enum class ControlMessage { STATISTICS, SIGNAL, ACK, STOP, COUNT, TRANSMIT, NFIELDS };
// COUNT means 统计事务涉及到的record关联性

class ControlMessageFactory {

public:
  static std::size_t new_statistics_message(Message &message, double value) {
    /*
     * The structure of a statistics message: (statistics value : double)
     *
     */

    // the message is not associated with a table or a partition, use 0.
    auto message_size = MessagePiece::get_header_size() + sizeof(double);
    auto message_piece_header = MessagePiece::construct_message_piece_header(
        static_cast<uint32_t>(ControlMessage::STATISTICS), message_size, 0, 0);

    Encoder encoder(message.data);
    encoder << message_piece_header;
    encoder << value;
    message.flush();
    return message_size;
  }

  static std::size_t new_signal_message(Message &message, uint32_t value) {

    /*
     * The structure of a signal message: (signal value : uint32_t)
     */

    // the message is not associated with a table or a partition, use 0.
    auto message_size = MessagePiece::get_header_size() + sizeof(uint32_t);
    auto message_piece_header = MessagePiece::construct_message_piece_header(
        static_cast<uint32_t>(ControlMessage::SIGNAL), message_size, 0, 0);

    Encoder encoder(message.data);
    encoder << message_piece_header;
    encoder << value;
    message.flush();
    return message_size;
  }

  static std::size_t new_ack_message(Message &message) {
    /*
     * The structure of an ack message: ()
     */

    auto message_size = MessagePiece::get_header_size();
    auto message_piece_header = MessagePiece::construct_message_piece_header(
        static_cast<uint32_t>(ControlMessage::ACK), message_size, 0, 0);
    Encoder encoder(message.data);
    encoder << message_piece_header;
    message.flush();
    return message_size;
  }

  static std::size_t new_stop_message(Message &message) {
    /*
     * The structure of a stop message: ()
     */

    auto message_size = MessagePiece::get_header_size();
    auto message_piece_header = MessagePiece::construct_message_piece_header(
        static_cast<uint32_t>(ControlMessage::STOP), message_size, 0, 0);
    Encoder encoder(message.data);
    encoder << message_piece_header;
    message.flush();
    return message_size;
  }



  template <class WorkloadType>
  static std::size_t new_transmit_message(Message &message, 
                                          const myMove<WorkloadType>& move) {
    /**
     * @brief 同一个表的迁移
     * @note 可能为零
     * 
     */
    // int tableId = ycsb::tableID;

    
    int32_t total_key_len = (int32_t)move.records.size();
    if(total_key_len > 30){
      LOG(INFO) << "too large";
    }
    // DCHECK(move.records.size() >= 0);
  
    int32_t key_size = sizeof(u_int64_t);// move.records[0].key_size;
    int32_t normal_size = sizeof(int32_t);

    // int32_t field_size = (total_key_len > 0) ? move.records[0].field_size: 0;

    auto cal_message_length = [&](const myMove<WorkloadType>& move){
      auto message_size = MessagePiece::get_header_size() + 
                          normal_size + // len of keys
                          normal_size;  // dest partition
      // LOG(INFO) << "New message send: ";
      for(size_t i = 0 ; i < move.records.size(); i ++ ){
        // 
        message_size += normal_size +   // src partition
                        key_size +      // key
                        normal_size +   // field size
                        move.records[i].field_size; // field value
        // LOG(INFO) << "[ " << *(int32_t*)& move.records[i].table_id << "] " << move.records[i].src_partition_id << " -> " << move.dest_partition_id;
      }

      return message_size;
    };
    auto message_size = cal_message_length(move);
    // MessagePiece::get_header_size() + 
    //                     key_size +                 // len of keys
    //                     key_size +                 // field_size
    //                     (key_size + key_size + field_size) * total_key_len + // src partition + keys + value 
    //                     key_size;       // dest partition

    // LOG(INFO) << "message_size: " << message_size << " = " << field_size << " * " << total_key_len;

    auto message_piece_header = MessagePiece::construct_message_piece_header(
        static_cast<uint32_t>(ControlMessage::TRANSMIT), message_size, 0, 0);
    Encoder encoder(message.data);
    encoder << message_piece_header;

    encoder.write_n_bytes((void*)&total_key_len, normal_size);
    encoder.write_n_bytes((void*)&move.dest_partition_id, normal_size);

    // encoder.write_n_bytes((void*)&field_size, key_size);

    for (size_t i = 0 ; i < static_cast<size_t>(total_key_len); i ++ ){
      // ITable* table = db.find_table(tableId, move.records[i].src_partition_id);
      auto field_size = move.records[i].field_size;
      encoder.write_n_bytes((void*)&move.records[i].src_partition_id, normal_size);
      encoder.write_n_bytes((void*)&move.records[i].record_key_  , key_size);
      encoder.write_n_bytes((void*)&field_size                      , normal_size);
      encoder.write_n_bytes((void*)&move.records[i].value           , field_size);
    }

    message.flush();
    return message_size;
  }
};

} // namespace star
