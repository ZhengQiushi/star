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




  static std::size_t new_transmit_message(Message &message, const myMove& move) {

    auto key_size = sizeof(int32_t);
    
    int32_t total_key_len = (int32_t)move.keys.size();

    auto message_size = MessagePiece::get_header_size() +
                        key_size +                 // len of keys
                        key_size * total_key_len + // keys
                        key_size + key_size;       // src and dest partition

    auto message_piece_header = MessagePiece::construct_message_piece_header(
        static_cast<uint32_t>(ControlMessage::TRANSMIT), message_size, 0, 0);
    Encoder encoder(message.data);
    encoder << message_piece_header;

    encoder.write_n_bytes((void*)&total_key_len, key_size);
    for (size_t i = 0 ; i < move.keys.size(); i ++ ){
      auto key = move.keys[i];
      encoder.write_n_bytes((void*)&key, key_size);
    }

    encoder.write_n_bytes((void*)&move.src_partition_id, key_size);
    encoder.write_n_bytes((void*)&move.dest_partition_id, key_size);

    message.flush();
    return message_size;
  }
};

} // namespace star
