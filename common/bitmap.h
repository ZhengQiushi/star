//
// Created by wangyunlai on 2021/5/7.
//
#pragma once

#include <stdio.h>
#include <string.h>
#include <cmath>
namespace common {

class Bitmap {
public:
  Bitmap();
  Bitmap(int size);
  Bitmap(char* bitmap, int size);
  ~Bitmap();
  /**
   * @brief 
   * 
   * @param size max-size of the bitmap (max-bit size)
   */
  void init(int size);
  bool get_bit(int index);
  void set_bit(int index);
  void clear_bit(int index);

  /**
   * @param start 从哪个位开始查找，start是包含在内的
   */
  int next_unsetted_bit(int start);
  int next_setted_bit(int start);

private:
  char *bitmap_;
  int size_;
};

int find_first_zero(char byte, int start)
{
  for (int i = start; i < 8; i++) {
    if ((byte & (1 << i)) == 0) {
      return i;
    }
  }
  return -1;
}

int find_first_setted(char byte, int start)
{
  for (int i = start; i < 8; i++) {
    if ((byte & (1 << i)) != 0) {
      return i;
    }
  }
  return -1;
}

Bitmap::Bitmap() : bitmap_(nullptr), size_(0)
{}
Bitmap::Bitmap(int size) : size_(size)
{
  init(size);
}
Bitmap::Bitmap(char* bitmap, int size) : bitmap_(bitmap), size_(size){

}

Bitmap::~Bitmap(){
  delete []bitmap_;
}

void Bitmap::init(int size)
{ 
  size_ = size;
  int char_nums = int(std::ceil(size / 8)) + 1;
  bitmap_ = new char[char_nums];
  memset(bitmap_, 0, sizeof(char) * char_nums);
}

bool Bitmap::get_bit(int index)
{
  char bits = bitmap_[index / 8];
  return (bits & (1 << (index % 8))) != 0;
}

void Bitmap::set_bit(int index)
{
  char &bits = bitmap_[index / 8];
  bits |= (1 << (index % 8));
}

void Bitmap::clear_bit(int index)
{
  char &bits = bitmap_[index / 8];
  bits &= ~(1 << (index % 8));
}

int Bitmap::next_unsetted_bit(int start)
{
  int ret = -1;
  int start_in_byte = start % 8;
  for (int iter = start / 8, end = (size_ % 8 == 0 ? size_ / 8 : size_ / 8 + 1); iter <= end; iter++) {
    char byte = bitmap_[iter];
    if (byte != -1) {
      int index_in_byte = find_first_zero(byte, start_in_byte);
      if (index_in_byte >= 0) {
        ret = iter * 8 + index_in_byte;
        break;
      }

      start_in_byte = 0;
    }
  }

  if (ret >= size_) {
    ret = -1;
  }
  return ret;
}

int Bitmap::next_setted_bit(int start)
{
  int ret = -1;
  int start_in_byte = start % 8;
  for (int iter = start / 8, end = (size_ % 8 == 0 ? size_ / 8 : size_ / 8 + 1); iter <= end; iter++) {
    char byte = bitmap_[iter];
    if (byte != 0x00) {
      int index_in_byte = find_first_setted(byte, start_in_byte);
      if (index_in_byte >= 0) {
        ret = iter * 8 + index_in_byte;
        break;
      }

      start_in_byte = 0;
    }
  }

  if (ret >= size_) {
    ret = -1;
  }
  return ret;
}

}  // namespace common
