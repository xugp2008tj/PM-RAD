//
// Copyright Stanislav Seletskiy <s.seletskiy@gmail.com>
//
#ifndef BLOOM_FILTER_H_
#define BLOOM_FILTER_H_


#define BF_LOG2   0.6931471805599453
#define BF_LOG2_2 0.4804530139182013

#define BF_KEY_BUFFER_SIZE 255
#define BF_DUMP_SIGNATURE 0xb100f11e


typedef unsigned int     uint;
typedef unsigned char    bf_bitmap_t;
typedef uint64_t         bf_index_t;
typedef unsigned char    bf_hash_t;

typedef struct bf_s             bf_t;
typedef struct bf_dump_header_s bf_dump_header_t;

struct bf_s
{
	bf_index_t bits_count;
	bf_index_t bits_used;
	bf_index_t bytes_count;

	double error_rate;

	bf_bitmap_t* bitmap;

	bf_index_t (*hash_func)(char*, uint);
	uint hashes_count;
};

struct bf_dump_header_s
{
	uint signature;
	bf_index_t bits_count;
	bf_index_t bits_used;
	double error_rate;
	uint hashes_count;
};

//hash function
bf_index_t murmur(char* key, uint key_len);


bf_t* bf_create(
	double error, bf_index_t key_count,
	bf_index_t (*hash)(char*, uint));

bf_t* bf_load(const char* path,
	bf_index_t (*hash)(char*, uint));

void  bf_add(bf_t* filter, const char* key, uint len);
uint  bf_has(bf_t* filter, const char* key, uint len);
uint  bf_destroy(bf_t* filter);
uint  bf_save(bf_t* filter, const char* path);
void  bf_merge(bf_t* filter1, bf_t* filter2);

uint bf_bitmap_set(bf_bitmap_t* bitmap, bf_index_t bit);
uint bf_bitmap_get(bf_bitmap_t* bitmap, bf_index_t bit);

bf_index_t bf_get_index(bf_t* filter, const char* key, uint key_len, uint hash_index);

#endif
