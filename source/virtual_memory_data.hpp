/*************************************************************************
	> File Name: virtual_memory_data.hpp
	> Author: kaijiang
	> Mail: kaijiangchen@gmail.com
	> Created Time: Mon 18 Nov 2013 09:56:28 AM CST
 ************************************************************************/
#ifndef VIRTUAL_MEMORY_DATA_H
#define VIRTUAL_MEMORY_DATA_H

#include <stdint.h>
#include <vector>
#include "virtual_memory_mapper.hpp"
#include <iostream>
#include <fstream>
#include <string>
#include <algorithm>
#include <set>
#include <boost/algorithm/string.hpp>

using namespace std;

namespace kaijiang_api
{
	void ConstructVirtualMemoryDataFileName(std::string& file)
	{
        boost::algorithm::trim(file); 
        std::string ex1 = ".block";
        std::string ex2 = ".index";
		size_t pos = file.rfind(ex1);
		if(pos != file.npos && pos + ex1.size() == file.size())
		{
			file = file.substr(0, pos);
			return;
		}
		pos = file.rfind(ex2);
		if(pos != file.npos && pos + ex2.size() == file.size())
		{
			file = file.substr(0, pos);
			return;
		}
	}

	// description: definition of data index
	// member:
	//  id -- the data's id, 64 bit
	//  off -- the position of data block in the file
	//  size -- the bytes of data block
	typedef struct 
	{
		uint64_t id;		// id, the key
		uint64_t off;		// start index in the BlockData array
		uint32_t size;		// BlockData number
	}VirtualMemoryDataIndex;

	bool CmpMemoryIndexId(const VirtualMemoryDataIndex& index1, const VirtualMemoryDataIndex& index2)
	{
		return index1.id < index2.id;
	}
	
    // description: scan block data via this class's instance
    template<typename BlockData>
        class BlockDataScanner
        {
            public:
                BlockDataScanner()
                {
                }

                virtual ~BlockDataScanner()
                {
                }
				
                virtual bool Process(const BlockData& data_unit, const uint32_t sequence_num, void* resource) = 0;

        };

	// description: implement of HandleBlockDataUnit: to copy block data
	// parameters:
	//  [IN] data_unit each data unit
	//  [IN] sequence_num -- current sequence
	//  [OUT] esource -- self-defined resource
	// return:
	//  true -- copy success
	template<typename BlockData>
		bool CopyHandler(const BlockData& data_unit, const uint32_t sequence_num, void* resource)
		{
			BlockData* block_data = (BlockData*)resource;
			block_data[sequence_num] = data_unit;

			return true;
		}

    // description:scan block data, using BlockDataScanner
    template<typename BlockData>
        struct ResouceUsingBlockDataScanner
        {
            BlockDataScanner<BlockData>* scanner;
            void* resource;
        };
    template <typename BlockData>
        bool UsingBlockDataScanner(const BlockData& data_unit, const uint32_t sequence_num, void* resource)
        {
            ResouceUsingBlockDataScanner<BlockData>* rsc = (ResouceUsingBlockDataScanner<BlockData>*)resource;
            if(rsc == NULL)
                return false;
            return rsc->scanner->Process(data_unit, sequence_num, rsc->resource);
        }

    template<typename BlockData>
        class VirtualMemoryData
        {
            public:
                // description: handle each block data unit when scanning the block list
                // parameters:
                //  [IN] data_unit -- each data unit
                //  [IN] sequence_num -- current sequence 
                //  [IN/OUT] resource -- self-defined resource
                // return:
                //  true -- success
                typedef bool (*HandleBlockDataUnit)(const BlockData& data_unit, const uint32_t sequence_num, void* resource);

            private:
                std::vector<VirtualMemoryDataIndex> sorted_binary_index_list;
                VirtualMemoryMapper* virtual_memory_mapper;
                uint32_t block_size;

            private:
                // descrption: get data location
                // parameters:
                //  [IN] id -- the id
                //  [OUT] off -- the offset in the block list
                //  [OUT] size -- the block data size
                // return:
                //  true -- success
                bool GetLocation(const uint64_t id, uint64_t& off, uint32_t& size) const 
                {
                    if(id == 0 || sorted_binary_index_list.size() == 0)
                    {
#ifdef DEBUG
                        cerr<<"id == 0 || sorted_binary_index_list.size() == 0, id="<<id<<endl;
#endif
                        return false;
                    }
                    VirtualMemoryDataIndex index_query;
                    index_query.id = id;
                    std::vector<VirtualMemoryDataIndex>::const_iterator low_index = std::lower_bound (sorted_binary_index_list.begin(), 
                            sorted_binary_index_list.end(), 
                            index_query, 
                            CmpMemoryIndexId);

                    if(low_index == sorted_binary_index_list.end())
                    {
#ifdef DEBUG
                        cerr<<"find no block infor by id="<<id<<endl;
#endif
                        return false;
                    }

                    if(low_index->id != id)
                    {
#ifdef DEBUG
                        cerr<<"find no block infor by id="<<id<<endl;
#endif
                        return false;
                    }
                    if(low_index->off + low_index->size > block_size)
                    {
#ifdef DEBUG
                        cerr<<"find no block infor by id="<<id<<endl;
#endif
                        return false;
                    }

                    off = low_index->off;
                    size = low_index->size;
#ifdef DEBUG
                    cerr<<id<<", "<<off<<", "<<size<<endl;
#endif

                    return true;
                }

                // desciption: scan specified block data list
                // parameters:
                //  [IN] off -- the offset of the data list
                //  [IN] size -- the size of scanning block data units
                //  [IN] handler -- the handler that handle each block data
                //  [IN/OUT] resource -- the self-defined resource
                // return:
                //  number of block data units that handled successfully
                uint32_t Scan(const uint64_t off, const uint32_t size, HandleBlockDataUnit handler, void* resource) const
                {
                    try
                    {
                        const BlockData* block_data = (const BlockData*)(virtual_memory_mapper->GetData());
                        uint32_t iblock = 0;
                        uint32_t index_of_blockdata = 0;
                        for(uint32_t index = off; index < off + size; index++)
                        {
#ifdef DEBUG
                            cerr<<"<"<<index<<" "<<iblock<<" "<<block_data[index]<<">"<<endl;
#endif
                            if(handler(block_data[index], index_of_blockdata, resource))
                                iblock++;
                            index_of_blockdata++;
                        }
#ifdef DEBUG
                        cerr<<endl;
#endif
                        return iblock; 
                    }
                    catch(...)
                    {
                        return 0;
                    }
                    return 0;
                }
            public:
                // description: get indexes' size
                // parameters:
                //  nothing
                // return:
                //  size
                inline uint32_t IndexSize() const {return sorted_binary_index_list.size();}

                // description: get block size,block is unit of data
                // parameters:
                //  nothing
                // return:
                // size
                inline uint32_t BlockSize() const{return block_size;}

                VirtualMemoryData(const VirtualMemoryData& other)
                {
                    virtual_memory_mapper = new VirtualMemoryMapper(*(other.virtual_memory_mapper));
                    sorted_binary_index_list = other.sorted_binary_index_list;
                    block_size = other.block_size;
                }

                VirtualMemoryData()
                {
                    virtual_memory_mapper = NULL;
                    block_size = 0;
                }

                VirtualMemoryData& operator=(const VirtualMemoryData& other)
                {
                    if(&other == this)
                        return *this;

                    if(this->virtual_memory_mapper) delete this->virtual_memory_mapper;

                    this->sorted_binary_index_list = other.sorted_binary_index_list;
                    this->virtual_memory_mapper = new VirtualMemoryMapper(*other.virtual_memory_mapper);
                    block_size = other.block_size;

                    return *this;
                }

                VirtualMemoryData(const char* file)
                {
                    block_size = 0;
                    std::string file_name(file);
                    ConstructVirtualMemoryDataFileName(file_name);

                    virtual_memory_mapper = NULL;
                    std::ifstream binar_index_file((file_name + ".index").c_str(), ios::binary);
                    if(!binar_index_file.is_open())
                    {
                        cerr<<file_name<<".index can not be opened."<<endl;
                        return;
                    }

                    VirtualMemoryDataIndex index;
                    while(!binar_index_file.read((char*)(&index), sizeof(VirtualMemoryDataIndex)).eof())
                    {
#ifdef DEBUG
                        cerr<<"id = "<<index.id<<", off = "<<index.off<<", size = "<<index.size<<endl;
#endif
                        sorted_binary_index_list.push_back(index);
                    }
#ifdef DEBUG
                    cerr<<sorted_binary_index_list.size()<<endl;
#endif
                    // sort index by id
                    sort(sorted_binary_index_list.begin(), sorted_binary_index_list.end(), CmpMemoryIndexId);
                    binar_index_file.close();

                    // binary_block_file_handle
                    std::string block_file_name = file_name + ".block";
                    virtual_memory_mapper = new VirtualMemoryMapper(block_file_name.c_str());
                    block_size = virtual_memory_mapper->GetSize()/sizeof(BlockData);
                    cerr<<"[INFO] virtual_memory_data_size = "<<block_size<<endl;
                }

                virtual ~VirtualMemoryData()
                {
                    if(virtual_memory_mapper)delete virtual_memory_mapper; 
                }


                // description: scan the block list
                // parameters:
                //  [IN] id -- the id that indexes the block list
                //  [IN] handler -- the handler than handle each block data unit
                //  [IN/OUT] resource -- self-defined resource
                // return:
                //  number of units
                uint32_t Scan(const uint64_t id, HandleBlockDataUnit handler, void* resource) const
                {
                    if(handler == NULL || resource == NULL)
                        return 0;

                    uint64_t off = 0;
                    uint32_t size = 0;
                    if(!GetLocation(id, off, size))
                        return 0;

                    return Scan(off,  size,  handler, resource);
                }
                uint32_t Scan(const uint64_t id, BlockDataScanner<BlockData>* scanner, void* resource) const
                {
                    if(scanner == NULL || resource == NULL)
                        return 0;
                    uint64_t off = 0;
                    uint32_t size = 0;
                    if(!GetLocation(id, off, size))
                        return 0;

                    ResouceUsingBlockDataScanner<BlockData> for_scanner;
                    for_scanner.scanner = scanner;
                    for_scanner.resource = resource;
                    return Scan(off,  size,  UsingBlockDataScanner, (void*)(&for_scanner));
                }

                // description: copy block data
                // parameters:
                //  [IN] id -- the id
                //  [OUT] the_block_size -- copy block data size
                // return:
                //  data pointer
                BlockData* Get(const uint64_t id, uint32_t& the_block_size) const
                {
                    the_block_size = 0;
                    uint64_t off = 0;
                    uint32_t size = 0;
                    if(!GetLocation(id, off, size))
                        return 0;

                    BlockData* block_data = new BlockData[size];
                    if((the_block_size = Scan(off, size, CopyHandler, (void*)block_data)) == 0)
                    {
                        delete [] block_data;
                        return NULL;
                    }

                    return block_data;
                }

                // description: get specified BlockData in specified id's list, in other words, return just one data object if there exists
                // parameters:
                //  [IN] id -- the id
                //  [IN] index -- the index 
                //  [OUT data -- output data via this pointer
                // return:
                //  true -- if exists and copy success
                bool GetData(const uint64_t id, const uint32_t index, BlockData* data) const
                {
                    if(data == NULL)
                        return false;

                    uint64_t off = 0;
                    uint32_t size = 0;
                    if(!GetLocation(id, off, size))
                        return false;
#ifdef DEBUG
                    cerr<<off<<", "<<size<<endl;
#endif

                    if(index >= size)
                        return false;

                    const BlockData* block_data = (const BlockData*)(virtual_memory_mapper->GetData());
                    (*data) = BlockData(block_data[off + index]);
                    return true; 
                }
        };

    template<typename BlockData>
        class VirtualMemoryDataWriter
        {
            private:
                std::set<uint64_t> index_id_set;
                std::ofstream block_fout;
                std::ofstream index_fout;
                VirtualMemoryDataIndex data_index;
                bool index_flushed;
                uint32_t block_size_writed;
            public:
                void FlushIndex()
                {
                    if(!index_flushed)
                    {
                        index_fout.write((char*)(&data_index), sizeof(VirtualMemoryDataIndex));
                        index_flushed = true;
                        data_index.off += block_size_writed;
                        data_index.size = 0;
                        block_size_writed = 0;
                    }
                }

                void Close()
                {
                    FlushIndex();
                    block_size_writed = 0;
                    data_index.id = 0;
                    data_index.size = 0;
                    data_index.off = 0;

                    if(block_fout.is_open())
                        block_fout.close();
                    if(index_fout.is_open())
                        index_fout.close();
                }

                VirtualMemoryDataWriter()
                {
                    index_flushed = true;
                    data_index.id = 0;
                    data_index.off = 0;
                    data_index.size = 0;
                    block_size_writed = 0;
                }

                virtual ~VirtualMemoryDataWriter()
                {
                    Close();
                }

                bool Open(const char* file)
                {
                    Close();

                    std::string file_name(file);
                    ConstructVirtualMemoryDataFileName(file_name);
                    block_fout.open((file_name + ".block").c_str(), ios::binary);
                    if(!block_fout.is_open())
                        return false;

                    index_fout.open((file_name + ".index").c_str(), ios::binary);
                    if(!index_fout.is_open())
                        return false;

                    return true;
                }

                bool SwitchIndex(const uint64_t id)
                {
                    if(index_id_set.count(id))
                        return false;

                    FlushIndex();

                    index_id_set.insert(id);
                    data_index.id = id;
                    index_flushed = false;
                }

                // description: 
                bool Write(const BlockData& block_data, bool flush_index = false)
                {
                    block_fout.write((char*)(&block_data), sizeof(BlockData));
                    data_index.size++;
                    block_size_writed++;
                    if(flush_index) FlushIndex();

                    return true;
                }
        };
};

#endif
